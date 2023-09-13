/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.ecosystem.io.lakehouse.sink;

import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.lakehouse.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.lakehouse.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.CommitFailedException;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.LakehouseConnectorException;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.LakehouseWriterException;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.delta.DeltaWriter;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.hudi.HoodieWriter;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.iceberg.IcebergWriter;
import org.apache.pulsar.functions.api.Record;


/**
 * Writer thread. Fetch records from queue, and write them into lakehouse table.
 */
@Slf4j
public class SinkWriter implements Runnable {
    private final SinkConnectorConfig sinkConnectorConfig;
    private Optional<LakehouseWriter> lakehouseWriter;
    private Record<GenericObject> lastRecord;
    private final GenericDatumReader<GenericRecord> datumReader;
    private final long timeIntervalPerCommit;
    private long lastCommitTime;
    private long recordCount;
    private final long maxRecordsPerCommit;
    private final int maxCommitFailedTimes;
    private volatile boolean running;
    private final LinkedBlockingQueue<Record<GenericObject>> messages;
    private int failedCommitCount;

    public SinkWriter(SinkConnectorConfig sinkConnectorConfig, LinkedBlockingQueue<Record<GenericObject>> messages) {
        this.messages = messages;
        this.sinkConnectorConfig = sinkConnectorConfig;
        this.lakehouseWriter = Optional.empty();
        this.datumReader = new GenericDatumReader<>();
        this.timeIntervalPerCommit = TimeUnit.SECONDS.toMillis(sinkConnectorConfig.getMaxCommitInterval());
        this.maxRecordsPerCommit = sinkConnectorConfig.getMaxRecordsPerCommit();
        this.maxCommitFailedTimes = sinkConnectorConfig.getMaxCommitFailedTimes();
        this.lastCommitTime = System.currentTimeMillis();
        this.recordCount = 0;
        this.failedCommitCount = 0;
        this.running = true;
    }

    /**
     * Set new AVRO schema for <code>GenericDatumReader</code> and the <code>LakehouseWriter</code>.
     * @param recordSchema new AVRO schema
     */
    private void updateWriterSchema(Schema recordSchema) throws IOException, LakehouseConnectorException {
        if (this.datumReader.getSchema() == null || !this.datumReader.getSchema().equals(recordSchema)) {
            this.datumReader.setSchema(recordSchema);
            this.datumReader.setExpected(recordSchema);
        }

        if (this.lakehouseWriter.isPresent()) {
            if (this.lakehouseWriter.get().updateSchema(recordSchema)) {
                this.resetStatus();
            }
        }
    }

    /**
     * <h2>
     * NOTE: (IMPORTANT) if a record has BYTES or NONE SchemaType, this method
     * will substitute it with the JSON SchemaType.
     * </h2>
     * <h3>
     * Explanation: The messages received from the MQTT protocol handler have their
     * schema set to one of these values. The Deltalake table, though, requires a
     * proper data structure. <i>This is a workaround that I'll need to address</i>.
     * </h3>
     * Process the schema of a single record.
     * <ul>
     * <li>If there's no schema in the record, use the one from the sink config.
     * </li>
     * <li>If the schema is different from the one used for the previous records,
     * update the writer's schema.</li>
     * </ul>
     * @param recordSchemaString schema string in AVRO JSON schema format pulled
     * from the record
     * @return parsed AVRO schema of the current record
     */
    private Schema parseRecordSchema(String recordSchemaString) throws IOException, LakehouseConnectorException {
        if (Strings.isNullOrEmpty(recordSchemaString.trim())) {
            log.info("Record doesn't contain schema description, pulling schema from sink configuration");
            recordSchemaString = this.sinkConnectorConfig.getSchemaString();
        }

        Schema recordSchema = SchemaConverter.convertPulsarAvroSchemaToNonNullSchema(
            new Schema.Parser().parse(recordSchemaString)
        );

        try {
            this.updateWriterSchema(recordSchema);
        } catch (IOException ioe) {
            throw new IOException(
                String.format(
                    "Failed to substitute the current writer's schema ({}) "
                    + "with the new record's schema ({}): {}",
                    ioe
                )
            );
        }
        return recordSchema;
    }

    /**
     * Process a single non-null record. Assumes that the schema of the record has
     * already been processed.
     * <ul>
     * <li>Read its schema and check if it's the same as the one used for the
     * previous records</li>
     * <li>Convert the record to AVRO GenericRecord</li>
     * <li>Write it to the lakehouse table</li>
     * </ul>
     * @param record new non-null record polled from the <code>this.messages</code>
     * @param recordSchema AVRO schema of the current record, processed by parseRecordSchema
     */
    private void processRecord(
        Record<GenericObject> record,
        Schema recordSchema
    ) throws LakehouseConnectorException, IOException {
        log.debug(
                "Handling message: {}",
                record.getMessage().map(m -> m.getMessageId().toString()).orElse("null"));

        try {
            Optional<GenericRecord> avroRecord = this.convertToAvroGenericRecord(
                record,
                recordSchema,
                datumReader
            );

            if (avroRecord.isPresent()) {
                this.lakehouseWriter.get().writeAvroRecord(avroRecord.get());
                this.lastRecord = record;
                recordCount++;
                if (commitTimeElapsed() || numRecordsExceedCommitLimit()) {
                    commitRecords();
                }
            } else {
                throw new IOException(
                    String.format(
                        "Conversion to AVRO record resulted in 'null' for message with id {})",
                        record.getMessage().map(m -> m.getMessageId().toString()).orElse("null")
                    )
                );
            }
        } catch (IOException ioe) {
            throw new IOException("Failed to convert the record to AVRO GenericRecord", ioe);
        } catch (AvroTypeException ate) {
            // TODO: implement the dead letter topic
            byte[] byteArray = (byte[]) record.getValue().getNativeObject();
            String recordString = new String(byteArray, StandardCharsets.UTF_8);
            log.warn("Message didn't match declared schema: {}; {}", ate.getMessage(), ate.getCause());
            log.warn("Message {}; schema: {}", recordString, recordSchema.toString());
            log.warn("Skipping message until we implement the dead letter topic");
        }
    }

    public void run() {
        while (this.running) {
            try {
                Optional<Record<GenericObject>> currentRecord = Optional.ofNullable(
                        messages.poll(100, TimeUnit.MILLISECONDS));

                if (currentRecord.isPresent()) {
                    Record<GenericObject> record = currentRecord.get();
                    Schema recordSchema = this.parseRecordSchema(
                        record.getSchema().getSchemaInfo().getSchemaDefinition()
                    );
                    if (!this.lakehouseWriter.isPresent()) {
                        this.lakehouseWriter = Optional.of(
                            this.createLakehouseWriter(this.sinkConnectorConfig, recordSchema)
                        );
                    }
                    this.processRecord(record, recordSchema);
                } else {
                    if (commitTimeElapsed() || numRecordsExceedCommitLimit()) {
                        commitRecords();
                    }
                }
            } catch  (IOException ioe) {
                log.error("Record processing failed: ", ioe);
            } 
            catch (InterruptedException | LakehouseConnectorException e) {
                log.error("Record processing failed: ", e, ". Shutting down the read loop.");
                this.running = false;
            } 
        }
    }

    private void commitRecords() throws LakehouseConnectorException {
        if (log.isDebugEnabled()) {
            log.debug("Commit ");
        }

        if (this.lakehouseWriter.isPresent()) {
            if (this.lakehouseWriter.get().flush()) {
                this.resetStatus();
            } else {
                this.failedCommitCount++;
                log.warn("Commit records failed {} times", this.failedCommitCount);
                if (this.failedCommitCount > this.maxCommitFailedTimes) {
                    String errMsg = "Exceed the max commit failed times, the allowed max failure times is "
                            + this.maxCommitFailedTimes;
                    log.error(errMsg);
                    throw new CommitFailedException(errMsg);
                }
            }
        } else {
            log.warn("The lakehouse writer hasn't been initialized yet. "
                + "Most likely the connector tried to commit befere the first "
                + "non-null record came in. Skipping commit."
            );
        }
    }

    private LakehouseWriter createLakehouseWriter(
        SinkConnectorConfig config,
        Schema schema
    ) throws LakehouseWriterException {
        switch (config.getType()) {
            case SinkConnectorConfig.LakehouseType.DELTA:
                return new DeltaWriter(config, schema);
            case SinkConnectorConfig.LakehouseType.ICEBERG:
                return new IcebergWriter(config, schema);
            case SinkConnectorConfig.LakehouseType.HUDI:
                return new HoodieWriter(config, schema);
            default:
                throw new LakehouseWriterException("Unknown type of the Lakehouse writer. Expected 'delta', 'iceberg',"
                    + " or 'hudi', but got " + config.getType());
        }
    }

    private void resetStatus() {
        if (this.lastRecord != null) {
            this.lastRecord.ack();
        }
        this.lastCommitTime = System.currentTimeMillis();
        this.recordCount = 0;
        this.failedCommitCount = 0;
    }

    private boolean commitTimeElapsed() {
        return System.currentTimeMillis() - lastCommitTime >= timeIntervalPerCommit;
    }

    private boolean numRecordsExceedCommitLimit() {
        return recordCount >= maxRecordsPerCommit;
    }

    public Optional<GenericRecord> convertToAvroGenericRecord(Record<GenericObject> record,
            Schema schema,
            GenericDatumReader<GenericRecord> datumReader
    ) throws IOException {
        switch (record.getValue().getSchemaType()) {
            case AVRO:
                return Optional.of((GenericRecord) record.getValue().getNativeObject());
            case JSON:
                log.info("Processing JSON record: {}", record.getValue().getNativeObject().toString());
                Decoder jsonDecoder = DecoderFactory.get()
                        .jsonDecoder(schema, record.getValue().getNativeObject().toString());
                return Optional.of(datumReader.read(null, jsonDecoder));
            case BYTES:
                // consider NONE?
                byte[] byteArray = (byte[]) record.getValue().getNativeObject();
                String recordString = new String(byteArray, StandardCharsets.UTF_8);

                log.info("Message:: {}", recordString);
                // Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(byteArray, null);
                // return Optional.of(datumReader.read(null, binaryDecoder));
                Decoder byteArrayToJsonDecoder = DecoderFactory.get().jsonDecoder(schema, recordString);
                return Optional.of(datumReader.read(null, byteArrayToJsonDecoder));
            default:
                try {
                    GenericRecord gr = PrimitiveFactory.getPulsarPrimitiveObject(record.getValue().getSchemaType(),
                            record.getValue().getNativeObject(), this.sinkConnectorConfig.getOverrideFieldName())
                            .getRecord();
                    return Optional.of(gr);
                } catch (Exception e) {
                    log.error("not support this kind of schema: {}", record.getValue().getSchemaType(), e);
                    return Optional.empty();
                }
        }
    }

    public boolean isRunning() {
        return this.running;
    }

    public void close() throws IOException {
        this.running = false;
        if (this.lakehouseWriter.isPresent()) {
            this.lakehouseWriter.get().close();
        }
        if (this.lastRecord != null) {
            this.lastRecord.ack();
        }
    }
}
