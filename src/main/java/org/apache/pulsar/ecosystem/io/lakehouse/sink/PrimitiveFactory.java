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

import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.schema.SchemaType;

/**
 *  PrimitiveFactory provides a way to get different PulsarObject according to the given schema type.
 */
public class PrimitiveFactory {

    public static PulsarObject getPulsarPrimitiveObject(SchemaType schemaType, Object value,
                                                         String overrideFieldName) {
        PulsarObject object;
        switch (schemaType) {
            case BYTES:
                // the nio heap buffer is not serializable
                object = new PulsarObject<>(ByteBuffer.wrap((byte[]) value), Schema.create(Schema.Type.BYTES));
                break;
            case STRING:
                object = new PulsarObject<>((String) value, Schema.create(Schema.Type.STRING));
                break;
            default:
                throw new RuntimeException("Failed to build pulsar object, the given type '" + schemaType + "' "
                    + "is not supported yet.");
        }
        if (StringUtils.isNotEmpty(overrideFieldName)) {
            object.overrideFieldName(overrideFieldName);
        }
        return object;
    }
}
