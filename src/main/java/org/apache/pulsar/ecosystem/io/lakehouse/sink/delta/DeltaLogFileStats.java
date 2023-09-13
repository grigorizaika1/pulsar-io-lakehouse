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

package org.apache.pulsar.ecosystem.io.lakehouse.sink.delta;

/**
 * This class represents stats (e.g. add.stats) JSON object in delta log records.
 * Stats in log records serve to optimize table aggregations.
 * NOTE: We don't this class it in the code yet.
 * TODO: Use when there's time to do so. Figure out how to get these stats from the parquet files.
 */
public class DeltaLogFileStats {
    int numRecords;
    String minValues;
    String maxValues;
    String nullCount;
    String tags;

    public DeltaLogFileStats(int numRecords, String minValues, String maxValues, String nullCount, String tags) {
        this.numRecords = numRecords;
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.nullCount = nullCount;
        this.tags = tags;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public String getMinValues() {
        return minValues;
    }

    public void setMinValues(String minValues) {
        this.minValues = minValues;
    }

    public String getMaxValues() {
        return maxValues;
    }

    public void setMaxValues(String maxValues) {
        this.maxValues = maxValues;
    }

    public String getNullCount() {
        return nullCount;
    }

    public void setNullCount(String nullCount) {
        this.nullCount = nullCount;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }
}