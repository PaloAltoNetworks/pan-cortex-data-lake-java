/**
 * SchemaMetadata
 * 
 * Copyright 2015-2020 Palo Alto Networks, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paloaltonetworks.cortex.data_lake;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue.ValueType;

/**
 * Schema metadata
 */
public class SchemaMetadata {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    public static enum TimeUnits {
        MINUTES, HOURS, DAYS, MONTHS, YEARS
    }

    public static enum Operations {
        ALL, QUERY, STREAM, COMPUTE
    };

    public static class MetadataPartionSchema {
        /**
         * time unit for partition scheme, like minute, hour, day
         */
        public final TimeUnits timeUnit;
        /**
         * requency of partition, like 5 minute
         */
        public final int frequency;

        public MetadataPartionSchema(int frequency) {
            this.frequency = frequency;
            this.timeUnit = TimeUnits.HOURS;
        }

        public MetadataPartionSchema(int frequency, TimeUnits timeUnit) {
            this.frequency = frequency;
            this.timeUnit = timeUnit;
        }

        static MetadataPartionSchema parse(JsonObject jsonResponse) throws SchemaServiceParseException {
            Integer frequency;
            TimeUnits timeUnit;
            // frequency - mandatory
            try {
                frequency = jsonResponse.getInt("frequency");
                logger.finest("Metadata frequency: " + frequency);
            } catch (Exception e) {
                logger.info("'frequency' is either missing or not an Integer value");
                throw new SchemaServiceParseException("'frequency' is either missing or not an Integer value");
            }
            // timeUnit - optional
            try {
                timeUnit = TimeUnits.valueOf(jsonResponse.getString("timeUnit", "HOURS"));
                logger.finest("Metadata frequency: " + frequency);
            } catch (Exception e) {
                logger.info("'timeUnit' is either missing or not a String value");
                throw new SchemaServiceParseException("'timeUnit' is either missing or not a valid enum value");
            }
            return new MetadataPartionSchema(frequency, timeUnit);
        }
    }

    public String partitionColumn;
    public Collection<String> tags;
    public Collection<String> timestampColumns;
    /**
     * timestamp format
     */
    public String timestampFormat;
    /**
     * timestamp Timezone
     */
    public String timestampTimezone;
    public MetadataPartionSchema partitionScheme;
    public boolean isPublic;
    public boolean derived;
    public String idColumn;
    public Collection<String> clusterColumns;
    /**
     * documentation about metadata section
     */
    public String doc;
    public Operations operations;
    public Collection<String> logical_types;
    public Collection<String> memoryAllocation;
    public Integer streamPartitionFactor;

    private SchemaMetadata() {
        isPublic = true;
        operations = Operations.ALL;
    }

    public static class Builder {
        private final SchemaMetadata obj;

        public Builder() {
            obj = new SchemaMetadata();
        }

        public SchemaMetadata build() {
            return obj;
        }

        public Builder partitionColumn(String val) {
            obj.partitionColumn = val;
            return this;
        }

        public Builder tags(Collection<String> val) {
            obj.tags = val;
            return this;
        }

        public Builder timestampColumns(Collection<String> val) {
            obj.timestampColumns = val;
            return this;
        }

        public Builder timestampFormat(String val) {
            obj.timestampFormat = val;
            return this;
        }

        public Builder timestampTimezone(String val) {
            obj.timestampTimezone = val;
            return this;
        }

        public Builder partitionScheme(Integer frequency) {
            obj.partitionScheme = new MetadataPartionSchema(frequency);
            return this;
        }

        public Builder partitionScheme(Integer frequency, TimeUnits timeunit) {
            obj.partitionScheme = new MetadataPartionSchema(frequency, timeunit);
            return this;
        }

        public Builder partitionScheme(MetadataPartionSchema partitionSchema) {
            obj.partitionScheme = partitionSchema;
            return this;
        }

        public Builder isPublic(Boolean val) {
            obj.isPublic = val;
            return this;
        }

        public Builder derived(Boolean val) {
            obj.derived = val;
            return this;
        }

        public Builder idColumn(String val) {
            obj.idColumn = val;
            return this;
        }

        public Builder clusterColumns(Collection<String> val) {
            obj.clusterColumns = val;
            return this;
        }

        public Builder doc(String val) {
            obj.doc = val;
            return this;
        }

        public Builder operations(Operations val) {
            obj.operations = val;
            return this;
        }

        public Builder logical_types(Collection<String> val) {
            obj.logical_types = val;
            return this;
        }

        public Builder memoryAllocation(Collection<String> val) {
            obj.memoryAllocation = val;
            return this;
        }

        public Builder streamPartitionFactor(Integer val) {
            obj.streamPartitionFactor = val;
            return this;
        }
    }

    private static Collection<String> extractStringArray(String label, JsonObject jsonResponse)
            throws SchemaServiceParseException {
        JsonArray labelArray;
        try {
            if (jsonResponse.isNull(label))
                return null;
            labelArray = jsonResponse.getJsonArray(label);
        } catch (Exception e) {
            throw new SchemaServiceParseException(label + " is not a String array");
        }
        var returnValue = new ArrayList<String>(labelArray.size());
        for (var item : labelArray) {
            if (item.getValueType() != ValueType.STRING) {
                logger.info(label + " includes a non-string entry");
                throw new SchemaServiceParseException(label + " includes a non-string entry");
            }
            returnValue.add(item.toString());
        }
        logger.finest("Metadata '" + label + "' successfully parsed");
        return returnValue;
    }

    static SchemaMetadata parse(JsonObject jsonResponse) throws SchemaServiceParseException {
        var schemaBuilder = new SchemaMetadata.Builder();

        logger.finest("request to parse a Metadata Object");
        // partitionColumn - mandatory
        try {
            schemaBuilder.partitionColumn(jsonResponse.getString("partitionColumn"));
            logger.finest("Metadata partitionColumn: " + schemaBuilder.obj.partitionColumn);
        } catch (Exception e) {
            logger.info("'partitionColumn' is either missing or not a string");
            throw new SchemaServiceParseException("'partitionColumn' is either missing or not a string");
        }
        // tags - optional
        try {
            schemaBuilder.tags(extractStringArray("tags", jsonResponse));
        } catch (ClassCastException e) {
            logger.info("tags is not an array object");
            throw new SchemaServiceParseException("'tags' is not an array object");
        }
        // timestampColumns - optional
        try {
            schemaBuilder.timestampColumns(extractStringArray("timestampColumns", jsonResponse));
        } catch (ClassCastException e) {
            logger.info("timestampColumns is not an array object");
            throw new SchemaServiceParseException("'timestampColumns' is not an array object");
        }
        // timestampFormat - optional
        try {
            schemaBuilder.timestampFormat(jsonResponse.getString("timestampFormat", null));
            logger.finest("Metadata timestampFormat: " + schemaBuilder.obj.timestampFormat);
        } catch (ClassCastException e) {
            logger.info("'timestampFormat' is not a string");
            throw new SchemaServiceParseException("'timestampFormat' is not a string");
        }
        // timestampTimezone - optional
        try {
            schemaBuilder.timestampTimezone(jsonResponse.getString("timestampTimezone", null));
            logger.finest("Metadata timestampFormat: " + schemaBuilder.obj.timestampTimezone);
        } catch (ClassCastException e) {
            logger.info("'timestampTimezone' is not a string");
            throw new SchemaServiceParseException("'timestampTimezone' is not a string");
        }
        // partitionScheme - mandatory
        try {
            schemaBuilder.partitionScheme(MetadataPartionSchema.parse(jsonResponse.getJsonObject("partitionScheme")));
            logger.finest("Metadata partitionScheme successfully parsed");
        } catch (Exception e) {
            logger.info("'partitionScheme' does not exist or can't be parsed");
            throw new SchemaServiceParseException("'partitionScheme' does not exist or can't be parsed");
        }
        // public - mandatory
        try {
            schemaBuilder.isPublic(jsonResponse.getBoolean("public", true));
            logger.finest("Metadata public: " + schemaBuilder.obj.isPublic);
        } catch (ClassCastException e) {
            logger.info("'public' is not a boolean value");
            throw new SchemaServiceParseException("'public' is not a boolean value");
        }
        // derived - mandatory
        try {
            schemaBuilder.derived(jsonResponse.getBoolean("derived", true));
            logger.finest("Metadata derived: " + schemaBuilder.obj.derived);
        } catch (ClassCastException e) {
            logger.info("'derived' is not a boolean value");
            throw new SchemaServiceParseException("'derived' is not a boolean value");
        }
        // idColumn - optional
        try {
            schemaBuilder.idColumn(jsonResponse.getString("idColumn", null));
            logger.finest("Metadata idColumn: " + schemaBuilder.obj.idColumn);
        } catch (ClassCastException e) {
            logger.info("'idColumn' is not a string");
            throw new SchemaServiceParseException("'idColumn' is not a string");
        }
        // clusterColumns - optional
        try {
            schemaBuilder.clusterColumns(extractStringArray("clusterColumns", jsonResponse));
        } catch (ClassCastException e) {
            logger.info("clusterColumns is not an array object");
            throw new SchemaServiceParseException("'clusterColumns' is not an array object");
        }
        // doc - optional
        try {
            schemaBuilder.doc(jsonResponse.getString("doc", null));
            logger.finest("Metadata doc: " + schemaBuilder.obj.doc);
        } catch (ClassCastException e) {
            logger.info("'doc' is not a string");
            throw new SchemaServiceParseException("'doc' is not a string");
        }
        // operations - optional
        try {
            schemaBuilder.operations(Operations.valueOf(jsonResponse.getString("operations", null)));
            logger.finest("Metadata operations: " + schemaBuilder.obj.operations);
        } catch (Exception e) {
            logger.info("'operations' is either missing or not a String value");
            throw new SchemaServiceParseException("'operations' is either missing or not a valid enum value");
        }
        // logical_types - optional
        try {
            schemaBuilder.logical_types(extractStringArray("logical_types", jsonResponse));
        } catch (ClassCastException e) {
            logger.info("logical_types is not an array object");
            throw new SchemaServiceParseException("'logical_types' is not an array object");
        }
        // memoryAllocation - optional
        try {
            schemaBuilder.memoryAllocation(extractStringArray("memoryAllocation", jsonResponse));
        } catch (ClassCastException e) {
            logger.info("memoryAllocation is not an array object");
            throw new SchemaServiceParseException("'memoryAllocation' is not an array object");
        }
        // streamPartitionFactor - mandatory
        try {
            schemaBuilder.streamPartitionFactor(jsonResponse.getInt("streamPartitionFactor"));
            logger.finest("Metadata streamPartitionFactor: " + schemaBuilder.obj.streamPartitionFactor);
        } catch (Exception e) {
            logger.info("'streamPartitionFactor' is either missing or not an Integer value");
            throw new SchemaServiceParseException("'streamPartitionFactor' is either missing or not an Integer value");
        }
        return schemaBuilder.build();
    }
}