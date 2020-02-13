/**
 * SchemaServicePayload
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

import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.json.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;

public class SchemaServicePayload {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    public String schemaId;
    public SchemaMetadata metadata;
    public Schema structure;
    public Integer version;

    public static class Builder {
        private final SchemaServicePayload obj;

        public Builder() {
            obj = new SchemaServicePayload();
        }

        public Builder schemaId(String val) {
            obj.schemaId = val;
            return this;
        }

        public Builder metadata(SchemaMetadata val) {
            obj.metadata = val;
            return this;
        }

        public Builder structure(Schema val) {
            obj.structure = val;
            return this;
        }

        public Builder structure(String val) throws SchemaServiceParseException {
            try {
                obj.structure = new Schema.Parser().parse(val);
            } catch (SchemaParseException e) {
                throw new SchemaServiceParseException("Provided value is not a valid AVRO schema JSON document.");
            }
            return this;
        }

        public Builder version(Integer val) {
            obj.version = val;
            return this;
        }

        public SchemaServicePayload build() {
            return obj;
        }
    }

    public static class RecordBuilder {
        final String name;
        String namespace;
        public SchemaBuilder.FieldAssembler<Schema> fieldAssembler;

        private RecordBuilder(String name, String namespace, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
            this.name = name;
            this.namespace = namespace;
            this.fieldAssembler = fieldAssembler;
        }

        public static RecordBuilder factory(String name, String namespace, String doc) {
            if (!Pattern.matches("^[a-zA-Z_][a-zA-Z0-9_]+$", name + namespace))
                throw new SchemaParseException(
                        "Either name or namespace contains invalid characters (/^[a-zA-Z_][a-zA-Z0-9_]+$/)");
            var rBuilder = SchemaBuilder.record(name).namespace(namespace);
            if (doc == null)
                return new RecordBuilder(name, namespace, rBuilder.fields());
            return new RecordBuilder(name, namespace, rBuilder.doc(doc).fields());
        }
    }

    public static SchemaServicePayload fromRecordBuilder(RecordBuilder builder, SchemaMetadata metadata) {
        var returnValue = new SchemaServicePayload();
        returnValue.metadata = metadata;
        returnValue.schemaId = builder.namespace + "." + builder.name;
        returnValue.version = null;
        returnValue.structure = builder.fieldAssembler.endRecord();
        return returnValue;
    }

    public static SchemaServicePayload parse(JsonObject jsonResponse) throws SchemaServiceParseException {
        var payloadBuilder = new SchemaServicePayload.Builder();

        logger.finest("request to parse a SchemaServicePayload Object");
        // schemaId mandatory
        try {
            payloadBuilder.schemaId(jsonResponse.getString("schemaId"));
            logger.finest("SchemaServicePayload schemaId: " + payloadBuilder.obj.schemaId);
        } catch (Exception e) {
            logger.info("'schemaId' is either missing or not a string");
            throw new SchemaServiceParseException("'schemaId' is either missing or not a string");
        }
        // metadata mandatory;
        try {
            payloadBuilder.metadata(SchemaMetadata.parse(jsonResponse.getJsonObject("metadata")));
            logger.finest("SchemaServicePayload metadata successfully parsed");
        } catch (Exception e) {
            logger.info("'metadata' is either missing or not a valid object");
            throw new SchemaServiceParseException("'metadata' is either missing or not a valid object");
        }
        // structure mandatory;
        try {
            payloadBuilder.structure(new Schema.Parser().parse(jsonResponse.getString("structure")));
            logger.finest("SchemaServicePayload structure successfully parsed");
        } catch (Exception e) {
            logger.info("'structure' is either missing or not a valid object");
            throw new SchemaServiceParseException("'structure' is either missing or not a valid object");
        }
        // version mandatory;
        try {
            payloadBuilder.version(jsonResponse.getInt("version"));
            logger.finest("SchemaServicePayload version: " + payloadBuilder.obj.version);
        } catch (Exception e) {
            logger.info("'version' is either missing or not an Integer");
            throw new SchemaServiceParseException("'version' is either missing or not an Integer");
        }

        return payloadBuilder.build();
    }
}