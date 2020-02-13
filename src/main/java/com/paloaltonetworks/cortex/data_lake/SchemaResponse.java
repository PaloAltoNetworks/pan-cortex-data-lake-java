/**
 * SchemaResponse
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

public class SchemaResponse {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    public final String schemaId;
    public final Integer version;
    public final Collection<String> errors;

    private SchemaResponse(String schemaId, Integer version, Collection<String> errors) {
        this.schemaId = schemaId;
        this.version = version;
        this.errors = errors;
    }

    static SchemaResponse parse(JsonObject jsonResponse) throws SchemaServiceParseException {
        String schemaId;
        Integer version;
        Collection<String> errors;
        logger.finest("request to parse a Schema Service Response");
        try {
            schemaId = jsonResponse.getString("schemaId");
            logger.finest("SchemaResponse schemaId: " + schemaId);
        } catch (Exception e) {
            logger.info("schemaId is missing");
            throw new SchemaServiceParseException("'schemaId' mandatory string property missing");
        }
        try {
            if (jsonResponse.isNull("version"))
                version = null;
            else
                version = jsonResponse.getInt("version");
            logger.finest("SchemaResponse version: " + version);
        } catch (Exception e) {
            logger.info("version is missing");
            throw new SchemaServiceParseException("'version' mandatory number property missing");
        }
        if (jsonResponse.isNull("errors"))
            errors = null;
        else {
            JsonArray _errors = null;
            try {
                _errors = jsonResponse.getJsonArray("errors");
            } catch (Exception e) {
                logger.info("errors is missing");
                throw new SchemaServiceParseException("'errors' mandatory array property missing");
            }
            if (_errors != null) {
                errors = new ArrayList<String>(_errors.size());
                for (var item : _errors) {
                    if (item.getValueType() != ValueType.STRING)
                        throw new SchemaServiceParseException("item in the errors list is not a String");
                    errors.add(item.toString());
                }
            } else
                errors = null;
        }
        return new SchemaResponse(schemaId, version, errors);
    }
}
