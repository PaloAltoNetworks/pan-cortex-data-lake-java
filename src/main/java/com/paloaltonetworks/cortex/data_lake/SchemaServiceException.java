/**
 * SchemaServiceException
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
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

/**
 * Object representation of a Cortex API Error response (checked)
 */
public class SchemaServiceException extends Exception {
    private static final long serialVersionUID = 1L;
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");
    /**
     * HTTP status code as seen in the response header
     */
    public final int httpStatusCode;
    public final String path;
    public final String timestamp;
    public final String schemaId;
    public final Integer version;
    /**
     * Cortex API error list
     */
    public final SchemaApiError schemaApiError;

    private SchemaServiceException(String message, int httpStatusCode, String path, String timestamp, String schemaId,
            Integer version, SchemaApiError schemaApiError) {
        super(message);
        this.httpStatusCode = httpStatusCode;
        this.path = path;
        this.timestamp = timestamp;
        this.schemaId = schemaId;
        this.version = version;
        this.schemaApiError = schemaApiError;
    }

    static SchemaServiceException factory(String message, int httpStatusCode, JsonStructure response)
            throws SchemaServiceParseException {
        if (response == null)
            return new SchemaServiceException(message, httpStatusCode, null, null, null, null, null);
        if (response.getValueType() != ValueType.OBJECT) {
            throw new SchemaServiceParseException(
                    "unable to parse Schema Service response '" + response.toString() + "'");
        }
        var responseObj = response.asJsonObject();
        String path = responseObj.getString("path", null);
        logger.finest("SchemaException path: " + path);
        String timestamp = responseObj.getString("timestamp", null);
        logger.finest("SchemaException timestamp: " + timestamp);
        String schemaId = responseObj.getString("schemaId", null);
        logger.finest("SchemaException schemaId: " + schemaId);
        Integer version;
        try {
            version = responseObj.getInt("version");
            logger.finest("SchemaException version: " + version);
        } catch (Exception e) {
            version = null;
            logger.finest("SchemaException version unparseable. Defaulting to null");
        }
        String error = responseObj.getString("error", null);
        logger.finest("SchemaException error: " + error);
        String _message = responseObj.getString("message", null);
        logger.finest("SchemaException message: " + _message);
        return new SchemaServiceException(message, httpStatusCode, path, timestamp, schemaId, version,
                new SchemaApiError(error, _message));
    }
}
