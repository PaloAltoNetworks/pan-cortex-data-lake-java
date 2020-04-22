/**
 * QueryApiError
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

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * Represents a Query API error message.
 */
public class QueryApiError {
    /**
     * Error code. Typically this is the error code seen in the first element of the
     * errors array.
     */
    public final Integer errorCode;
    /**
     * Textual description of the error.
     */
    public final String message;
    /**
     * Diagnostic context, if any.
     */
    public final String context;
    private static Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    private QueryApiError(Integer errorCode, String message, String context) {
        this.errorCode = errorCode;
        this.message = message;
        this.context = context;
    }

    static QueryApiError parse(JsonObject jsonResponse) throws QueryServiceParseRuntimeException {
        logger.finest("request to parse a Query API Error Item");
        if (jsonResponse == null) {
            logger.info("Query API error JSON object is null.");
            throw new QueryServiceParseRuntimeException("response can't be null");
        }
        Integer errorCode = null;
        String message = jsonResponse.getString("message", null);
        logger.finest("Query API Error message: " + message);
        String context = jsonResponse.getString("context", null);
        logger.finest("Query API Error context: " + context);
        try {
            JsonNumber errorCodeNum = jsonResponse.getJsonNumber("errorCode");
            if (errorCodeNum != null) {
                errorCode = errorCodeNum.intValueExact();
                logger.finest("Query API Error errorCode: " + String.valueOf(errorCode));
            }
        } catch (Exception e) {
            logger.info("Query API error JSON errorCode is not an integer.");
            throw new QueryServiceParseRuntimeException("'errorCode' field is invalid");
        }
        return new QueryApiError(errorCode, message, context);
    }

    static Collection<QueryApiError> parse(JsonArray jsonResponse) throws QueryServiceParseRuntimeException {
        logger.finest("request to parse a Query API Error Collection");
        if (jsonResponse == null) {
            logger.info("Query API error JSON object is null.");
            throw new QueryServiceParseRuntimeException("response can't be null");
        }
        Collection<QueryApiError> errorItems = new ArrayList<QueryApiError>(jsonResponse.size());
        for (JsonValue item : jsonResponse) {
            try {
                JsonObject errorItem = item.asJsonObject();
                errorItems.add(QueryApiError.parse(errorItem));
            } catch (Exception e) {
                logger.info("Query API error JSON error item is not an object or can't be parsed");
                throw new QueryServiceParseRuntimeException("error item is invalid");
            }
        }
        return errorItems;
    }

    String asString() {
        return String.format("ErrorCode: %d, Message: %s, Context: %s", errorCode, message, context);
    }
}
