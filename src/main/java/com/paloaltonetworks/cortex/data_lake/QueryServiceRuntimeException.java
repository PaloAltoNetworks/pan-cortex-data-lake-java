/**
 * QueryServiceRuntimeException
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
import java.util.List;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonStructure;

/**
 * Object representation of a Cortex API Error response used in async operations
 * (unchecked)
 */
public class QueryServiceRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    /**
     * HTTP status code as seen in the response header
     */
    public final int httpStatusCode;
    /**
     * Cortex API error list
     */
    public final List<QueryApiError> cortexApiError;
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    private QueryServiceRuntimeException(String message, int httpStatusCode, List<QueryApiError> cortexApiError) {
        super(message);
        this.httpStatusCode = httpStatusCode;
        this.cortexApiError = cortexApiError;
    }

    static List<QueryApiError> parse(JsonStructure jsonResponse) throws QueryServiceParseRuntimeException {
        JsonArray arrayResponse;
        logger.finest("request to parse a List of CortexApiError");
        try {
            arrayResponse = jsonResponse.asJsonArray();
        } catch (ClassCastException e) {
            JsonObject objectResponse;
            try {
                objectResponse = jsonResponse.asJsonObject();
            } catch (ClassCastException e2) {
                logger.info("Response is neither an Error Array nor an Error Object");
                throw new QueryServiceParseRuntimeException("Response is neither an Error Array nor an Error Object");
            }
            logger.finest("single object received. Will convert it int a single item array.");
            List<QueryApiError> apiErrorList = new ArrayList<QueryApiError>(1);
            apiErrorList.add(QueryApiError.parse(objectResponse));
            return apiErrorList;
        }
        return new ArrayList<QueryApiError>(arrayResponse.getValuesAs(QueryApiError::parse));
    }

    /**
     * Creates a QueryServiceException object
     * 
     * @param message        Exception message
     * @param httpStatusCode HTTP Error status code header value
     * @param jsonResponse   Cortex API error response body
     * @return a initialized QueryServiceException object conform to the Cortex API
     *         error interface.
     */
    static QueryServiceRuntimeException factory(String message, int httpStatusCode, JsonStructure jsonResponse)
            throws QueryServiceParseRuntimeException {
        return new QueryServiceRuntimeException(message, httpStatusCode,
                QueryServiceRuntimeException.parse(jsonResponse));
    }
}