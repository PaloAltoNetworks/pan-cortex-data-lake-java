/**
 * QueryServiceException
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

import java.util.Collection;
import javax.json.JsonStructure;

/**
 * Object representation of a Cortex API Error response (checked)
 */
public class QueryServiceException extends Exception {
    private static final long serialVersionUID = 1L;
    /**
     * HTTP status code as seen in the response header
     */
    public final int httpStatusCode;
    /**
     * Cortex API error list
     */
    public final Collection<QueryApiError> queryApiError;

    private QueryServiceException(String message, int httpStatusCode, Collection<QueryApiError> queryApiError) {
        super(message);
        this.httpStatusCode = httpStatusCode;
        this.queryApiError = queryApiError;
    }

    /**
     * Creates a QueryServiceException object
     * 
     * @param message        Exception message
     * @param httpStatusCode HTTP Error status code header value
     * @param jsonResponse   Cortex API error response body
     * @return a initialized QueryServiceException object
     * @throws QueryServiceParseException When the jsonResponse argument does not
     *                                    conform to the Cortex API error interface.
     */
    static QueryServiceException factory(String message, int httpStatusCode, JsonStructure jsonResponse)
            throws QueryServiceParseException {
        try {
            return new QueryServiceException(message, httpStatusCode, QueryServiceRuntimeException.parse(jsonResponse));
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        }
    }

    static QueryServiceException fromException(QueryServiceRuntimeException e) {
        return new QueryServiceException(e.getMessage(), e.httpStatusCode, e.cortexApiError);
    }
}