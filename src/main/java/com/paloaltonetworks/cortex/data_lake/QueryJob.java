/**
 * QueryJob
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
import javax.json.JsonObject;

/**
 * Object representation of the Query Service Job Create response model
 */
public class QueryJob {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");
    public final String jobId;
    public final String uri;

    private QueryJob(String jobId, String uri) {
        this.jobId = jobId;
        this.uri = uri;
    }

    static QueryJob parse(JsonObject jsonResponse) throws QueryServiceParseRuntimeException {
        String jobId;
        String uri;
        logger.finest("request to parse a QueryJob");
        try {
            jobId = jsonResponse.getString("jobId");
            logger.finest("QueryJob jobId: " + jobId);
        } catch (Exception e) {
            logger.info("jobId is missing");
            throw new QueryServiceParseRuntimeException("'jobId' mandatory string property missing");
        }
        try {
            uri = jsonResponse.getString("uri");
            logger.finest("QueryJob uri: " + uri);
        } catch (Exception e) {
            logger.info("uri is missing");
            throw new QueryServiceParseRuntimeException("'uri' mandatory string property missing");
        }
        return new QueryJob(jobId, uri);
    }
}
