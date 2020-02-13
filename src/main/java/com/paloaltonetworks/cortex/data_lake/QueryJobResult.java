/**
 * QueryJobResult
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

import java.util.List;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import com.paloaltonetworks.cortex.data_lake.QueryJobDetail.JobState;

/**
 * Object representation of the Query Service <b>JobResult</b> model.
 */
public class QueryJobResult {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Format of the retrieved log records. Log records are returned in the response
     * object’s page.result.result array. Each element of this array is a single log
     * record. This parameter identifies each such record’s format.
     */
    public enum ResultFormat {
        /**
         * Individual records are provided as an array. Each element of this log record
         * array is a log field value. Position is meaningful in this array. Use the
         * result object’s schema.fields array to identify the log field to which any
         * given member of the log record array belongs.
         */
        valuesArray,
        /**
         * Individual log records are provided as a dictionary, or JSON object. Each
         * such object’s field name is identical to the log record field name.
         */
        valuesDictionary;
    }

    /**
     * Identifies the schema used by the log records contained in the result set.
     * 
     * This field is omitted when the resultFormat query parameter is dictionary.
     */
    public static class Schema {
        /**
         * Describes a single log field.
         */
        public final JsonArray fields;

        private Schema(JsonArray fields) {
            this.fields = fields;
        }

        static Schema parse(JsonObject jsonObject) throws QueryServiceParseRuntimeException {
            logger.finest("request to parse a Schema");
            try {
                JsonArray jFieldsArray = jsonObject.getJsonArray("fields");
                return new Schema(jFieldsArray);
            } catch (ClassCastException e) {
                {
                    logger.info("schema result is not an array.");
                    throw new QueryServiceParseRuntimeException("schema result is not an array.");
                }
            }
        }
    }

    /**
     * Contains the data records in its container page.
     */
    public static class PageResult {
        /**
         * Array of result log records. If resultFormat is dictionary, each array
         * element is a JSON object where each dictionary key is a log field name. Else,
         * each array element is an array of log field values.
         */
        public final List<JsonValue> data;

        private PageResult(JsonArray data) {
            this.data = data;
        }

        static PageResult parse(JsonObject jsonObject) throws QueryServiceParseRuntimeException {
            JsonArray jDataArray = null;
            logger.finest("request to parse a PageResult");
            try {
                jDataArray = jsonObject.getJsonArray("data");
                return new PageResult(jDataArray);
            } catch (ClassCastException e) {
                try {
                    if (jsonObject.isNull("data")) {
                        logger.finest("data is null.");
                        return new PageResult(null);
                    }
                } catch (Exception e2) {
                    {
                        logger.info("page result data is not a value");
                        throw new QueryServiceParseRuntimeException("page result data is not a value");
                    }
                }
                logger.info("'data' is neither an array nor NULL");
                throw new QueryServiceParseRuntimeException("'data' is neither an array nor NULL");
            }
        }
    }

    /**
     * A page of the job results.
     */
    public static class Page {
        /**
         * Value used to retrieve the next page in the result set.
         */
        public final String pageCursor;
        public final PageResult result;

        private Page(String pageCursor, PageResult result) {
            this.pageCursor = pageCursor;
            this.result = result;
        }

        static Page parse(JsonObject jsonObject) throws QueryServiceParseRuntimeException {
            String pageCursor = null;
            JsonObject result;
            logger.finest("request to parse a Page");
            try {
                pageCursor = jsonObject.getString("pageCursor");
            } catch (NullPointerException e) {
                logger.finest("'pageCursor' not in the response. Assuming null");
            } catch (ClassCastException e) {
                try {
                    if (jsonObject.isNull("pageCursor"))
                        logger.finest("'pageCursor' is null");
                    else {
                        logger.info("'pageCursor' is neither String nor NULL");
                        throw new QueryServiceParseRuntimeException("'pageCursor' is neither String nor NULL");
                    }
                } catch (JsonException e2) {
                    {
                        logger.info("'pageCursor' is not a value");
                        throw new QueryServiceParseRuntimeException("'pageCursor' is not a value");
                    }
                }
            }
            try {
                result = jsonObject.getJsonObject("result");
            } catch (Exception e) {
                {
                    logger.info("field 'result' is not a valid object");
                    throw new QueryServiceParseRuntimeException("field 'result' is not a valid object");
                }
            }
            if (result == null) {
                logger.info("mandatory field 'result' is missing");
                throw new QueryServiceParseRuntimeException("mandatory field 'result' is missing");
            }
            return new Page(pageCursor, PageResult.parse(result));
        }
    }

    /**
     * The unique ID assigned to this query job
     */
    public final String jobId;
    /**
     * Job state. Job is not completed unless the state is Done or Failed. If
     * Pending, no job results are available.
     */
    public final JobState state;
    /**
     * Determines the format of the result array elements.
     */
    public final ResultFormat resultFormat;
    /**
     * Number of log records contained in the result set.
     */
    public final Integer rowsInJob;
    /**
     * Number of log records contained in the current page.
     */
    public final Integer rowsInPage;
    /**
     * Identifies the schema used by the log records contained in the result set.
     * This field is omitted when the resultFormat query parameter is dictionary.
     */
    public final Schema schema;
    /**
     * A page of the job results.
     */
    public final Page page;

    private QueryJobResult(String jobId, JobState state, ResultFormat resultFormat, Integer rowsInJob,
            Integer rowsInPage, Schema schema, Page page) {
        this.jobId = jobId;
        this.state = state;
        this.resultFormat = resultFormat;
        this.rowsInJob = rowsInJob;
        this.rowsInPage = rowsInPage;
        this.schema = schema;
        this.page = page;
    }

    static QueryJobResult parse(JsonObject jsonObject) throws QueryServiceParseRuntimeException {
        String jobId;
        JobState state;
        ResultFormat resultFormat;
        Integer rowsInJob = null;
        Integer rowsInPage = null;
        JsonObject schema = null;
        JsonObject page;
        logger.finest("request to parse a QueryJobResult");
        try {
            jobId = jsonObject.getString("jobId");
            logger.finest("jobId: " + jobId);
        } catch (Exception e) {
            {
                logger.info("field 'jobId' is either missing or not a string");
                throw new QueryServiceParseRuntimeException("field 'jobId' is either missing or not a string");
            }
        }
        try {
            String stateStr = jsonObject.getString("state");
            state = JobState.valueOf(stateStr);
            logger.finest("state: " + stateStr);
        } catch (Exception e) {
            {
                logger.info("field 'state' is either missing or not a valid enum key");
                throw new QueryServiceParseRuntimeException("field 'state' is either missing or not a valid enum key");
            }
        }
        try {
            String resultFormatStr = jsonObject.getString("resultFormat");
            resultFormat = ResultFormat.valueOf(resultFormatStr);
            logger.finest("resultFormat: " + resultFormatStr);
        } catch (Exception e) {
            {
                logger.info("field 'resultFormat' is either missing or not a valid enum key");
                throw new QueryServiceParseRuntimeException(
                        "field 'resultFormat' is either missing or not a valid enum key");
            }
        }
        try {
            rowsInJob = jsonObject.getJsonNumber("rowsInJob").intValueExact();
            logger.finest("rowsInJob: " + rowsInJob);
        } catch (Exception e) {
            logger.finest("Problems parsing 'rowsInJob'. Assuming null");
        }
        try {
            rowsInPage = jsonObject.getJsonNumber("rowsInPage").intValueExact();
            logger.finest("rowsInPage: " + rowsInPage);
        } catch (Exception e) {
            logger.finest("Problems parsing 'rowsInPage'. Assuming null");
        }
        try {
            schema = jsonObject.getJsonObject("schema");
        } catch (Exception e) {
            logger.finest("Problems parsing 'schema'. Assuming null");
        }
        try {
            page = jsonObject.getJsonObject("page");
        } catch (Exception e) {
            {
                logger.info("field 'page' is not a valid object");
                throw new QueryServiceParseRuntimeException("field 'page' is not a valid object");
            }
        }
        if (page == null) {
            {
                logger.info("mandatory field 'page' is missing");
                throw new QueryServiceParseRuntimeException("mandatory field 'page' is missing");
            }
        }
        return new QueryJobResult(jobId, state, resultFormat, rowsInJob, rowsInPage,
                (schema == null) ? null : Schema.parse(schema), Page.parse(page));
    }
}