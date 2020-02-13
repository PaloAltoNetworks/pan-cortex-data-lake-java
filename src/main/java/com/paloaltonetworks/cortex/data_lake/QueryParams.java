/**
 * QueryParams
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
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Object representation of the Query Service API <b>params</b> model.
 */
public class QueryParams {
    /**
     * Possible query running priorities
     */
    public static enum Priority {
        /**
         * Run with the highest priority.
         */
        immediate,
        /**
         * Run with middle priority.
         */
        foreground,
        /**
         * Run with lowest priority.
         */
        background;
    }

    /**
     * SQL query that identifies the log records you want this query job to
     * retrieve.
     */
    public final String query;
    /**
     * Identifies the SQL query dialect that the query string uses. Defaults to
     * Csql. Currently, only Csql is supported.
     */
    public final String dialect;
    /**
     * Client’s requested priority for this job. Default: <b>foreground</b>
     */
    public final Priority priority;
    /**
     * Identifies the maximum number of milliseconds the job can run within Cortex
     * before it completes. If this limit is reached before the job has retrieved
     * its full query set, the job reports a state of Failed. In this case, some
     * query results may be available, but the result set is not guaranteed to be
     * complete. Default: <b>null</b>
     */
    public final Integer timeoutMs;
    /**
     * Maximum number of milliseconds the request’s HTTP connection remains open
     * waiting for a response. If the requested page cannot be returned in this
     * amount of time, the service closes the connection without returning results.
     * Maximum value is 2000 (2 seconds). If 0, the HTTP connection is closed
     * immediately upon completion of the HTTP request. Default: <b>null</b>
     */
    public final Integer maxWait;
    /**
     * Default number of log records retrieved for page of results. The value
     * specified here identifies the number of records appearing in the response
     * object’s result array. If the page is the last in the result set, this is the
     * maximum number of records that will appear in the result array. This
     * parameter’s maximum value is 100000. Default: <b>10000</b>
     */
    public final Integer defaultPageSize;
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Object builder
     * 
     * @param query           Mandatory {@link QueryParams#query SQL command}
     * @param dialect         Optional {@link QueryParams#dialect SQL dialect}
     * @param priority        Optional {@link QueryParams.Priority job priority}
     * @param timeoutMs       Optional {@link QueryParams#timeoutMs job timeout}
     * @param maxWait         Optional {@link QueryParams#maxWait request max wait
     *                        time}
     * @param defaultPageSize Optimal {@link QueryParams#defaultPageSize response
     *                        page size}
     */
    public QueryParams(String query, String dialect, Priority priority, Integer timeoutMs, Integer maxWait,
            Integer defaultPageSize) {
        if (query == null)
            throw new IllegalArgumentException("mandatory property 'query' can't be null");
        this.query = query;
        this.dialect = dialect;
        this.priority = priority;
        this.timeoutMs = (timeoutMs != null && timeoutMs > 0) ? timeoutMs : null;
        this.maxWait = (maxWait != null && maxWait > 0) ? maxWait : null;
        this.defaultPageSize = (defaultPageSize != null && defaultPageSize > 0) ? defaultPageSize : null;
    }

    /**
     * Short Object builder with only mandatory paramaters
     * 
     * @param query {@link QueryParams#query SQL command}
     */
    public QueryParams(String query) {
        this(query, null, null, null, null, null);
    }

    JsonObject toJson() {
        JsonObjectBuilder queryParamsBuilder = Json.createObjectBuilder().add("query", query);
        if (dialect != null)
            queryParamsBuilder.add("dialect", dialect);
        if (priority != null || timeoutMs != null || maxWait != null || defaultPageSize != null) {
            JsonObjectBuilder propertiesBuilder = Json.createObjectBuilder();
            if (priority != null)
                propertiesBuilder.add("priority", priority.name());
            if (timeoutMs != null)
                propertiesBuilder.add("timeoutMs", timeoutMs);
            if (maxWait != null)
                propertiesBuilder.add("maxWait", maxWait);
            if (defaultPageSize != null)
                propertiesBuilder.add("defaultPageSize", defaultPageSize);
            queryParamsBuilder.add("properties", propertiesBuilder);
        }
        return queryParamsBuilder.build();
    }

    static QueryParams parse(JsonObject jsonResponse) throws QueryServiceParseException {
        logger.finest("request to parse a QueryParams");
        if (jsonResponse == null) {
            logger.info("'null' response: 'query' mandatory property missing");
            throw new QueryServiceParseException("'null' response: 'query' mandatory property missing");
        }
        String query;
        try {
            query = jsonResponse.getString("query");
            logger.finest("query" + query);
        } catch (Exception e) {
            logger.info("'query' mandatory string property missing");
            throw new QueryServiceParseException("'query' mandatory string property missing");
        }
        String dialect = jsonResponse.getString("dialect", null);
        Priority priority = null;
        String priorityStr = null;
        Integer timeoutMs = null;
        Integer maxWait = null;
        Integer defaultPageSize = null;
        try {
            JsonObject propertiesObject = jsonResponse.getJsonObject("properties");
            priorityStr = propertiesObject.getString("priority", null);
            try {
                timeoutMs = propertiesObject.getInt("timeoutMs");
                logger.finest("timeoutMs: " + timeoutMs);
            } catch (Exception e) {
                logger.finest("'timeoutMs' will be null because it was either missing or not an Integer");
            }
            try {
                maxWait = propertiesObject.getInt("maxWait");
                logger.finest("maxWait: " + timeoutMs);
            } catch (Exception e) {
                logger.finest("'maxWait' will be null because it was either missing or not an Integer");
            }
            try {
                defaultPageSize = propertiesObject.getInt("defaultPageSize");
                logger.finest("defaultPageSize: " + timeoutMs);
            } catch (Exception e) {
                logger.finest("'defaultPageSize' will be null because it was either missing or not an Integer");
            }
        } catch (Exception e) {
            logger.finest("all properties will be null.");
        }
        if (priorityStr != null)
            try {
                priority = Priority.valueOf(priorityStr);
                logger.finest("priority: " + priorityStr);
            } catch (Exception e) {
                {
                    logger.info("invalid enum value for 'priority'");
                    throw new QueryServiceParseException(String.format("invalid priority '%s'", priorityStr));
                }
            }
        return new QueryParams(query, dialect, priority, timeoutMs, maxWait, defaultPageSize);
    }
}