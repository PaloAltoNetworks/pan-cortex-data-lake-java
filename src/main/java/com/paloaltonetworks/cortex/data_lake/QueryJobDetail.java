/**
 * QueryJobDetail
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
 * Object representation of the Query Service <b>jobDetails</b> model
 */
public class QueryJobDetail {
    static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Represents the completion of a Query Service job
     */
    public static final class Progress {
        /**
         * Identifies the amount of work completed by the Query Service job.
         */
        public final Integer completionPct;

        private Progress(int completionPct) {
            this.completionPct = completionPct;
        }

        static Progress parse(JsonObject jsonResponse) throws QueryServiceParseException {
            logger.finest("request to parse a Progress");
            if (jsonResponse == null) {
                logger.finest("Progress object is null.");
                return null;
            }
            Integer completionPct;
            try {
                completionPct = jsonResponse.getJsonNumber("completionPct").intValueExact();
                logger.finest("completionPct: " + completionPct);
            } catch (Exception e) {
                logger.info("completionPct is missing or not an integer.");
                throw new QueryServiceParseException("'completionPct' mandatory field is either missing or invalid");
            }
            return new Progress(completionPct);
        }
    }

    /**
     * Represents a Query Service job statistics.
     */
    public static final class Statistics {
        /**
         * Milliseconds needed to complete the job
         */
        public final Integer runTimeMs;
        /**
         * Percentage of the job retrieved from cache
         */
        public final Integer cachePct;
        /**
         * Expected amount of milliseconds to complete the job
         */
        public final Integer etaMs;

        private Statistics(Integer runTimeMs, Integer cachePct, Integer etaMs) {
            this.runTimeMs = runTimeMs;
            this.cachePct = cachePct;
            this.etaMs = etaMs;
        }

        static Statistics parse(JsonObject jsonResponse) {
            logger.finest("request to parse a Statistics");
            if (jsonResponse == null) {
                logger.finest("Statistics object is null.");
                return null;
            }
            Integer runTimeMs = null;
            Integer cachePct = null;
            Integer etaMs = null;
            try {
                runTimeMs = jsonResponse.getJsonNumber("runTimeMs").intValueExact();
                logger.finest("runTimeMs: " + runTimeMs);
            } catch (Exception e) {
                logger.finest(String.format("'runTimeMs' will keep being null due to: %s", e.getMessage()));
            }
            try {
                cachePct = jsonResponse.getJsonNumber("cachePct").intValueExact();
                logger.finest("cachePct: " + cachePct);
            } catch (Exception e) {
                logger.finest(String.format("'cachePct' will keep being null due to: %s", e.getMessage()));
            }
            try {
                etaMs = jsonResponse.getJsonNumber("etaMs").intValueExact();
                logger.finest("etaMs: " + etaMs);
            } catch (Exception e) {
                logger.finest(String.format("'etaMs' will keep being null due to: %s", e.getMessage()));
            }
            return new Statistics(runTimeMs, cachePct, etaMs);
        }
    }

    /**
     * Job is not completed unless the state is Done or Failed. If Pending, no job
     * results are available.
     */
    public static enum JobState {
        /**
         * Job has been submitted but is not yet processing data.
         */
        PENDING("pending"),
        /**
         * Job is actively processing data. Some query pages might be available for
         * retrieval.
         */
        RUNNING("running"),
        /**
         * Job is complete. All job data is ready for retrieval.
         */
        DONE("done"),
        /**
         * Job did not successfully complete.
         */
        FAILED("failed"),
        /**
         * Job did not finish in requested time limit.
         */
        TIMEDOUT("timedout"),
        /**
         * Job was terminated because of a cancel request.
         */
        CANCELLED("cancelled");

        public final String jobState;

        JobState(String jobState) {
            this.jobState = jobState;
        }
    }

    /**
     * The unique ID assigned to this query job.
     */
    public final String jobId;
    /**
     * Job is not completed unless the state is Done or Failed. If Pending, no job
     * results are available.
     */
    public final JobState state;
    /**
     * Timestamp when the query job was submitted to Cortex.
     */
    public final long submitTime;
    /**
     * Starting time range for this query. Log records older than this timestamp are
     * not considered for query evaluation. *
     */
    public final Long startTime;
    /**
     * Ending time range for this query. Log records newer than this timestamp are
     * not considered for query evaluation.
     */
    public final Long endTime;
    /**
     * {@link QueryJobDetail.Progress job progress}
     */
    public final Progress progress;
    /**
     * {@link QueryParams query params}
     */
    public final QueryParams params;
    /**
     * {@link QueryJobDetail.Statistics job statistics}
     */
    public final Statistics statistics;

    private QueryJobDetail(String jobId, JobState state, long submitTime, Long startTime, Long endTime,
            Progress progress, QueryParams params, Statistics statistics) {
        this.jobId = jobId;
        this.state = state;
        this.submitTime = submitTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.progress = progress;
        this.params = params;
        this.statistics = statistics;
    }

    static QueryJobDetail parse(JsonObject jsonResponse) throws QueryServiceParseRuntimeException {
        String jobId;
        Long submitTime;
        JobState state;
        Long startTime = null;
        Long endTime = null;
        Progress progress = null;
        QueryParams params = null;
        Statistics statistics = null;

        logger.finest("request to parse a QueryJobDetail");
        try {
            jobId = jsonResponse.getString("jobId");
            logger.finest(("jobId: " + jobId));
        } catch (Exception e) {
            {
                logger.info("jobId is missing.");
                throw new QueryServiceParseRuntimeException("'jobId' mandatory property missing");
            }
        }
        try {
            submitTime = jsonResponse.getJsonNumber("submitTime").longValueExact();
            logger.finest(("submitTime: " + submitTime));
        } catch (Exception e) {
            {
                logger.info("submitTime is missing or not an integer.");
                throw new QueryServiceParseRuntimeException(
                        "'submitTime' mandatory property is either missiong or not a valid JSON number");
            }
        }
        try {
            state = JobState.valueOf(jsonResponse.getString("state"));
            logger.finest(("state: " + state));
        } catch (Exception e) {
            {
                logger.info("state is missing or not a valid enum value.");
                throw new QueryServiceParseRuntimeException(
                        "'state' mandatory property is either missing, or invalid type or invalid enum value");
            }
        }
        try {
            startTime = jsonResponse.getJsonNumber("startTime").longValueExact();
            logger.finest(("startTime: " + startTime));
        } catch (Exception e) {
            logger.finest(String.format("'startTime' will keep being null due to: %s", e.getMessage()));
        }
        try {
            endTime = jsonResponse.getJsonNumber("endTime").longValueExact();
            logger.finest(("endTime: " + endTime));
        } catch (Exception e) {
            logger.finest(String.format("'endTime' will keep being null due to: %s", e.getMessage()));
        }
        try {
            progress = Progress.parse(jsonResponse.getJsonObject("progress"));
        } catch (Exception e) {
            logger.finest(String.format("'progress' will keep being null due to: %s", e.getMessage()));
        }
        try {
            JsonObject paramsObj = jsonResponse.getJsonObject("params");
            if (paramsObj != null)
                params = QueryParams.parse(jsonResponse.getJsonObject("params"));
        } catch (Exception e) {
            logger.finest(String.format("'params' will keep being null due to: %s", e.getMessage()));
        }
        try {
            statistics = Statistics.parse(jsonResponse.getJsonObject("statistics"));
        } catch (Exception e) {
            logger.finest(String.format("'statistics' will keep being null due to: %s", e.getMessage()));
        }
        return new QueryJobDetail(jobId, state, submitTime, startTime, endTime, progress, params, statistics);
    }
}