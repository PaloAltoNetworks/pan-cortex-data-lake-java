/**
 * QueryServiceClientException
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

/**
 * Describes an issue building up the query result iterator data.
 */
public class QueryServiceClientException extends Exception {

    private static final long serialVersionUID = 1L;
    public QueryJobDetail.JobState state;
    public String jobId;
    public Collection<QueryApiError> errors;

    public QueryServiceClientException(String message, String jobId, QueryJobDetail.JobState state,
            Collection<QueryApiError> errors) {
        super(message);
        this.state = state;
        this.jobId = jobId;
        this.errors = errors;
    }

    public static QueryServiceClientException fromJobDetails(String message, QueryJobDetail jobDetail) {
        return new QueryServiceClientException(message, jobDetail.jobId, jobDetail.state, jobDetail.errors);
    }
}