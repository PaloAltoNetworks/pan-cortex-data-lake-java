/**
 * QueryService
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;

import com.paloaltonetworks.cortex.data_lake.QueryJobResult.ResultFormat;

/**
 * Low level Cortex Query Service API wrapper class. It provides methods that
 * mimic the service endpoints.
 * 
 * Developers would typically preefer the {@link QueryServiceClient} subclass
 * that provides an Iterable implementation to navigate through the query
 * results.
 */
public class QueryService {
    private final Http2Fetch client;
    private final CredentialTuple defaultCred;
    private static Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Low level Cortex Query Service API wrapper class. Constructor that generates
     * an underlying mTLS Http2Fetcher to be used by any operation triggered from
     * this object.
     * 
     * @param defaultEntryPoint Full Qualified Domain Name to the Cortex API
     *                          instance to use.
     * @param keystore          Name of the file that contains the client
     *                          certificate.
     * @param password          Password used to encrypt the client certificate.
     * @throws UnrecoverableKeyException Issues with the local OS SSL Libraries.
     * @throws KeyManagementException    Issues with the local OS SSL Libraries.
     * @throws KeyStoreException         Issues with the local OS SSL Libraries.
     * @throws NoSuchAlgorithmException  Issues with the local OS SSL Libraries.
     * @throws CertificateException      Issues with the local OS SSL Libraries.
     * @throws IOException               Problems opening the client certificate
     *                                   file.
     */
    public QueryService(String keystore, char[] password, String defaultEntryPoint) throws UnrecoverableKeyException,
            KeyManagementException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        this.client = new Http2Fetch(keystore, password, defaultEntryPoint, null);
        defaultCred = null;
    }

    /**
     * Low level Cortex Query Service API wrapper class. Constructor that allows
     * reusing an existing Http2Fetch object.
     * 
     * @param client Object to use to interface with the Cortex API End Point
     * @param cred   default credentails to be used
     */
    public QueryService(Http2Fetch client, CredentialTuple cred) {
        this.client = client;
        defaultCred = cred;
    }

    /**
     * Low level Cortex Query Service API wrapper class. Constructor that creates an
     * underlying JWT Http2Fetch to be used by operations triggered from this object
     * 
     * @param cred default credentails to be used
     * @throws KeyManagementException   Issues with the local OS SSL Libraries.
     * @throws NoSuchAlgorithmException Issues with the local OS SSL Libraries.
     */
    public QueryService(Credentials cred) throws KeyManagementException, NoSuchAlgorithmException {
        this.client = new Http2Fetch(cred, null, false);
        defaultCred = null;
    }

    private CredentialTuple sw(CredentialTuple cred) {
        return (cred == null) ? defaultCred : cred;
    }

    private BodyPublisher prepareCreateJob(String jobId, QueryParams queryParams) {
        JsonObjectBuilder jsonBody = Json.createObjectBuilder().add("params", queryParams.toJson());
        if (jobId != null)
            jsonBody.add("jobId", jobId);
        var body = jsonBody.build().toString();
        logger.finer("HTTP2 request body: " + body);
        return BodyPublishers.ofString(body);
    }

    private QueryJob processCreateJob(CortexApiResult<JsonStructure> response)
            throws QueryServiceRuntimeException, QueryServiceParseRuntimeException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw QueryServiceRuntimeException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonObject objectResponse;
        try {
            objectResponse = response.result.asJsonObject();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON object");
            throw new QueryServiceParseRuntimeException("response is not a valid JSON object");
        }
        return QueryJob.parse(objectResponse);
    }

    /**
     * Create a Cortex Query Service Job
     * 
     * Cortex Data Lake contains log data that is written by various products and
     * apps, such as Palo Alto Networks next-generation firewalls. Use this API to
     * create query jobs that return log data matching your query criteria. You
     * define query criteria using a SQL SELECT statement that you specify as part
     * of the payload for this API. You can obtain query results using the uri
     * contained in this API’s response object
     * 
     * @param jobId       Identifies the ID that you want the query job to use. This
     *                    ID must be unique within the service. Maximum length is
     *                    1000 characters. May contain any alphanumeric character,
     *                    and dash (-). This property is optional, but strongly
     *                    recommended. By default, Cortex generates a unique ID for
     *                    the query job.
     * @param queryParams Details of the query to be executed
     * @param cred        Optional credential tuple to override default one
     * @return The QueryJob object for this request
     * @throws IllegalArgumentException   In case queryParams is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     *                                    certificate location?)
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public QueryJob createJob(String jobId, QueryParams queryParams, CredentialTuple cred)
            throws IOException, InterruptedException, IllegalArgumentException, QueryServiceParseException,
            QueryServiceException, Http2FetchException, URISyntaxException {
        if (queryParams == null)
            throw new IllegalArgumentException("'queryParams' parameter is mandatory");
        logger.finest("createJob request with jobId " + jobId);
        CortexApiResult<JsonStructure> response = client.post(Constants.EP_QUERY + "jobs", this.sw(cred),
                prepareCreateJob(jobId, queryParams), "content-type", "application/json");
        try {
            return processCreateJob(response);
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        } catch (QueryServiceRuntimeException e) {
            throw QueryServiceException.fromException(e);
        }
    }

    /**
     * Create a Cortex Query Service Job
     * 
     * Cortex Data Lake contains log data that is written by various products and
     * apps, such as Palo Alto Networks next-generation firewalls. Use this API to
     * create query jobs that return log data matching your query criteria. You
     * define query criteria using a SQL SELECT statement that you specify as part
     * of the payload for this API. You can obtain query results using the uri
     * contained in this API’s response object
     * 
     * @param jobId       Identifies the ID that you want the query job to use. This
     *                    ID must be unique within the service. Maximum length is
     *                    1000 characters. May contain any alphanumeric character,
     *                    and dash (-). This property is optional, but strongly
     *                    recommended. By default, Cortex generates a unique ID for
     *                    the query job.
     * @param queryParams Details of the query to be executed
     * @param cred        Optional credential tuple to override default one
     * @return A CompletableFuture that resolves to a QueryJob object for this
     *         request
     * @throws URISyntaxException  unsupported usage of this object
     * @throws Http2FetchException unsupported usage of this object
     */
    public CompletableFuture<QueryJob> createJobAsync(String jobId, QueryParams queryParams, CredentialTuple cred)
            throws Http2FetchException, URISyntaxException {
        if (queryParams == null)
            throw new IllegalArgumentException("'queryParams' parameter is mandatory");
        logger.finest("createJobAsync request with jobId " + jobId);
        return client.postAsync(Constants.EP_QUERY + "jobs", this.sw(cred), prepareCreateJob(jobId, queryParams),
                "content-type", "application/json").thenApply(this::processCreateJob);
    }

    private String prepareGetJobList(String tenantId, Long createdAfter, Integer maxJobs, QueryJobDetail.JobState state)
            throws IllegalArgumentException {
        if (tenantId == null || tenantId.isEmpty())
            throw new IllegalArgumentException("'tenantId' parameter is mandatory");
        HashMap<String, String> params = new HashMap<String, String>(4);
        params.put("tenantId", tenantId);
        if (createdAfter != null)
            params.put("createdAfter", createdAfter.toString());
        if (maxJobs != null)
            params.put("maxJobs", maxJobs.toString());
        if (state != null)
            params.put("state", state.jobState);
        return Constants.EP_QUERY + "jobs?" + Tools.querify(params);
    }

    private List<QueryJobDetail> processGetJobList(CortexApiResult<JsonStructure> response)
            throws QueryServiceRuntimeException, QueryServiceParseRuntimeException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw QueryServiceRuntimeException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonArray arrayResponse;
        try {
            arrayResponse = response.result.asJsonArray();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON array");
            throw new QueryServiceParseRuntimeException("response is not a valid JSON array");
        }
        return arrayResponse.getValuesAs(QueryJobDetail::parse);
    }

    /**
     * Retrieves a list of query jobs that match specified criteria. The retrieved
     * list of jobs is in chronological order, from most recent to oldest.
     * 
     * @param tenantId     Mandatory Tenant ID
     * @param createdAfter Lower limit job creation timestamp. Jobs are only listed
     *                     if they were created after the time identified here.
     *                     Value must be a Unix epoch timestamp.
     * @param maxJobs      Maximum number of jobs to list.
     * @param state        Return only jobs in the specified state.
     * @param cred         Optional credential tuple to override default one
     * @return The list of Query Job Details
     * @throws IllegalArgumentException   In case tennantId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     *                                    certificate location?)
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public List<QueryJobDetail> getJobsList(String tenantId, Long createdAfter, Integer maxJobs,
            QueryJobDetail.JobState state, CredentialTuple cred)
            throws QueryServiceParseException, QueryServiceException, IOException, InterruptedException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        CortexApiResult<JsonStructure> response = client.get(prepareGetJobList(tenantId, createdAfter, maxJobs, state),
                this.sw(cred), (String[]) null);
        logger.finest("getJobsList request for tenantId " + tenantId);
        try {
            return processGetJobList(response);
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        } catch (QueryServiceRuntimeException e) {
            throw QueryServiceException.fromException(e);
        }
    }

    /**
     * Retrieves a list of query jobs that match specified criteria. The retrieved
     * list of jobs is in chronological order, from most recent to oldest.
     * 
     * @param tenantId Mandatory Tenant ID
     * @param cred     Optional credential tuple to override default one
     * @return The list of Query Job Details
     * @throws IllegalArgumentException   In case tennantId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     *                                    certificate location?)
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public List<QueryJobDetail> getJobsList(String tenantId, CredentialTuple cred)
            throws QueryServiceParseException, QueryServiceException, IOException, InterruptedException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        return getJobsList(tenantId, null, null, null, cred);
    }

    /**
     * Retrieves a list of query jobs that match specified criteria. The retrieved
     * list of jobs is in chronological order, from most recent to oldest.
     * 
     * @param tenantId     Mandatory Tenant ID
     * @param createdAfter Lower limit job creation timestamp. Jobs are only listed
     *                     if they were created after the time identified here.
     *                     Value must be a Unix epoch timestamp.
     * @param maxJobs      Maximum number of jobs to list.
     * @param state        Return only jobs in the specified state.
     * @param cred         Optional credential tuple to override default one
     * @return The list of Query Job Details
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case tennantId is null.
     */
    public CompletableFuture<List<QueryJobDetail>> getJobsListAsync(String tenantId, Long createdAfter, Integer maxJobs,
            QueryJobDetail.JobState state, CredentialTuple cred)
            throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        logger.finest("getJobsListAsync request for tenantId " + tenantId);
        return client
                .getAsync(prepareGetJobList(tenantId, createdAfter, maxJobs, state), this.sw(cred), (String[]) null)
                .thenApply(this::processGetJobList);
    }

    /**
     * Retrieves a list of query jobs that match specified criteria. The retrieved
     * list of jobs is in chronological order, from most recent to oldest.
     * 
     * @param tenantId Mandatory Tenant ID
     * @param cred     Optional credential tuple to override default one
     * @return The list of Query Job Details
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case tennantId is null.
     */
    public CompletableFuture<List<QueryJobDetail>> getJobsListAsync(String tenantId, CredentialTuple cred)
            throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        return getJobsListAsync(tenantId, null, null, null, cred);
    }

    private String prepareJobById(String jobId) throws IllegalArgumentException {
        if (jobId == null || jobId.isEmpty())
            throw new IllegalArgumentException("'jobId' parameter is mandatory");
        return Constants.EP_QUERY + "jobs/" + jobId;
    }

    private QueryJobDetail processJobById(CortexApiResult<JsonStructure> response)
            throws QueryServiceRuntimeException, QueryServiceParseRuntimeException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw QueryServiceRuntimeException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonObject objectResponse;
        try {
            objectResponse = response.result.asJsonObject();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON object");
            throw new QueryServiceParseRuntimeException("response is not a valid JSON object");
        }
        return QueryJobDetail.parse(objectResponse);
    }

    /**
     * Returns detailed information about the query job. This information includes
     * the job’s current state, it’s submission and start times, the estimated
     * amount of work that has been completed, and the original query parameters
     * (SELECT statement, page size, and so forth.)
     * 
     * @param jobId ID of the query job for which you want to retrieve job
     *              information.
     * @param cred  Optional credential tuple to override default one
     * @return Job Details Object
     * @throws IllegalArgumentException   In case tennantId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public QueryJobDetail getJobStatus(String jobId, CredentialTuple cred)
            throws QueryServiceParseException, QueryServiceException, IOException, InterruptedException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        CortexApiResult<JsonStructure> response = client.get(prepareJobById(jobId), this.sw(cred), (String[]) null);
        logger.finest("getJobStatus request for jobId " + jobId);
        try {
            return processJobById(response);
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        } catch (QueryServiceRuntimeException e) {
            throw QueryServiceException.fromException(e);
        }
    }

    /**
     * Returns detailed information about the query job. This information includes
     * the job’s current state, it’s submission and start times, the estimated
     * amount of work that has been completed, and the original query parameters
     * (SELECT statement, page size, and so forth.)
     * 
     * @param jobId ID of the query job for which you want to retrieve job
     *              information.
     * @param cred  Optional credential tuple to override default one
     * @return CompletableFuture that resolves to a QueryJobDetails Object.
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case tennantId is null.
     */
    public CompletableFuture<QueryJobDetail> getJobStatusAsync(String jobId, CredentialTuple cred)
            throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        logger.finest("getJobStatus request for jobId " + jobId);
        return client.getAsync(prepareJobById(jobId), this.sw(cred), (String[]) null).thenApply(this::processJobById);
    }

    /**
     * Asks the query service to cancel the identified query job. A successful
     * response to this call does not guarantee that the job has been, or will be,
     * canceled.
     * 
     * @param jobId ID of the query job that you want to cancel. This ID is
     *              contained in the jobId response field that is returned when you
     *              create the query job.
     * @param cred  Optional credential tuple to override default one
     * @return void
     * @throws IllegalArgumentException   In case jobId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public QueryJobDetail deleteJob(String jobId, CredentialTuple cred)
            throws QueryServiceParseException, QueryServiceException, IOException, InterruptedException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        CortexApiResult<JsonStructure> response = client.delete(prepareJobById(jobId), this.sw(cred), (String[]) null);
        logger.finest("deleteJob request for jobId " + jobId);
        try {
            return processJobById(response);
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        } catch (QueryServiceRuntimeException e) {
            throw QueryServiceException.fromException(e);
        }
    }

    /**
     * Asks the query service to cancel the identified query job. A successful
     * response to this call does not guarantee that the job has been, or will be,
     * canceled.
     * 
     * @param jobId ID of the query job that you want to cancel. This ID is
     *              contained in the jobId response field that is returned when you
     *              create the query job.
     * @param cred  Optional credential tuple to override default one
     * @return Completable future that will resolve to void if the operation is
     *         competed successfully.
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case jobId is null.
     */
    public CompletableFuture<QueryJobDetail> deleteJobAsync(String jobId, CredentialTuple cred)
            throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        logger.finest("deleteJobAsync request for jobId " + jobId);
        return client.deleteAsync(prepareJobById(jobId), this.sw(cred), (String[]) null)
                .thenApply(this::processJobById);
    }

    private String prepareGetJobResults(String jobId, Integer maxWait, ResultFormat resultFormat, Integer pageSize,
            String pageCursor, Integer pageNumber, Integer offset) throws IllegalArgumentException {
        if (jobId == null || jobId.isEmpty())
            throw new IllegalArgumentException("'jobId' parameter is mandatory");
        HashMap<String, String> params = new HashMap<String, String>(6);
        if (maxWait != null)
            params.put("maxWait", maxWait.toString());
        if (resultFormat != null)
            params.put("resultFormat", resultFormat.name());
        if (pageSize != null)
            params.put("pageSize", pageSize.toString());
        if (pageCursor != null)
            params.put("pageCursor", pageCursor);
        if (pageNumber != null)
            params.put("pageNumber", pageNumber.toString());
        if (offset != null)
            params.put("offset", offset.toString());
        if (params.size() == 0)
            return Constants.EP_QUERY + "jobResults/" + jobId;
        return Constants.EP_QUERY + "jobResults/" + jobId + "?" + Tools.querify(params);
    }

    private QueryJobResult processGetJobResults(CortexApiResult<JsonStructure> response)
            throws QueryServiceRuntimeException, QueryServiceParseRuntimeException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw QueryServiceRuntimeException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonObject objectResponse;
        try {
            objectResponse = response.result.asJsonObject();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON object");
            throw new QueryServiceParseRuntimeException("response is not a valid JSON object");
        }
        return QueryJobResult.parse(objectResponse);
    }

    /**
     * Retrieve a page of query results for the query job identified by jobId.
     * 
     * Use pageSize to identify how many log records to return on the page you are
     * retrieving. There is no requirement to specify the same page size for each
     * subsequent job page retrieval. The query page you want to retrieve can be
     * identified either by pageNumber, cursor, or offset. It is an error to specify
     * more than one of these values. If you do not specify any of these values,
     * then the first page is returned. You have not retrieved the full query result
     * set until the response object state field is either Done or Failed. A state
     * of Pending indicates that the job is scheduled but has not yet started
     * running. A state of Running indicates that log records are not yet available
     * for the page that you are requesting.
     * 
     * Query results are returned as an array, one log record per array element. See
     * the resultFormat query parameter description, below, for log record
     * formatting options.
     * 
     * @param jobId        The ID of the job for which you want to retrieve a page
     *                     of results. This ID is contained in the jobId response
     *                     field that is returned when you create the query job.
     * @param maxWait      Maximum number of milliseconds you want the HTTP
     *                     connection to remain open waiting for a response. If the
     *                     requested page cannot be returned in this amount of time,
     *                     the service closes the connection without returning
     *                     results. This parameter’s maximum value is 2000 (2
     *                     seconds). If this parameter is 0, the HTTP connection is
     *                     closed immediately upon completion of the HTTP request.
     *                     Default value: 0
     * @param resultFormat Format of the retrieved log records. Log records are
     *                     returned in the response object’s page.result.result
     *                     array. Each element of this array is a single log record.
     *                     This parameter identifies each such record’s format.
     *                     Default value is valuesArray
     * @param pageSize     Number of log records you want retrieved for this
     *                     request. The value you specify here identifies the number
     *                     of records that will appear in the response object’s
     *                     page.result.result array. If you are retrieving the last
     *                     page in the job’s result set, this is the maximum number
     *                     of records that will appear in the page.result.result
     *                     array. Default: 10000
     * @param pageCursor   Cursor value to use for fetching this page. Each call to
     *                     this API contains a response object page.pageCursor
     *                     field. The value of this field is the cursor value that
     *                     you use to retrieve the next page. It is an error to use
     *                     this parameter with the pageNumber and/or offset
     *                     parameters.
     * @param pageNumber   Page number to fetch. Page numbers are calculated by
     *                     using the default page size (10000). If this parameter is
     *                     used, then pageSize is ignored.
     * @param offset       Log record number that you want to start this page with,
     *                     counting from the first log record in the result set.
     * @param cred         Optional credential tuple to override default one
     * @return a QueryJobResult object with the response.
     * @throws IllegalArgumentException   In case jobId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public QueryJobResult getJobResults(String jobId, Integer maxWait, ResultFormat resultFormat, Integer pageSize,
            String pageCursor, Integer pageNumber, Integer offset, CredentialTuple cred)
            throws IOException, InterruptedException, QueryServiceParseException, QueryServiceException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        if (pageCursor != null && pageNumber != null)
            throw new IllegalArgumentException("use either 'pageCursor' or 'pageNumber'");

        logger.finest("getJobResults request for jobId " + jobId);
        CortexApiResult<JsonStructure> response = client.get(
                prepareGetJobResults(jobId, maxWait, resultFormat, pageSize, pageCursor, pageNumber, offset),
                this.sw(cred), (String[]) null);
        try {
            return processGetJobResults(response);
        } catch (QueryServiceParseRuntimeException e) {
            throw new QueryServiceParseException(e.getMessage());
        } catch (QueryServiceRuntimeException e) {
            throw QueryServiceException.fromException(e);
        }
    }

    /**
     * Retrieve a page of query results for the query job identified by jobId.
     * 
     * Use pageSize to identify how many log records to return on the page you are
     * retrieving. There is no requirement to specify the same page size for each
     * subsequent job page retrieval. The query page you want to retrieve can be
     * identified either by pageNumber, cursor, or offset. It is an error to specify
     * more than one of these values. If you do not specify any of these values,
     * then the first page is returned. You have not retrieved the full query result
     * set until the response object state field is either Done or Failed. A state
     * of Pending indicates that the job is scheduled but has not yet started
     * running. A state of Running indicates that log records are not yet available
     * for the page that you are requesting.
     * 
     * Query results are returned as an array, one log record per array element. See
     * the resultFormat query parameter description, below, for log record
     * formatting options.
     * 
     * @param jobId      The ID of the job for which you want to retrieve a page of
     *                   results. This ID is contained in the jobId response field
     *                   that is returned when you create the query job.
     * @param pageSize   Number of log records you want retrieved for this request.
     *                   The value you specify here identifies the number of records
     *                   that will appear in the response object’s
     *                   page.result.result array. If you are retrieving the last
     *                   page in the job’s result set, this is the maximum number of
     *                   records that will appear in the page.result.result array.
     *                   Default: 10000
     * @param pageCursor Cursor value to use for fetching this page. Each call to
     *                   this API contains a response object page.pageCursor field.
     *                   The value of this field is the cursor value that you use to
     *                   retrieve the next page. It is an error to use this
     *                   parameter with the pageNumber and/or offset parameters.
     * @param cred       Optional credential tuple to override default one
     * @return a QueryJobResult object with the response.
     * @throws IllegalArgumentException   In case jobId is null.
     * @throws QueryServiceParseException When the Query Service API response does
     *                                    not conform to the expected interface.
     * @throws QueryServiceException      When the Query Service API request returns
     *                                    a 4xx or 5xx Status Code
     * @throws InterruptedException       Low level HTTP2Client issue
     * @throws IOException                Low level HTTP2Client issue (client
     * @throws URISyntaxException         unsupported usage of this object
     * @throws Http2FetchException        unsupported usage of this object
     */
    public QueryJobResult getJobResults(String jobId, Integer pageSize, String pageCursor, CredentialTuple cred)
            throws IOException, InterruptedException, QueryServiceParseException, QueryServiceException,
            IllegalArgumentException, Http2FetchException, URISyntaxException {
        return getJobResults(jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize, pageCursor, null,
                null, cred);
    }

    /**
     * Retrieve a page of query results for the query job identified by jobId.
     * 
     * Use pageSize to identify how many log records to return on the page you are
     * retrieving. There is no requirement to specify the same page size for each
     * subsequent job page retrieval. The query page you want to retrieve can be
     * identified either by pageNumber, cursor, or offset. It is an error to specify
     * more than one of these values. If you do not specify any of these values,
     * then the first page is returned. You have not retrieved the full query result
     * set until the response object state field is either Done or Failed. A state
     * of Pending indicates that the job is scheduled but has not yet started
     * running. A state of Running indicates that log records are not yet available
     * for the page that you are requesting.
     * 
     * Query results are returned as an array, one log record per array element. See
     * the resultFormat query parameter description, below, for log record
     * formatting options.
     * 
     * @param jobId        The ID of the job for which you want to retrieve a page
     *                     of results. This ID is contained in the jobId response
     *                     field that is returned when you create the query job.
     * @param maxWait      Maximum number of milliseconds you want the HTTP
     *                     connection to remain open waiting for a response. If the
     *                     requested page cannot be returned in this amount of time,
     *                     the service closes the connection without returning
     *                     results. This parameter’s maximum value is 2000 (2
     *                     seconds). If this parameter is 0, the HTTP connection is
     *                     closed immediately upon completion of the HTTP request.
     *                     Default value: 0
     * @param resultFormat Format of the retrieved log records. Log records are
     *                     returned in the response object’s page.result.result
     *                     array. Each element of this array is a single log record.
     *                     This parameter identifies each such record’s format.
     *                     Default value is valuesArray
     * @param pageSize     Number of log records you want retrieved for this
     *                     request. The value you specify here identifies the number
     *                     of records that will appear in the response object’s
     *                     page.result.result array. If you are retrieving the last
     *                     page in the job’s result set, this is the maximum number
     *                     of records that will appear in the page.result.result
     *                     array. Default: 10000
     * @param pageCursor   Cursor value to use for fetching this page. Each call to
     *                     this API contains a response object page.pageCursor
     *                     field. The value of this field is the cursor value that
     *                     you use to retrieve the next page. It is an error to use
     *                     this parameter with the pageNumber and/or offset
     *                     parameters.
     * @param pageNumber   Page number to fetch. Page numbers are calculated by
     *                     using the default page size (10000). If this parameter is
     *                     used, then pageSize is ignored.
     * @param offset       Log record number that you want to start this page with,
     *                     counting from the first log record in the result set.
     * @param cred         Optional credential tuple to override default one
     * @return a CompletableFuture that resolves to the QueryJobResult object with
     *         the response.
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case jobId is null.
     */
    public CompletableFuture<QueryJobResult> getJobResultsAsync(String jobId, Integer maxWait,
            ResultFormat resultFormat, Integer pageSize, String pageCursor, Integer pageNumber, Integer offset,
            CredentialTuple cred) throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        if (pageCursor != null && pageNumber != null)
            throw new IllegalArgumentException("use either 'pageCursor' or 'pageNumber'");

        logger.finest("getJobResultsAsync` request for jobId " + jobId);
        return client
                .getAsync(prepareGetJobResults(jobId, maxWait, resultFormat, pageSize, pageCursor, pageNumber, offset),
                        this.sw(cred), (String[]) null)
                .thenApply(this::processGetJobResults);
    }

    /**
     * Retrieve a page of query results for the query job identified by jobId.
     * 
     * Use pageSize to identify how many log records to return on the page you are
     * retrieving. There is no requirement to specify the same page size for each
     * subsequent job page retrieval. The query page you want to retrieve can be
     * identified either by pageNumber, cursor, or offset. It is an error to specify
     * more than one of these values. If you do not specify any of these values,
     * then the first page is returned. You have not retrieved the full query result
     * set until the response object state field is either Done or Failed. A state
     * of Pending indicates that the job is scheduled but has not yet started
     * running. A state of Running indicates that log records are not yet available
     * for the page that you are requesting.
     * 
     * Query results are returned as an array, one log record per array element. See
     * the resultFormat query parameter description, below, for log record
     * formatting options.
     * 
     * @param jobId      The ID of the job for which you want to retrieve a page of
     *                   results. This ID is contained in the jobId response field
     *                   that is returned when you create the query job.
     * @param pageSize   Number of log records you want retrieved for this request.
     *                   The value you specify here identifies the number of records
     *                   that will appear in the response object’s
     *                   page.result.result array. If you are retrieving the last
     *                   page in the job’s result set, this is the maximum number of
     *                   records that will appear in the page.result.result array.
     *                   Default: 10000
     * @param pageCursor Cursor value to use for fetching this page. Each call to
     *                   this API contains a response object page.pageCursor field.
     *                   The value of this field is the cursor value that you use to
     *                   retrieve the next page. It is an error to use this
     *                   parameter with the pageNumber and/or offset parameters.
     * @param cred       Optional credential tuple to override default one
     * @return a CompletableFuture that resolves to the QueryJobResult object with
     *         the response.
     * @throws URISyntaxException       unsupported usage of this object
     * @throws Http2FetchException      unsupported usage of this object
     * @throws IllegalArgumentException In case jobId is null.
     */
    public CompletableFuture<QueryJobResult> getJobResultsAsync(String jobId, Integer pageSize, String pageCursor,
            CredentialTuple cred) throws IllegalArgumentException, Http2FetchException, URISyntaxException {
        return getJobResultsAsync(jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize, pageCursor, null,
                null, cred);
    }
}