/**
 * QueryIterable
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.UUID;
import java.util.logging.Logger;
import javax.json.JsonValue;
import com.paloaltonetworks.cortex.data_lake.QueryJobDetail.JobState;

/**
 * A Cortex API Query iterable object with insights in the underlying job.
 * 
 * method iteratorException() is late bound. It will return null until either an
 * iterator or a spliterator is instantiated from this iterable and the
 * corresponding exception is thrown.
 */
public class QueryIterable implements Iterable<JsonValue> {

    private static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");
    private static final int DEFAULT_PAGE_SIZE = 400;
    private static final int MAX_RETRIES = 10;
    private static final int DEFAULT_DELAY = 200;
    private final String sqlCommand;
    private final int delay;
    private final int retries;
    private final CredentialTuple cred;
    private static final String errMsg1 = "Can't iterate on a page with null result data.";
    private Integer size = null;
    private Exception iteratorException = null;
    private int iterator = 0;
    final QueryService qs;
    String jobId = null;
    final int pageSize;
    QueryJobResult preloadPageResults = null;
    Iterator<JsonValue> preloadPageIterator = null;

    /**
     * Constructs an Iterable object to navigate a Cortex API Query.
     * 
     * @param qs       Query Service object to be used
     * @param sqlCmd   the SQL command for this job.
     * @param pageSize page size to use.
     * @param delay    delay (milliseconds) to wait for the job to settle.
     * @param retries  number of attempts to check for job to be completed.
     * @param cred     default credentails to be used
     * @return an Iterable object to navigate the query results.
     */
    QueryIterable(QueryService qs, String sqlCommand, Integer pageSize, Integer delay, Integer retries,
            CredentialTuple cred) {
        this.qs = qs;
        this.sqlCommand = sqlCommand;
        this.pageSize = (pageSize == null) ? DEFAULT_PAGE_SIZE : pageSize;
        this.delay = (delay == null) ? DEFAULT_DELAY : delay;
        this.retries = (retries == null) ? MAX_RETRIES : retries;
        this.cred = cred;
    }

    /**
     * Constructs an Iterable object to navigate a Cortex API Query with default
     * values.
     * 
     * Defaults are:
     * <ul>
     * <li>DEFAULT_PAGE_SIZE = 400</li>
     * <li>MAX_RETRIES = 10</li>
     * <li>DEFAULT_DELAY = 200</li>
     * </ul>
     * 
     * @param qs     Query Service object to be used
     * @param sqlCmd the SQL command for this job.
     * @param cred   default credentails to be used
     * @return an Iterable object to navigate the query results.
     */
    QueryIterable(QueryService qs, String sqlCommand, CredentialTuple cred) {
        this(qs, sqlCommand, null, null, null, cred);
    }

    private QueryJobResult settleJobResult(QueryJobResult pageResults) throws QueryServiceClientException {
        size = pageResults.rowsInJob;
        if (pageResults.page.result.data == null) {
            logger.info(errMsg1);
            iteratorException = new QueryServiceClientException(errMsg1);
            throw (QueryServiceClientException) iteratorException;
        }
        return pageResults;
    }

    QueryJobResult loadPage(String pageCursor)
            throws InterruptedException, QueryServiceParseException, QueryServiceException, QueryServiceClientException,
            IOException, IllegalArgumentException, Http2FetchException, URISyntaxException {
        return qs.getJobResults(jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize, pageCursor, null,
                null, this.cred);
    }

    QueryJobResult loadPage(int pageNum)
            throws InterruptedException, QueryServiceParseException, QueryServiceException, QueryServiceClientException,
            IOException, IllegalArgumentException, Http2FetchException, URISyntaxException {
        return qs.getJobResults(jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize, null, pageNum,
                null, this.cred);
    }

    synchronized QueryJobResult lazyInit()
            throws IllegalArgumentException, InterruptedException, QueryServiceParseException, QueryServiceException,
            QueryServiceClientException, IOException, Http2FetchException, URISyntaxException {
        if (jobId != null) {
            return null; // No need to initialize the job.
        }
        jobId = qs.createJob(UUID.randomUUID().toString(),
                new QueryParams(sqlCommand, null, null, null, null, pageSize), this.cred).jobId;
        JobState state = qs.getJobStatus(jobId, this.cred).state;
        int attempts = 0;
        while ((state == JobState.PENDING || state == JobState.RUNNING) && attempts++ < retries) {
            Thread.sleep(delay);
            state = qs.getJobStatus(jobId, this.cred).state;
        }
        if (attempts >= retries) {
            String msg = String.format("JobId %s still in status %s after %s attempts", jobId, state, attempts);
            logger.info(msg);
            iteratorException = new QueryServiceClientException(msg);
            throw (QueryServiceClientException) iteratorException;
        }
        if (state != JobState.DONE) {
            String msg = String.format("JobId %s failed with status %s", jobId, state);
            logger.info(msg);
            iteratorException = new QueryServiceClientException(msg);
            throw (QueryServiceClientException) iteratorException;
        }
        return settleJobResult(loadPage(null));
    }

    /**
     * Object that can be used to iterate over the entries on this query job
     * results.
     */
    @Override
    public Iterator<JsonValue> iterator() {
        iteratorStarted();
        return new Iterator<JsonValue>() {

            private boolean endSignal = false;
            private QueryJobResult pageResults = preloadPageResults;
            private Iterator<JsonValue> pageIterator = preloadPageIterator;

            private void iteratorPreLoad() throws IllegalArgumentException, InterruptedException,
                    QueryServiceParseException, QueryServiceException, QueryServiceClientException, IOException,
                    Http2FetchException, URISyntaxException {
                var pr = lazyInit();
                if (pr != null) { // lazy init created a new job and we must store the first page result.
                    pageResults = pr;
                    pageIterator = pr.page.result.data.iterator();
                }
                if (pageResults.page.pageCursor != null && !pageIterator.hasNext()) {
                    pageResults = loadPage(pageResults.page.pageCursor);
                    pageIterator = pageResults.page.result.data.iterator();
                }
            }

            private void endTraker() {
                if (!endSignal) {
                    endSignal = true;
                    iteratorEnded();
                }
            }

            @Override
            public boolean hasNext() {
                try {
                    iteratorPreLoad();
                } catch (QueryServiceClientException e) {
                    endTraker();
                    return false;
                } catch (Exception e) {
                    iteratorException = e;
                    logger.info("Failed preLoad() due to: " + e.getMessage());
                    return false;
                }
                if (pageIterator.hasNext())
                    return true;
                endTraker();
                return false;
            }

            @Override
            public JsonValue next() {
                try {
                    iteratorPreLoad();
                } catch (QueryServiceClientException e) {
                    endTraker();
                    throw new NoSuchElementException();
                } catch (Exception e) {
                    iteratorException = e;
                    logger.info("Failed lazyInit() due to: " + e.getMessage());
                    throw new NoSuchElementException();
                }
                if (pageIterator.hasNext())
                    return pageIterator.next();
                endTraker();
                throw new NoSuchElementException();
            }
        };
    }

    @Override
    public Spliterator<JsonValue> spliterator() {
        iteratorStarted();
        return new QuerySpliterator(this);
    }

    synchronized void iteratorStarted() {
        iterator++;
    }

    synchronized void iteratorEnded() {
        iterator--;
        if (iterator == 0) {
            try {
                qs.deleteJob(jobId, this.cred);
            } catch (Exception e) {
                logger.info("Failed deleteJob() due to: " + e.getMessage());
            } finally {
                jobId = null;
            }
        }
    }

    private void iterablePreload() {
        try {
            var pr = lazyInit();
            if (pr != null) { // lazy init created a new job and we must store the first page result.
                preloadPageResults = pr;
                preloadPageIterator = pr.page.result.data.iterator();
                jobId = pr.jobId;
                size = pr.rowsInJob;
            }
        } catch (Exception e) {
            iteratorException = e;
        }
    }

    /**
     * Underlaying jobId used for the query.
     * 
     * @return the jobId
     */
    public String jobId() {
        if (jobId == null)
            iterablePreload();
        return jobId;
    }

    /**
     * Amount of records produced by the query job.
     * 
     * The value won't be available until its iterator hasNext() or next() is called
     * due to its lazy initialization strategy.
     * 
     * @return the amount of records produced by the query.
     */
    public Integer size() {
        if (size == null)
            iterablePreload();
        return size;
    }

    /**
     * The underlying exception object that forced the iteration to be truncated.
     * 
     * @return underlying exception
     */
    public Exception iteratorException() {
        return iteratorException;
    }
}