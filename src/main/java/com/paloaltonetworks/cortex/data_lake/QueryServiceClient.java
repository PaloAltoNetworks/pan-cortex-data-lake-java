/**
 * QueryServiceClient
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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.json.JsonValue;

/**
 * High level class to navigate Cortex API Query results.
 * 
 * Developers might be interested in its iterable and stream methods.
 */
public class QueryServiceClient extends QueryService {

    private static final Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * High Level Cortex Query Service subclass to manage queries as Collections.
     * Constructor that generates an underlying mTLS Http2Fetcher to be used by any
     * operation triggered from this object.
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
    public QueryServiceClient(String keystore, char[] password, String defaultEntryPoint)
            throws UnrecoverableKeyException, KeyManagementException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException {
        super(keystore, password, defaultEntryPoint);
    }

    /**
     * High Level Cortex Query Service subclass to manage queries as Collections.
     * Constructor that allows reusing an existing Http2Fetch object.
     * 
     * @param client Object to use to interface with the Cortex API End Point
     * @param cred   default credentails to be used
     */
    public QueryServiceClient(Http2Fetch client, CredentialTuple cred) {
        super(client, cred);
    }

    /**
     * High Level Cortex Query Service subclass to manage queries as Collections.
     * Constructor that creates an underlying JWT Http2Fetch to be used by
     * operations triggered from this object
     * 
     * @param cred default credentails to be used
     * @throws KeyManagementException   Issues with the local OS SSL Libraries.
     * @throws NoSuchAlgorithmException Issues with the local OS SSL Libraries.
     */
    public QueryServiceClient(Function<Boolean, Map.Entry<String, String>> cred)
            throws KeyManagementException, NoSuchAlgorithmException {
        super(cred);
    }

    /**
     * Constructs an Iterable object to navigate a Cortex API Query.
     * 
     * @param sqlCommand the SQL command for this job.
     * @param pageSize   page size to use.
     * @param delay      delay (milliseconds) to wait for the job to settle.
     * @param retries    number of attempts to check for job to be completed.
     * @param cred       Optional credential tuple to override default one
     * @return an Iterable object to navigate the query results.
     */
    public QueryIterable iterable(String sqlCommand, Integer pageSize, Integer delay, Integer retries,
            CredentialTuple cred) {
        if (sqlCommand == null) {
            logger.info("'sqlCommand' can't be null.");
            throw new IllegalArgumentException("'sqlCommand' can't be null.");
        }
        return new QueryIterable(this, sqlCommand, pageSize, delay, retries, cred);
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
     * @param sqlCommand the SQL command for this job.
     * @param cred       Optional credential tuple to override default one
     * @return an Iterable object to navigate the query results.
     */
    public QueryIterable iterable(String sqlCommand, CredentialTuple cred) {
        return new QueryIterable(this, sqlCommand, cred);
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
     * @param sqlCommand the SQL command for this job.
     * @return an Iterable object to navigate the query results.
     */
    public QueryIterable iterable(String sqlCommand) {
        return new QueryIterable(this, sqlCommand, null);
    }

    /**
     * Constructs a stream object to allow a parallel processing of items produced
     * by a query
     * 
     * @param sqlCommand the SQL command for this job.
     * @param pageSize   page size to use.
     * @param delay      delay (milliseconds) to wait for the job to settle.
     * @param retries    number of attempts to check for job to be completed.
     * @param cred       Optional credential tuple to override default one
     * @return an object that implements the parallel Stream interface
     */
    public Stream<JsonValue> stream(String sqlCommand, Integer pageSize, Integer delay, Integer retries,
            CredentialTuple cred) {
        if (sqlCommand == null) {
            logger.info("'sqlCommand' can't be null.");
            throw new IllegalArgumentException("'sqlCommand' can't be null.");
        }
        return StreamSupport.stream(iterable(sqlCommand, pageSize, delay, retries, cred).spliterator(), true);
    }

    /**
     * Constructs a stream object to allow a parallel processing of items produced
     * by a query with defaults values:
     * 
     * Defaults are:
     * <ul>
     * <li>DEFAULT_PAGE_SIZE = 400</li>
     * <li>MAX_RETRIES = 10</li>
     * <li>DEFAULT_DELAY = 200</li>
     * </ul>
     * 
     * @param sqlCommand the SQL command for this job.
     * @param cred       Optional credential tuple to override default one
     * @return an object that implements the parallel Stream interface
     */
    public Stream<JsonValue> stream(String sqlCommand, CredentialTuple cred) {
        if (sqlCommand == null) {
            logger.info("'sqlCommand' can't be null.");
            throw new IllegalArgumentException("'sqlCommand' can't be null.");
        }
        return StreamSupport.stream(iterable(sqlCommand, cred).spliterator(), true);
    }

    /**
     * Constructs a stream object to allow a parallel processing of items produced
     * by a query with defaults values:
     * 
     * Defaults are:
     * <ul>
     * <li>DEFAULT_PAGE_SIZE = 400</li>
     * <li>MAX_RETRIES = 10</li>
     * <li>DEFAULT_DELAY = 200</li>
     * </ul>
     * 
     * @param sqlCommand the SQL command for this job.
     * @return an object that implements the parallel Stream interface
     */
    public Stream<JsonValue> stream(String sqlCommand) {
        if (sqlCommand == null) {
            logger.info("'sqlCommand' can't be null.");
            throw new IllegalArgumentException("'sqlCommand' can't be null.");
        }
        return StreamSupport.stream(iterable(sqlCommand, null).spliterator(), true);
    }
}