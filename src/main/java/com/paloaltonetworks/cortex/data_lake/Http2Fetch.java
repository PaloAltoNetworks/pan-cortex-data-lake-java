/**
 * Http2Fetch
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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest.BodyPublisher;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.KeyManagerFactory;
import javax.json.Json;
import javax.json.JsonStructure;
import javax.json.stream.JsonParsingException;

/**
 * Builds on top of {@link java.net.http.HttpClient} to implement a HTTP2
 * fetcher for Cortex API endpoints.
 */
public class Http2Fetch {
    static class UrlContext {
        String entryPoint;
        String authHeader;

        UrlContext(Function<Boolean, Map.Entry<String, String>> cred) {
            Map.Entry<String, String> credData = cred.apply(true);
            entryPoint = credData.getKey();
            authHeader = "Bearer " + credData.getValue();
        }
    }

    private final HttpClient client;
    private final Duration timeout;
    private final Map<String, UrlContext> urlContextCache;
    private final Function<Boolean, Map.Entry<String, String>> defCred;
    private final UrlContext defCredUrlContext;
    private final String defaultEntryPoint;
    private final Lock defLocker;
    private final Lock cacheLocker;
    private static Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Creates a mTLS HTTP2 fetcher using the certificate provided.
     * 
     * @param keystore          filename containing the client certificate
     * @param password          password to decrypt the client certificate (use null
     *                          for no encryption)
     * @param defaultEntryPoint fqdn for the Cortex Data Lake API to use (region)
     * @param timeout           Timeout passed to any request triggered by this
     *                          instance
     * @throws KeyStoreException         In case there is any issue processing
     *                                   certificates.
     * @throws NoSuchAlgorithmException  In case there is any issue processing
     *                                   certificates.
     * @throws CertificateException      In case there is any issue processing
     *                                   certificates.
     * @throws IOException               In case there is any issue opening the
     *                                   container file.
     * @throws UnrecoverableKeyException In case there is any issue processing
     *                                   certificates.
     * @throws KeyManagementException    In case there is any issue processing
     *                                   certificates.
     */
    public Http2Fetch(String keystore, char[] password, String defaultEntryPoint, Duration timeout)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, KeyManagementException {
        KeyStore ks = KeyStore.getInstance(new File(keystore), password);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
        kmf.init(ks, password);
        SSLContext sc = SSLContext.getInstance("TLS");
        // TODO allow trust manager and random generator to be provided in a constructor
        // overflow
        sc.init(kmf.getKeyManagers(), null, null);
        client = HttpClient.newBuilder().version(Version.HTTP_2).sslContext(sc).build();
        this.timeout = timeout;
        this.defaultEntryPoint = defaultEntryPoint;
        urlContextCache = new ConcurrentHashMap<String, UrlContext>();
        defCredUrlContext = null;
        defCred = null;
        defLocker = new ReentrantLock();
        cacheLocker = new ReentrantLock();
    }

    /**
     * Creates a JWT HTTP2 fetcher
     * 
     * @param cred     If not null then this object will be used as the credentials
     *                 for any request that does not override it.
     * @param timeout  Timeout passed to any request triggered by this instance
     * @param unsecure To change default behavior and trust any server certificate
     * @throws NoSuchAlgorithmException underlying SSL support issue
     * @throws KeyManagementException   underlying SSL support issues
     */
    public Http2Fetch(Function<Boolean, Map.Entry<String, String>> cred, Duration timeout, boolean unsecure)
            throws NoSuchAlgorithmException, KeyManagementException {
        this.timeout = timeout;
        this.defaultEntryPoint = null;
        urlContextCache = new ConcurrentHashMap<String, UrlContext>();
        if (cred != null) {
            defCred = cred;
            defCredUrlContext = new UrlContext(cred);
            logger.info("Updated authentication header for default data lake");
        } else {
            defCred = null;
            defCredUrlContext = null;
        }
        SSLContext sc = SSLContext.getInstance("TLS");
        if (unsecure) {
            TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            } };
            sc.init(null, trustAllCerts, null);
        } else {
            sc.init(null, null, null);
        }
        client = HttpClient.newBuilder().version(Version.HTTP_2).sslContext(sc).build();
        defLocker = new ReentrantLock();
        cacheLocker = new ReentrantLock();
    }

    /**
     * Creates an uninitialized fetcher
     * 
     * @param cred If not null then this object will be used as the credentials for
     *             any request that does not override it.
     * @throws NoSuchAlgorithmException underlying SSL support issue
     * @throws KeyManagementException   underlying SSL support issues
     */
    public Http2Fetch(Function<Boolean, Map.Entry<String, String>> cred)
            throws KeyManagementException, NoSuchAlgorithmException {
        this(cred, null, false);
    }

    /**
     * Creates an uninitialized fetcher
     * 
     * @throws NoSuchAlgorithmException underlying SSL support issue
     * @throws KeyManagementException   underlying SSL support issues
     */
    public Http2Fetch() throws KeyManagementException, NoSuchAlgorithmException {
        this(null, null, false);
    }

    /**
     * If you're planning to use this Http2Fetch object in a multi-thread
     * application then you must initialize in advance all credentials that will be
     * used to avoid race conditions in the lazy init procedure.
     * 
     * Please notice that the init process, itself, is not thread-safe.
     * 
     * @param ct credential tuple that should be initialized
     * @throws URISyntaxException  unsupported usage of this object
     * @throws Http2FetchException unsupported usage of this object
     */
    public void init(CredentialTuple ct) throws Http2FetchException, URISyntaxException {
        getRequest("", ct);
    }

    /**
     * If you're planning to use this Http2Fetch object in a multi-thread
     * application then you must initialize in advance its default credentials
     * object.
     * 
     * @throws URISyntaxException  unsupported usage of this object
     * @throws Http2FetchException unsupported usage of this object
     */
    public void init() throws Http2FetchException, URISyntaxException {
        getRequest("", null);
    }

    private Builder getRequest(String path, CredentialTuple ct) throws Http2FetchException, URISyntaxException {
        if (ct == null && defCred == null) {
            if (defaultEntryPoint == null) {
                throw new Http2FetchException("object does not have default entry point neither default credential");
            }
            return null;
        }
        Builder reqBuilder = null;
        if (ct != null) {
            UrlContext cacheEntry = urlContextCache.get(ct.dlid);
            if (cacheLocker.tryLock()) {
                try {
                    if (cacheEntry == null) {
                        cacheEntry = new UrlContext(ct.cred);
                        urlContextCache.put(ct.dlid, cacheEntry);
                        logger.info("Updated authentication header for data lake " + ct.dlid);
                    } else {
                        Map.Entry<String, String> credData = ct.cred.apply(false);
                        if (credData != null) {
                            cacheEntry.authHeader = "Bearer " + credData.getValue();
                            logger.info("Updated authentication header for data lake " + ct.dlid);
                        }
                    }
                } finally {
                    cacheLocker.unlock();
                }
            }
            if (cacheEntry == null) {
                throw new Http2FetchException("race condition: use init(cred) before entering into parallel mode");
            }
            reqBuilder = HttpRequest.newBuilder(new URI("https://" + cacheEntry.entryPoint + path));
            reqBuilder.header("authorization", cacheEntry.authHeader);
        }
        if (ct == null && defCred != null) {
            if (defLocker.tryLock()) {
                try {
                    Map.Entry<String, String> credData = defCred.apply(false);
                    if (credData != null) {
                        defCredUrlContext.authHeader = "Bearer " + credData.getValue();
                        logger.info("Updated authentication header for default data lake");
                    }
                } finally {
                    defLocker.unlock();
                }
            }
            reqBuilder = HttpRequest.newBuilder(new URI("https://" + defCredUrlContext.entryPoint + path));
            reqBuilder.header("authorization", defCredUrlContext.authHeader);
        }
        if (reqBuilder == null) {
            reqBuilder = HttpRequest.newBuilder(new URI("https://" + defaultEntryPoint + path));
        }
        if (timeout != null)
            reqBuilder.timeout(timeout);
        return reqBuilder;
    }

    private CortexApiResult<JsonStructure> op(HttpRequest request) throws InterruptedException, IOException {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        logger.finer("HTTP2 response status code: " + response.statusCode());
        String responseBody = response.body();
        if (responseBody.equals("null")) {
            logger.finer("HTTP2 response body is null");
            return null;
        }
        logger.finer("HTTP2 response body: " + responseBody);
        try {
            JsonStructure jsobj = Json.createReader(new StringReader(responseBody)).read();
            return new CortexApiResult<JsonStructure>(jsobj, response.statusCode());
        } catch (JsonParsingException e) {
            logger.info("CORTEX response is not a valid JSON object: " + responseBody);
            return new CortexApiResult<JsonStructure>(null, response.statusCode());
        }
    }

    private CompletableFuture<CortexApiResult<JsonStructure>> opAsync(HttpRequest request) {
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            logger.finer("HTTP2 response status code: " + response.statusCode());
            String responseBody = response.body();
            if (responseBody.equals("null")) {
                logger.finer("HTTP2 response body is null");
                return null;
            }
            logger.finer("HTTP2 response body: " + responseBody);
            JsonStructure jsobj = Json.createReader(new StringReader(responseBody)).read();
            return new CortexApiResult<JsonStructure>(jsobj, response.statusCode());
        });
    }

    CortexApiResult<JsonStructure> get(String path, CredentialTuple ct)
            throws Http2FetchException, URISyntaxException, InterruptedException, IOException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.GET().build();
        Http2Fetch.logger.fine(String.format("GET op to %s", request.uri().toString()));
        return op(request);
    }

    CortexApiResult<JsonStructure> get(String path, CredentialTuple ct, String... headers)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.GET().build();
        Http2Fetch.logger.fine(String.format("GET op to %s", request.uri().toString()));
        return op(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> getAsync(String path, CredentialTuple ct)
            throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.GET().build();
        Http2Fetch.logger.fine(String.format("GET op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> getAsync(String path, CredentialTuple ct, String... headers)
            throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.GET().build();
        Http2Fetch.logger.fine(String.format("GET op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CortexApiResult<JsonStructure> delete(String path, CredentialTuple ct)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.DELETE().build();
        Http2Fetch.logger.fine(String.format("DELETE op to %s", request.uri().toString()));
        return op(request);
    }

    CortexApiResult<JsonStructure> delete(String path, CredentialTuple ct, String... headers)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.DELETE().build();
        Http2Fetch.logger.fine(String.format("DELETE op to %s", request.uri().toString()));
        return op(reqBuilder.build());
    }

    CompletableFuture<CortexApiResult<JsonStructure>> deleteAsync(String path, CredentialTuple ct)
            throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.DELETE().build();
        Http2Fetch.logger.fine(String.format("DELETE op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> deleteAsync(String path, CredentialTuple ct, String... headers)
            throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.DELETE().build();
        Http2Fetch.logger.fine(String.format("DELETE op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CortexApiResult<JsonStructure> post(String path, CredentialTuple ct, BodyPublisher publisher)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.POST(publisher).build();
        Http2Fetch.logger.fine(String.format("POST op to %s", request.uri().toString()));
        return op(request);
    }

    CortexApiResult<JsonStructure> post(String path, CredentialTuple ct, BodyPublisher publisher, String... headers)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.POST(publisher).build();
        Http2Fetch.logger.fine(String.format("POST op to %s", request.uri().toString()));
        return op(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> postAsync(String path, CredentialTuple ct,
            BodyPublisher publisher) throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.POST(publisher).build();
        Http2Fetch.logger.fine(String.format("POST op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> postAsync(String path, CredentialTuple ct,
            BodyPublisher publisher, String... headers) throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.POST(publisher).build();
        Http2Fetch.logger.fine(String.format("POST op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CortexApiResult<JsonStructure> put(String path, CredentialTuple ct, BodyPublisher publisher)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.PUT(publisher).build();
        Http2Fetch.logger.fine(String.format("PUT op to %s", request.uri().toString()));
        return op(request);
    }

    CortexApiResult<JsonStructure> put(String path, CredentialTuple ct, BodyPublisher publisher, String... headers)
            throws IOException, InterruptedException, Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.PUT(publisher).build();
        Http2Fetch.logger.fine(String.format("PUT op to %s", request.uri().toString()));
        return op(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> putAsync(String path, CredentialTuple ct, BodyPublisher publisher)
            throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        HttpRequest request = reqBuilder.PUT(publisher).build();
        Http2Fetch.logger.fine(String.format("PUT op to %s", request.uri().toString()));
        return opAsync(request);
    }

    CompletableFuture<CortexApiResult<JsonStructure>> putAsync(String path, CredentialTuple ct, BodyPublisher publisher,
            String... headers) throws Http2FetchException, URISyntaxException {
        Builder reqBuilder = getRequest(path, ct);
        if (headers != null)
            reqBuilder.headers(headers);
        HttpRequest request = reqBuilder.PUT(publisher).build();
        Http2Fetch.logger.fine(String.format("PUT op to %s", request.uri().toString()));
        return opAsync(request);
    }
}