/**
 * SchemaService
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import org.apache.avro.SchemaBuilder;

/**
 * Low level Cortex Query Service API wrapper class. It provides methods that
 * mimic the service endpoints.
 * 
 * Developers would typically preefer the {@link QueryServiceClient} subclass
 * that provides an Iterable implementation to navigate through the query
 * results.
 */
public class SchemaService {
    private final Http2Fetch client;
    private static Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");

    /**
     * Low level Cortex Query Service API wrapper class
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
    public SchemaService(String keystore, char[] password, String defaultEntryPoint) throws UnrecoverableKeyException,
            KeyManagementException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        this.client = new Http2Fetch(keystore, password, defaultEntryPoint, null);
    }

    /**
     * Low level Cortex Schema Service API wrapper class
     * 
     * @param client Object to use to interface with the Cortex API End Point
     * @throws URISyntaxException Failed to construct a valid URI. (did you provide
     *                            the right baseFqdn value?)
     */
    public SchemaService(Http2Fetch client) throws URISyntaxException {
        this.client = client;
    }

    private SchemaServicePayload processGetSchema(CortexApiResult<JsonStructure> response)
            throws SchemaServiceException, SchemaServiceParseException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw SchemaServiceException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonObject objectResponse;
        try {
            objectResponse = response.result.asJsonObject();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON object");
            throw new SchemaServiceParseException("response is not a valid JSON object");
        }
        return SchemaServicePayload.parse(objectResponse);
    }

    public SchemaServicePayload get(String schemaId) throws IOException, InterruptedException, IllegalArgumentException,
            SchemaServiceException, SchemaServiceParseException, Http2FetchException, URISyntaxException {
        logger.finest("get request for shcema: " + schemaId);
        CortexApiResult<JsonStructure> response = client.get(Constants.EP_SCHEMA + schemaId, null);
        return processGetSchema(response);
    }

    private Collection<SchemaServicePayload> processGetAllSchemas(CortexApiResult<JsonStructure> response)
            throws SchemaServiceException, SchemaServiceParseException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw SchemaServiceException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonArray objectResponse;
        try {
            objectResponse = response.result.asJsonArray();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON array");
            throw new SchemaServiceParseException("response is not a valid JSON array");
        }
        var schemaArray = new ArrayList<SchemaServicePayload>(objectResponse.size());
        for (var schema : objectResponse) {
            SchemaServicePayload parsedSchema;
            try {
                parsedSchema = SchemaServicePayload.parse(schema.asJsonObject());
            } catch (Exception e) {
                logger.info("Failed to parse schema entry");
                parsedSchema = null;
            }
            schemaArray.add(parsedSchema);
        }
        return schemaArray;
    }

    public Collection<SchemaServicePayload> get() throws IOException, InterruptedException, IllegalArgumentException,
            SchemaServiceException, SchemaServiceParseException, Http2FetchException, URISyntaxException {
        logger.finest("get request for all schemas");
        CortexApiResult<JsonStructure> response = client.get(Constants.EP_SCHEMA, null);
        return processGetAllSchemas(response);
    }

    private BodyPublisher prepareCreate(SchemaServicePayload payload) {
        var mPointer = payload.metadata;
        JsonObjectBuilder partitionBuilder = Json.createObjectBuilder();
        partitionBuilder.add("timeUnit", mPointer.partitionScheme.timeUnit.toString());
        partitionBuilder.add("frequency", mPointer.partitionScheme.frequency);
        JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
        metadataBuilder.add("partitionColumn", mPointer.partitionColumn);
        if (mPointer.tags != null)
            metadataBuilder.add("tags", Json.createArrayBuilder(mPointer.tags));
        if (mPointer.timestampColumns != null)
            metadataBuilder.add("timestampColumns", Json.createArrayBuilder(mPointer.timestampColumns));
        if (mPointer.timestampFormat != null)
            metadataBuilder.add("timestampFormat", mPointer.timestampFormat);
        if (mPointer.timestampTimezone != null)
            metadataBuilder.add("timestampTimezone", mPointer.timestampTimezone);
        metadataBuilder.add("partitionScheme", partitionBuilder);
        metadataBuilder.add("public", mPointer.isPublic);
        metadataBuilder.add("derived", mPointer.derived);
        if (mPointer.idColumn != null)
            metadataBuilder.add("idColumn", mPointer.idColumn);
        if (mPointer.clusterColumns != null)
            metadataBuilder.add("clusterColumns", Json.createArrayBuilder(mPointer.clusterColumns));
        if (mPointer.doc != null)
            metadataBuilder.add("doc", mPointer.doc);
        metadataBuilder.add("operations", mPointer.operations.toString());
        if (mPointer.logical_types != null)
            metadataBuilder.add("logical_types", Json.createArrayBuilder(mPointer.logical_types));
        if (mPointer.memoryAllocation != null)
            metadataBuilder.add("memoryAllocation", Json.createArrayBuilder(mPointer.memoryAllocation));
        metadataBuilder.add("streamPartitionFactor", mPointer.streamPartitionFactor);
        JsonObjectBuilder jsonBody = Json.createObjectBuilder();
        jsonBody.add("schemaId", payload.schemaId);
        jsonBody.add("metadata", metadataBuilder);
        jsonBody.add("structure", SchemaBuilder.unionOf().type(payload.structure).endUnion().toString());
        var body = jsonBody.build().toString();
        logger.finer("HTTP2 request body: " + body);
        return BodyPublishers.ofString(body);
    }

    private SchemaResponse processCreate(CortexApiResult<JsonStructure> response)
            throws SchemaServiceException, SchemaServiceParseException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw SchemaServiceException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
        JsonObject objectResponse;
        try {
            objectResponse = response.result.asJsonObject();
        } catch (ClassCastException e) {
            logger.info("response is not a valid JSON object");
            throw new SchemaServiceParseException("response is not a valid JSON object");
        }
        return SchemaResponse.parse(objectResponse);
    }

    public SchemaResponse create(SchemaServicePayload payload) throws IOException, InterruptedException,
            SchemaServiceException, SchemaServiceParseException, Http2FetchException, URISyntaxException {
        if (payload == null || payload.metadata == null || payload.structure == null)
            throw new IllegalArgumentException("either 'payload' or its metadata or structure is null");
        logger.finest("create request for schema " + payload.schemaId);
        CortexApiResult<JsonStructure> response = client.post(Constants.EP_SCHEMA, null, prepareCreate(payload),
                "content-type", "application/json");
        return processCreate(response);
    }

    public SchemaResponse update(SchemaServicePayload payload) throws IOException, InterruptedException,
            SchemaServiceException, SchemaServiceParseException, Http2FetchException, URISyntaxException {
        if (payload == null || payload.metadata == null || payload.structure == null || payload.schemaId == null)
            throw new IllegalArgumentException("either 'payload' or its metadata or structure or schemaId is null");
        logger.finest("create request for schema " + payload.schemaId);
        CortexApiResult<JsonStructure> response = client.put(Constants.EP_SCHEMA + payload.schemaId, null,
                prepareCreate(payload), "content-type", "application/json");
        return processCreate(response);
    }

    private void processDelete(CortexApiResult<JsonStructure> response)
            throws SchemaServiceException, SchemaServiceParseException {
        if (response.statusCode >= 400) {
            logger.info("invalid response code " + response.statusCode);
            throw SchemaServiceException.factory(String.format("invalid response code %s", response.statusCode),
                    response.statusCode, response.result);
        }
    }

    public void delete(String schemaId) throws IOException, InterruptedException, SchemaServiceException,
            SchemaServiceParseException, Http2FetchException, URISyntaxException {
        if (schemaId == null)
            throw new IllegalArgumentException("schemaId can't be null");
        logger.finest("delete request for schema " + schemaId);
        CortexApiResult<JsonStructure> response = client.delete(Constants.EP_SCHEMA + schemaId, null, (String[]) null);
        processDelete(response);
    }
}
