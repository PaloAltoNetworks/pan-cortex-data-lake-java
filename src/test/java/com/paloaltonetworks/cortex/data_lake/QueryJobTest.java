package com.paloaltonetworks.cortex.data_lake;

import static org.junit.Assert.assertEquals;
import java.io.StringReader;
import javax.json.Json;

import org.junit.Test;

public class QueryJobTest {
    private static String RESPONSE = "{\"jobId\":\"876833d2-2c8d-4852-bdbd-66eea5f94acb\",\"uri\":\"/query/v2/jobs/876833d2-2c8d-4852-bdbd-66eea5f94acb\"}";
    private static String INVALID_JOBID = "{\"jobId\":18,\"uri\":\"/query/v2/jobs/876833d2-2c8d-4852-bdbd-66eea5f94acb\"}";
    private static String INVALID_URI = "{\"jobId\":\"876833d2-2c8d-4852-bdbd-66eea5f94acb\",\"uri\":18}";

    @Test
    public void constructor() {
        QueryJob qj = QueryJob.parse(Json.createReader(new StringReader(RESPONSE)).readObject());
        assertEquals("876833d2-2c8d-4852-bdbd-66eea5f94acb", qj.jobId);
        assertEquals("/query/v2/jobs/876833d2-2c8d-4852-bdbd-66eea5f94acb", qj.uri);
    }

    @Test(expected = QueryServiceParseRuntimeException.class)
    public void invalidJobId() {
        QueryJob.parse(Json.createReader(new StringReader(INVALID_JOBID)).readObject());
    }

    @Test(expected = QueryServiceParseRuntimeException.class)
    public void invalidUri() {
        QueryJob.parse(Json.createReader(new StringReader(INVALID_URI)).readObject());
    }
}