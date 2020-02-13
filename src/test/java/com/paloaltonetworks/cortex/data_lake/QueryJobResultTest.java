package com.paloaltonetworks.cortex.data_lake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.StringReader;
import javax.json.Json;

import org.junit.Test;

public class QueryJobResultTest {
    private final String BASIC_RESPONSE = "{\"jobId\":\"86d0d782-5ad2-442e-ab1b-67dee405382f\",\"state\":\"DONE\",\"rowsInJob\":10,\"rowsInPage\":11,\"resultFormat\":\"valuesArray\",\"schema\":{\"fields\":[]},\"page\":{\"pageCursor\":\"pagecursorstr\",\"result\":{\"data\":[]}}}";
    private final String MIN_RESPONSE = "{\"jobId\":\"86d0d782-5ad2-442e-ab1b-67dee405382f\",\"state\":\"DONE\",\"resultFormat\":\"valuesArray\",\"page\":{\"pageCursor\":null,\"result\":{\"data\":null}}}";
    private final String INVALID_RESPONSE = "{\"jobId\":18,\"state\":\"DONE\",\"resultFormat\":\"valuesArray\",\"page\":{\"pageCursor\":null,\"result\":{\"data\":null}}}";

    @Test
    public void parseBasicTest() {
        QueryJobResult qrj = QueryJobResult.parse(Json.createReader(new StringReader(BASIC_RESPONSE)).readObject());
        assertEquals("86d0d782-5ad2-442e-ab1b-67dee405382f", qrj.jobId);
        assertEquals(QueryJobDetail.JobState.DONE, qrj.state);
        assertEquals(Integer.valueOf(10), qrj.rowsInJob);
        assertEquals(Integer.valueOf(11), qrj.rowsInPage);
        assertEquals(QueryJobResult.ResultFormat.valuesArray, qrj.resultFormat);
        assertEquals("pagecursorstr", qrj.page.pageCursor);
        try {
            assertTrue(qrj.schema.fields.size() == 0);
            assertTrue(qrj.page.result.data.size() == 0);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void parseMinTest() {
        QueryJobResult qrj = QueryJobResult.parse(Json.createReader(new StringReader(MIN_RESPONSE)).readObject());
        assertEquals("86d0d782-5ad2-442e-ab1b-67dee405382f", qrj.jobId);
        assertEquals(QueryJobDetail.JobState.DONE, qrj.state);
        assertNull(qrj.rowsInJob);
        assertNull(qrj.rowsInPage);
        assertEquals(QueryJobResult.ResultFormat.valuesArray, qrj.resultFormat);
        assertNull(qrj.schema);
        try {
            assertNull(qrj.page.pageCursor);
            assertNull(qrj.page.result.data);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test(expected = QueryServiceParseRuntimeException.class)
    public void parseInvalid() {
        QueryJobResult.parse(Json.createReader(new StringReader(INVALID_RESPONSE)).readObject());
        fail("Expected exception not thrown");
    }
}
