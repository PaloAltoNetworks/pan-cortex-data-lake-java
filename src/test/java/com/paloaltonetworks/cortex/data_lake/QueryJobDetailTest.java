package com.paloaltonetworks.cortex.data_lake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.io.StringReader;
import javax.json.Json;

import org.junit.Test;

public class QueryJobDetailTest {
    private final static String SQL_COMMAND = "SELECT * from `2020001.firewall.traffic` LIMIT 10";
    private final static String JOB_COMPLETE = "{\"jobId\":\"str1\",\"state\":\"DONE\",\"submitTime\":10,\"startTime\":11,\"endTime\":12,\"progress\":{\"completionPct\":13},\"params\":{\"query\":\""
            + SQL_COMMAND
            + "\",\"dialect\":\"csql\",\"properties\":{\"priority\":\"foreground\",\"timeoutMs\":10,\"maxWait\":11,\"defaultPageSize\":12}},\"statistics\":{\"runTimeMs\":14,\"cachePct\":15,\"etaMs\":16}}";
    private final static String JOB_PARTIAL = "{\"jobId\":\"str1\",\"state\":\"DONE\",\"submitTime\":10,\"progress\":{\"completionPct\":13},\"params\":{\"query\":\""
            + SQL_COMMAND + "\"},\"statistics\":{\"runTimeMs\":14}}";
    private final static String JOB_MIN = "{\"jobId\":\"str1\",\"state\":\"DONE\",\"submitTime\":10,\"params\":{\"query\":\""
            + SQL_COMMAND + "\"}}";
    private final static String JOB_INVALID = "{\"jobId\":\"str1\",\"state\":\"DONE\",\"submitTime\":\"10\",\"params\":{\"query\":\""
            + SQL_COMMAND + "\"}}";

    @Test
    public void parserComplete() {
        try {
            QueryJobDetail qjd = QueryJobDetail.parse(Json.createReader(new StringReader(JOB_COMPLETE)).readObject());
            assertEquals("str1", qjd.jobId);
            assertEquals(Long.valueOf(10), Long.valueOf(qjd.submitTime));
            assertEquals(Long.valueOf(11), qjd.startTime);
            assertEquals(Long.valueOf(12), qjd.endTime);
            if (qjd.progress == null) {
                fail("'progress' property shouldn't be null");
                return;
            }
            assertEquals(Integer.valueOf(13), qjd.progress.completionPct);
            if (qjd.params == null) {
                fail("'params' propery shouldn't be null");
                return;
            }
            assertEquals(SQL_COMMAND, qjd.params.query);
            assertEquals("csql", qjd.params.dialect);
            assertEquals(QueryParams.Priority.foreground, qjd.params.priority);
            assertEquals(Integer.valueOf(10), qjd.params.timeoutMs);
            assertEquals(Integer.valueOf(11), qjd.params.maxWait);
            assertEquals(Integer.valueOf(12), qjd.params.defaultPageSize);
            if (qjd.statistics == null) {
                fail("'statistics' propery shouldn't be null");
                return;
            }
            assertEquals(Integer.valueOf(14), qjd.statistics.runTimeMs);
            assertEquals(Integer.valueOf(15), qjd.statistics.cachePct);
            assertEquals(Integer.valueOf(16), qjd.statistics.etaMs);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void parserPartial() {
        try {
            QueryJobDetail qjd = QueryJobDetail.parse(Json.createReader(new StringReader(JOB_PARTIAL)).readObject());
            assertEquals("str1", qjd.jobId);
            assertEquals(Long.valueOf(10), Long.valueOf(qjd.submitTime));
            assertEquals(null, qjd.endTime);
            if (qjd.progress == null) {
                fail("'progress' property shouldn't be null");
                return;
            }
            assertEquals(Integer.valueOf(13), qjd.progress.completionPct);
            if (qjd.params == null) {
                fail("'params' propery shouldn't be null");
                return;
            }
            assertEquals(SQL_COMMAND, qjd.params.query);
            assertEquals(null, qjd.params.dialect);
            assertEquals(null, qjd.params.priority);
            assertEquals(null, qjd.params.timeoutMs);
            assertEquals(null, qjd.params.maxWait);
            assertEquals(null, qjd.params.defaultPageSize);
            if (qjd.statistics == null) {
                fail("'statistics' propery shouldn't be null");
                return;
            }
            assertEquals(Integer.valueOf(14), qjd.statistics.runTimeMs);
            assertEquals(null, qjd.statistics.cachePct);
            assertEquals(null, qjd.statistics.etaMs);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void parsereMin() {
        try {
            QueryJobDetail qjd = QueryJobDetail.parse(Json.createReader(new StringReader(JOB_MIN)).readObject());
            assertEquals("str1", qjd.jobId);
            assertEquals(Long.valueOf(10), Long.valueOf(qjd.submitTime));
            assertEquals(null, qjd.endTime);
            assertEquals(null, qjd.progress);
            if (qjd.params == null) {
                fail("'params' propery shouldn't be null");
                return;
            }
            assertEquals(SQL_COMMAND, qjd.params.query);
            assertEquals(null, qjd.params.dialect);
            assertEquals(null, qjd.params.priority);
            assertEquals(null, qjd.params.timeoutMs);
            assertEquals(null, qjd.params.maxWait);
            assertEquals(null, qjd.params.defaultPageSize);
            assertEquals(null, qjd.statistics);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = QueryServiceParseRuntimeException.class)
    public void parseInvalid() {
        QueryJobDetail.parse(Json.createReader(new StringReader(JOB_INVALID)).readObject());
    }
}
