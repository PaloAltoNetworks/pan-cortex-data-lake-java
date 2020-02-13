package com.paloaltonetworks.cortex.data_lake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import java.io.StringReader;
import javax.json.Json;
import org.junit.Assert;
import org.junit.Test;

public class QueryParamsTest {
    private final static String SQL_COMMAND = "SELECT * from `2020001.firewall.traffic` LIMIT 10";
    private final static String PARAMS_COMPLETE = "{\"query\":\"" + SQL_COMMAND
            + "\",\"dialect\":\"csql\",\"properties\":{\"priority\":\"foreground\",\"timeoutMs\":10,\"maxWait\":11,\"defaultPageSize\":12}}";
    private final static String PARAMS_SHORT = "{\"query\":\"" + SQL_COMMAND + "\"}";

    @Test
    public void queryBuilder() {
        long okTests = 0;
        String queryParams = new QueryParams(SQL_COMMAND, "csql", QueryParams.Priority.foreground, 10, 11, 12).toJson()
                .toString();
        if (PARAMS_COMPLETE.equals(queryParams))
            okTests++;
        queryParams = new QueryParams(SQL_COMMAND, null, null, null, null, null).toJson().toString();
        if (PARAMS_SHORT.equals(queryParams))
            okTests++;
        queryParams = new QueryParams(SQL_COMMAND).toJson().toString();
        if (PARAMS_SHORT.equals(queryParams))
            okTests++;
        Assert.assertEquals(3, okTests);
    }

    @Test
    public void queryParserLong() {
        try {
            QueryParams qp = QueryParams.parse(Json.createReader(new StringReader(PARAMS_COMPLETE)).readObject());
            assertEquals(SQL_COMMAND, qp.query);
            assertEquals("csql", qp.dialect);
            assertEquals(QueryParams.Priority.foreground, qp.priority);
            assertEquals(Integer.valueOf(10), qp.timeoutMs);
            assertEquals(Integer.valueOf(11), qp.maxWait);
            assertEquals(Integer.valueOf(12), qp.defaultPageSize);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void queryParserShort() {
        try {
            QueryParams qp = QueryParams.parse(Json.createReader(new StringReader(PARAMS_SHORT)).readObject());
            assertEquals(SQL_COMMAND, qp.query);
            assertNull(qp.dialect);
            assertNull(qp.priority);
            assertNull(qp.timeoutMs);
            assertNull(qp.maxWait);
            assertNull(qp.defaultPageSize);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
