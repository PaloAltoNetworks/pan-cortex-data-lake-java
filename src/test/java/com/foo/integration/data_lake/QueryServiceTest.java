package com.foo.integration.data_lake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.json.JsonValue;

import com.paloaltonetworks.cortex.data_lake.Http2FetchException;
import com.paloaltonetworks.cortex.data_lake.QueryIterable;
import com.paloaltonetworks.cortex.data_lake.QueryJob;
import com.paloaltonetworks.cortex.data_lake.QueryJobDetail;
import com.paloaltonetworks.cortex.data_lake.QueryJobResult;
import com.paloaltonetworks.cortex.data_lake.QueryParams;
import com.paloaltonetworks.cortex.data_lake.QueryService;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;
import com.paloaltonetworks.cortex.data_lake.QueryServiceException;
import com.paloaltonetworks.cortex.data_lake.QueryServiceParseException;
import com.paloaltonetworks.cortex.data_lake.QueryServiceRuntimeException;
import com.paloaltonetworks.cortex.data_lake.QueryJobDetail.JobState;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class QueryServiceTest {
    private final static Integer limit = 1015;
    private final static int cursorTestPageSize = 100;
    private final static String SQL_COMMAND = "SELECT * from `2020001.firewall.traffic` LIMIT " + limit;
    private final static String SQL_COMMAND_SMALL = "SELECT * from `2020001.firewall.traffic` LIMIT 10";
    private final static String SQL_COMMAND_LARGE = "SELECT * from `2020001.firewall.traffic` LIMIT 40017";
    private final static String baseFqdn = System.getProperty("cortex.fqdn");
    private final static char[] password = "".toCharArray();
    private final static Integer pageSize = 10;
    private static QueryService qs;
    private static String tenantId;
    private Integer count;

    @BeforeClass
    public static void init() {
        // Logger.setLevel(Level.INFO);
        // Handler consoleHandler = new ConsoleHandler();
        // consoleHandler.setLevel(Level.FINEST);
        // logger.addHandler(consoleHandler);
        try {
            String keystore = System.getProperty("cortex.clientcert");
            tenantId = System.getProperty("cortex.tenantid");
            qs = new QueryService(keystore, password, baseFqdn);
        } catch (IllegalArgumentException e) {
            Assume.assumeNoException("unable to get properties 'cortex.clientcert' and/or 'cortex.tenantid'", e);
        } catch (Exception e) {
            Assume.assumeNoException(String.format("unable to run the tests due to: %s", e.getMessage()), e);
        }
    }

    @Test
    public void mandatoryArguments() {
        long okTests = 0;
        try {
            qs.getJobsList(null, null);
        } catch (IllegalArgumentException e) {
            okTests++;
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }

        try {
            qs.getJobsList("", null);
        } catch (IllegalArgumentException e) {
            okTests++;
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }

        try {
            qs.createJob(null, null, null);
        } catch (IllegalArgumentException e) {
            okTests++;
        } catch (IOException | InterruptedException | QueryServiceException | QueryServiceParseException
                | Http2FetchException | URISyntaxException e) {
            assumeNoException(e);
        }
        assertEquals(3, okTests);
    }

    @Test
    public void getJobsListTest() {
        List<QueryJobDetail> jobs;
        try {
            jobs = qs.getJobsList(tenantId, null);
            jobs.forEach(x -> assertNotNull(x.jobId));
        } catch (QueryServiceException e) {
            e.queryApiError.forEach(x -> System.out.println(x.message));
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobsListAsyncTest() {
        List<QueryJobDetail> jobs;
        try {
            jobs = qs.getJobsListAsync(tenantId, null).get();
            jobs.forEach(x -> assertNotNull(x.jobId));
        } catch (ExecutionException e) {
            if (e.getCause().getClass() == QueryServiceRuntimeException.class) {
                QueryServiceRuntimeException e2 = (QueryServiceRuntimeException) e.getCause();
                e2.cortexApiError.forEach(x -> System.out.println(x.message));
            }
            fail(e.getMessage());
        } catch (InterruptedException | IllegalArgumentException | Http2FetchException | URISyntaxException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void getJobsListFilteredByTimeTest() {
        List<QueryJobDetail> jobs;
        try {
            jobs = qs.getJobsList(tenantId, 1564700573686L, null, null, null);
            jobs.forEach(x -> assertNotNull(x.jobId));
        } catch (QueryServiceException e) {
            e.queryApiError.forEach(x -> System.out.println(x.message));
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobsListLimitResultsTest() {
        List<QueryJobDetail> jobs;
        try {
            jobs = qs.getJobsList(tenantId, null, 10, null, null);
            jobs.forEach(x -> assertNotNull(x.jobId));
            assertTrue(jobs.size() == 10);
        } catch (QueryServiceException e) {
            e.queryApiError.forEach(x -> System.out.println(x.message));
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobsListFilteredByStateTest() {
        List<QueryJobDetail> jobs;
        try {
            jobs = qs.getJobsList(tenantId, null, null, QueryJobDetail.JobState.FAILED, null);
            jobs.forEach(x -> assertNotNull(x.jobId));
        } catch (QueryServiceException e) {
            e.queryApiError.forEach(x -> System.out.println(x.message));
            fail(e.getMessage());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobsListExceptionTest() {
        try {
            qs.getJobsList(tenantId + "_x", null);
            fail("expected exception not thrown");
        } catch (QueryServiceException e) {
            assertEquals(Integer.valueOf(403), Integer.valueOf(e.httpStatusCode));
            if (e.queryApiError == null) {
                fail("Expected cortex api error missing");
                return;
            }
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobsListAsyncExceptionTest() {
        try {
            qs.getJobsListAsync(tenantId + "_x", null).get();
            fail("expected exception not thrown");
        } catch (ExecutionException e) {
            QueryServiceRuntimeException e2 = (QueryServiceRuntimeException) e.getCause();
            if (e2.getClass() == QueryServiceRuntimeException.class) {
                assertEquals(Integer.valueOf(403), Integer.valueOf(e2.httpStatusCode));
                if (e2.cortexApiError == null) {
                    fail("Expected cortex api error missing");
                    return;
                }
            } else
                fail("non-'QueryServiceRuntimeException' exception triggered");
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobStatusTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobDetail jobDetails = qs.getJobStatus(qj.jobId, null);
            assertEquals(uuid, jobDetails.jobId);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    // @Test
    public void getJobStatusExceptionTest() {
        try {
            qs.getJobStatus("ae074b61-f2ba-48e6-b62b-11dbd670c7f5-z", null);
        } catch (QueryServiceException e) {
            assertEquals(Integer.valueOf(404), Integer.valueOf(e.httpStatusCode));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobStatusAsyncTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobDetail jobDetails = qs.getJobStatusAsync(qj.jobId, null).get();
            assertEquals(uuid, jobDetails.jobId);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobStatusAsyncExceptionTest() {
        try {
            qs.getJobStatusAsync("ae074b61-f2ba-48e6-b62b-11dbd670c7f5_x", null).get();
            fail("expected exception not thrown");
        } catch (ExecutionException e) {
            QueryServiceRuntimeException e2 = (QueryServiceRuntimeException) e.getCause();
            if (e2.getClass() == QueryServiceRuntimeException.class) {
                assertEquals(Integer.valueOf(404), Integer.valueOf(e2.httpStatusCode));
                return;
            }
            fail("thrown a unexpected exception");
        } catch (Exception e) {
            fail("thrown a unexpected exception");
        }
    }

    @Test
    public void deleteJobTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            qs.deleteJob(qj.jobId, null);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void deleteJobExceptionTest() {
        try {
            qs.deleteJob("7eb17532-cbcc-409f-b8d9-a1d7519ee104-x", null);
            fail("expected exception not thrown");
        } catch (QueryServiceException e) {
            assertEquals(Integer.valueOf(404), Integer.valueOf(e.httpStatusCode));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void deleteJobStatusAsyncTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            qs.deleteJobAsync(qj.jobId, null).get();
            assertTrue(true);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void deleteJobStatusAsyncExceptionTest() {
        try {
            qs.deleteJobAsync("ae074b61-f2ba-48e6-b62b-11dbd670c7f5_x", null).get();
            fail("expected exception not thrown");
        } catch (ExecutionException e) {
            QueryServiceRuntimeException e2 = (QueryServiceRuntimeException) e.getCause();
            if (e2.getClass() == QueryServiceRuntimeException.class) {
                assertEquals(Integer.valueOf(404), Integer.valueOf(e2.httpStatusCode));
                return;
            }
            fail("thrown a unexpected exception");
        } catch (Exception e) {
            fail("thrown a unexpected exception");
        }
    }

    @Test
    public void createJobTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            assertEquals(uuid, qj.jobId);
            assertEquals("/query/v2/jobs/" + uuid, qj.uri);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void createJobAsyncTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJobAsync(uuid, new QueryParams(SQL_COMMAND), null).get();
            assertEquals(uuid, qj.jobId);
            assertEquals("/query/v2/jobs/" + uuid, qj.uri);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void createJobExceptionTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            assertEquals(uuid, qj.jobId);
            assertEquals("/query/v2/jobs/" + uuid, qj.uri);
            qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            fail("expected exception not thrown");
        } catch (QueryServiceException e) {
            assertEquals(Integer.valueOf(409), Integer.valueOf(e.httpStatusCode));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void createJobAsyncExceptionTest() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJobAsync(uuid, new QueryParams(SQL_COMMAND), null).get();
            assertEquals(uuid, qj.jobId);
            assertEquals("/query/v2/jobs/" + uuid, qj.uri);
            qs.createJobAsync(uuid, new QueryParams(SQL_COMMAND), null).get();
            fail("expected exception not thrown");
        } catch (ExecutionException e) {
            QueryServiceRuntimeException e2 = (QueryServiceRuntimeException) e.getCause();
            assertEquals(Integer.valueOf(409), Integer.valueOf(e2.httpStatusCode));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobResultsLong() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobResult qjr = qs.getJobResults(qj.jobId, null, QueryJobResult.ResultFormat.valuesDictionary,
                    pageSize, null, null, null, null);
            for (int attempts = 3; attempts > 0; attempts--) {
                if (qjr.state == JobState.DONE)
                    break;
                Thread.sleep(2000);
                qjr = qs.getJobResults(qj.jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize, null,
                        null, null, null);
            }
            if (qjr.state != JobState.DONE) {
                fail("Job did not complete ontime for the test assert.");
                return;
            }
            if (qjr.page == null || qjr.page.result.data == null) {
                fail("Job result does not contain expected data.");
                return;
            }
            if (qjr.rowsInJob == null) {
                fail("Job result does not contain the expected rowsInJob property.");
                return;
            }
            if (qjr.rowsInPage == null) {
                fail("Job result does not contain the expected rowsInPage property.");
                return;
            }
            if (qjr.resultFormat == null) {
                fail("Job result does not contain the expected resultFormat property.");
                return;
            }
            assertEquals(qj.jobId, qjr.jobId);
            assertEquals(limit, qjr.rowsInJob);
            assertEquals(pageSize, qjr.rowsInPage);
            assertEquals(pageSize, Integer.valueOf(qjr.page.result.data.size()));
            assertEquals(QueryJobResult.ResultFormat.valuesDictionary, qjr.resultFormat);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobResultsAsyncLong() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobResult qjr = qs.getJobResultsAsync(qj.jobId, null, QueryJobResult.ResultFormat.valuesDictionary,
                    pageSize, null, null, null, null).get();
            for (int attempts = 3; attempts > 0; attempts--) {
                if (qjr.state == JobState.DONE)
                    break;
                Thread.sleep(2000);
                qjr = qs.getJobResultsAsync(qj.jobId, null, QueryJobResult.ResultFormat.valuesDictionary, pageSize,
                        null, null, null, null).get();
            }
            if (qjr.state != JobState.DONE) {
                fail("Job did not complete ontime for the test assert.");
                return;
            }
            if (qjr.page == null || qjr.page.result.data == null) {
                fail("Job result does not contain expected data.");
                return;
            }
            if (qjr.rowsInJob == null) {
                fail("Job result does not contain the expected rowsInJob property.");
                return;
            }
            if (qjr.rowsInPage == null) {
                fail("Job result does not contain the expected rowsInPage property.");
                return;
            }
            if (qjr.resultFormat == null) {
                fail("Job result does not contain the expected resultFormat property.");
                return;
            }
            assertEquals(qj.jobId, qjr.jobId);
            assertEquals(limit, qjr.rowsInJob);
            assertEquals(pageSize, qjr.rowsInPage);
            assertEquals(pageSize, Integer.valueOf(qjr.page.result.data.size()));
            assertEquals(QueryJobResult.ResultFormat.valuesDictionary, qjr.resultFormat);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobResultsShort() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobResult qjr = qs.getJobResults(qj.jobId, pageSize, null, null);
            for (int attempts = 3; attempts > 0; attempts--) {
                if (qjr.state == JobState.DONE)
                    break;
                Thread.sleep(2000);
                qjr = qs.getJobResults(qj.jobId, pageSize, null, null);
            }
            if (qjr.state != JobState.DONE) {
                fail("Job did not complete ontime for the test assert.");
                return;
            }
            if (qjr.page == null || qjr.page.result.data == null) {
                fail("Job result does not contain expected data.");
                return;
            }
            if (qjr.rowsInJob == null) {
                fail("Job result does not contain the expected rowsInJob property.");
                return;
            }
            if (qjr.rowsInPage == null) {
                fail("Job result does not contain the expected rowsInPage property.");
                return;
            }
            if (qjr.resultFormat == null) {
                fail("Job result does not contain the expected resultFormat property.");
                return;
            }
            assertEquals(qj.jobId, qjr.jobId);
            assertEquals(limit, qjr.rowsInJob);
            assertEquals(pageSize, qjr.rowsInPage);
            assertEquals(pageSize, Integer.valueOf(qjr.page.result.data.size()));
            assertEquals(QueryJobResult.ResultFormat.valuesDictionary, qjr.resultFormat);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobResultsPagination() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobResult qjr = qs.getJobResults(qj.jobId, pageSize, null, null);
            for (int attempts = 3; attempts > 0; attempts--) {
                if (qjr.state == JobState.DONE)
                    break;
                Thread.sleep(2000);
                qjr = qs.getJobResults(qj.jobId, pageSize, null, null);
            }
            if (qjr.state != JobState.DONE) {
                fail("Job did not complete ontime for the test assert.");
                return;
            }
            if (qjr.page == null || qjr.page.result.data == null) {
                fail("Job result does not contain expected data.");
                return;
            }
            if (qjr.rowsInJob == null) {
                fail("Job result does not contain the expected rowsInJob property.");
                return;
            }
            if (qjr.rowsInPage == null) {
                fail("Job result does not contain the expected rowsInPage property.");
                return;
            }
            if (qjr.resultFormat == null) {
                fail("Job result does not contain the expected resultFormat property.");
                return;
            }
            assertEquals(qj.jobId, qjr.jobId);
            assertEquals(limit, qjr.rowsInJob);
            Integer rows = qjr.page.result.data.size();
            while (qjr.page.pageCursor != null) {
                qjr = qs.getJobResults(qj.jobId, cursorTestPageSize, qjr.page.pageCursor, null);
                rows += qjr.page.result.data.size();
            }
            assertEquals(limit, rows);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void getJobResultsAsyncPagination() {
        try {
            String uuid = UUID.randomUUID().toString();
            QueryJob qj = qs.createJob(uuid, new QueryParams(SQL_COMMAND), null);
            QueryJobResult qjr = qs.getJobResultsAsync(qj.jobId, pageSize, null, null).get();
            for (int attempts = 3; attempts > 0; attempts--) {
                if (qjr.state == JobState.DONE)
                    break;
                Thread.sleep(2000);
                qjr = qs.getJobResultsAsync(qj.jobId, pageSize, null, null).get();
            }
            if (qjr.state != JobState.DONE) {
                fail("Job did not complete ontime for the test assert.");
                return;
            }
            if (qjr.page == null || qjr.page.result.data == null) {
                fail("Job result does not contain expected data.");
                return;
            }
            if (qjr.rowsInJob == null) {
                fail("Job result does not contain the expected rowsInJob property.");
                return;
            }
            if (qjr.rowsInPage == null) {
                fail("Job result does not contain the expected rowsInPage property.");
                return;
            }
            if (qjr.resultFormat == null) {
                fail("Job result does not contain the expected resultFormat property.");
                return;
            }
            assertEquals(qj.jobId, qjr.jobId);
            assertEquals(limit, qjr.rowsInJob);
            Integer rows = qjr.page.result.data.size();
            while (qjr.page.pageCursor != null) {
                qjr = qs.getJobResultsAsync(qj.jobId, cursorTestPageSize, qjr.page.pageCursor, null).get();
                rows += qjr.page.result.data.size();
            }
            assertEquals(limit, rows);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void iteratorConsumerTest() {
        String keystore = System.getProperty("cortex.clientcert");
        try {
            QueryServiceClient qsc = new QueryServiceClient(keystore, password, baseFqdn);
            QueryIterable resultIterable = qsc.iterable(SQL_COMMAND, cursorTestPageSize, 200, 3, null);
            resultIterable.forEach(item -> item.asJsonObject().getString("log_source_id"));
            assertEquals(limit, resultIterable.size());
        } catch (IllegalArgumentException e) {
            Assume.assumeNoException("unable to get properties 'cortex.clientcert' and/or 'cortex.tenantid'", e);
        } catch (Exception e) {
            Assume.assumeNoException(String.format("unable to run the tests due to: %s", e.getMessage()), e);
        }
    }

    @Test
    public void preloadedIteratorConsumerTest() {
        String keystore = System.getProperty("cortex.clientcert");
        try {
            QueryServiceClient qsc = new QueryServiceClient(keystore, password, baseFqdn);
            QueryIterable resultIterable = qsc.iterable(SQL_COMMAND, cursorTestPageSize, 200, 3, null);
            assertEquals(limit, resultIterable.size());
            Integer iterations = 0;
            for (var item : resultIterable) {
                item.asJsonObject().getString("log_source_id");
                iterations++;
            }
            assertEquals(limit, resultIterable.size());
            assertEquals(limit, iterations);
        } catch (IllegalArgumentException e) {
            Assume.assumeNoException("unable to get properties 'cortex.clientcert' and/or 'cortex.tenantid'", e);
        } catch (Exception e) {
            Assume.assumeNoException(String.format("unable to run the tests due to: %s", e.getMessage()), e);
        }
    }

    @Test
    public void multiThread() {
        String keystore = System.getProperty("cortex.clientcert");
        final int threads = 14;
        Executor executor = Executors.newFixedThreadPool(threads);
        CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executor);
        QueryServiceClient qsc;
        try {
            qsc = new QueryServiceClient(keystore, password, baseFqdn);
            for (int idx = 0; idx < threads; idx++) {
                completionService.submit(new Callable<Integer>() {

                    @Override
                    public Integer call() throws Exception {
                        Integer iterations = 0;
                        QueryIterable resultIterable = qsc.iterable(SQL_COMMAND, cursorTestPageSize, 200, 3, null);
                        for (@SuppressWarnings("unused")
                        JsonValue item : resultIterable) {
                            iterations++;
                        }
                        if (iterations.equals(resultIterable.size())) {
                            return iterations;
                        }
                        return null;
                    }
                });
            }

            int received = 0;
            boolean errors = false;
            while (received < threads && !errors) {
                try {
                    Future<Integer> resultFuture = completionService.take();
                    try {
                        Integer result = resultFuture.get();
                        received++;
                        assertEquals(limit, result);
                    } catch (Exception e) {
                        errors = true;
                    }
                } catch (Exception e) {
                    errors = true;
                }
            }
            assertTrue(!errors);
        } catch (Exception e1) {
            fail(e1.toString());
        }
    }

    @Test
    public void preloadMultiThread() {
        String keystore = System.getProperty("cortex.clientcert");
        final int threads = 14;
        Executor executor = Executors.newFixedThreadPool(threads);
        CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executor);
        QueryServiceClient qsc;
        try {
            qsc = new QueryServiceClient(keystore, password, baseFqdn);
            for (int idx = 0; idx < threads; idx++) {
                completionService.submit(new Callable<Integer>() {

                    @Override
                    public Integer call() throws Exception {
                        Integer iterations = 0;
                        QueryIterable resultIterable = qsc.iterable(SQL_COMMAND, cursorTestPageSize, 200, 3, null);
                        assertEquals(limit, resultIterable.size());
                        for (@SuppressWarnings("unused")
                        JsonValue item : resultIterable) {
                            iterations++;
                        }
                        if (iterations.equals(resultIterable.size())) {
                            return iterations;
                        }
                        return null;
                    }
                });
            }

            int received = 0;
            boolean errors = false;
            while (received < threads && !errors) {
                try {
                    Future<Integer> resultFuture = completionService.take();
                    try {
                        Integer result = resultFuture.get();
                        received++;
                        assertEquals(limit, result);
                    } catch (Exception e) {
                        errors = true;
                    }
                } catch (Exception e) {
                    errors = true;
                }
            }
            assertTrue(!errors);
        } catch (Exception e1) {
            fail(e1.toString());
        }
    }

    @Test
    public void parallelStream() {
        String keystore = System.getProperty("cortex.clientcert");
        try {
            var qsc = new QueryServiceClient(keystore, password, baseFqdn);
            count = 0;
            qsc.stream(SQL_COMMAND, cursorTestPageSize, 200, 3, null).forEach(item -> {
                item.asJsonObject().getString("log_source_id");
                synchronized (this) {
                    count++;
                }
            });
            assertEquals(Integer.valueOf(limit), Integer.valueOf(count));
        } catch (Exception e) {
            fail("something went really wrong!!!");
        }
    }

    @Test
    public void parallelSmallStream() {
        String keystore = System.getProperty("cortex.clientcert");
        try {
            var qsc = new QueryServiceClient(keystore, password, baseFqdn);
            count = 0;
            qsc.stream(SQL_COMMAND_SMALL, cursorTestPageSize, 200, 3, null).forEach(item -> {
                item.asJsonObject().getString("log_source_id");
                synchronized (this) {
                    count++;
                }
            });
            assertEquals(Integer.valueOf(10), Integer.valueOf(count));
        } catch (Exception e) {
            fail("something went really wrong!!!");
        }
    }

    @Test
    public void parallelLargeStream() {
        String keystore = System.getProperty("cortex.clientcert");
        try {
            var qsc = new QueryServiceClient(keystore, password, baseFqdn);
            count = 0;
            qsc.stream(SQL_COMMAND_LARGE, null).forEach(item -> {
                item.asJsonObject().getString("log_source_id");
                synchronized (this) {
                    count++;
                }
            });
            assertEquals(Integer.valueOf(40017), Integer.valueOf(count));
        } catch (Exception e) {
            fail("something went really wrong!!!");
        }
    }
}