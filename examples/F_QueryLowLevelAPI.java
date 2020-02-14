
/**
 * This snippet shows how to use the QueryService low level API to execute a
 * simple query
 */

import java.util.Map;
import java.util.Map.Entry;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.Credentials;
import com.paloaltonetworks.cortex.data_lake.QueryJob;
import com.paloaltonetworks.cortex.data_lake.QueryJobDetail;
import com.paloaltonetworks.cortex.data_lake.QueryJobResult;
import com.paloaltonetworks.cortex.data_lake.QueryParams;
import com.paloaltonetworks.cortex.data_lake.QueryService;
import com.paloaltonetworks.cortex.data_lake.QueryJobDetail.JobState;

public class F_QueryLowLevelAPI {
    private static final String accessToken = "eyJh...4tgR";
    private static final String sqlCmd = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final Credentials cred = new Credentials() {

        @Override
        public Entry<String, String> GetToken(Boolean force) {
            if (force != null && force) {
                return new Map.Entry<String, String>() {

                    @Override
                    public String getKey() {
                        return Constants.USFQDN;
                    }

                    @Override
                    public String getValue() {
                        return accessToken;
                    }

                    @Override
                    public String setValue(String value) {
                        return null;
                    }
                };
            } else {
                return null;
            }
        }
    };

    public static void main(String[] args) throws Exception {
        QueryService qs = new QueryService(cred);

        /**
         * First argument (jobId) is optional and third argument (Credentials) being
         * null means to use default credentials provided in the constructor
         */
        QueryJob queryJobResponse = qs.createJob(null, new QueryParams(sqlCmd), null);
        System.out.println("Job Id " + queryJobResponse.jobId);

        /**
         * Wait 10 seconds for the job to complete
         */
        Thread.sleep(10000);

        /**
         * Get job status and verify it has been completed (state == 'DONE)
         */
        QueryJobDetail queryJobDetail = qs.getJobStatus(queryJobResponse.jobId, null);
        System.out.println("Job state " + queryJobDetail.state);
        if (queryJobDetail.state == JobState.DONE) {

            /**
             * Get job results
             */
            QueryJobResult queryResultResp = qs.getJobResults(queryJobResponse.jobId, null, null, null);
            System.out.println("Records in job " + queryResultResp.rowsInJob);
        }

        /**
         * Optionally you can delete the job
         */
        qs.deleteJob(queryJobResponse.jobId, null);
    }
}