
/**
 * This snippet shows how to use the High-Level QueryServiceClient
 * Iterator interface to execute multiple SQL queries in parallel
 */

import java.util.Map;
import java.util.Map.Entry;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.Credentials;
import com.paloaltonetworks.cortex.data_lake.Http2Fetch;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class D_QueryIteratorParallel {
    private static final String accessToken = "eyJh...yx7Q";
    private static final String sqlCmd1 = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final String sqlCmd2 = "SELECT * FROM `<instance_id>.firewall.threat` LIMIT 100";
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
                        return D_QueryIteratorParallel.accessToken;
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
        /**
         * Instantiate an Http2Fetch object to have access to its `init()` method
         */
        Http2Fetch client = new Http2Fetch(cred);
        client.init();

        QueryServiceClient qsc = new QueryServiceClient(client, null);

        /**
         * Thread One
         */
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (var item : qsc.iterable(sqlCmd1))
                    System.out.println(item);
            }
        }).start();

        /**
         * Thread Two
         */
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (var item : qsc.iterable(sqlCmd2))
                    System.out.println(item);
            }
        }).start();
    }
}