
/**
 * This snippet shows how to use the High-Level QueryServiceClient
 * Iterator interface to execute multiple SQL queries in parallel
 */

import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Function;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.Http2Fetch;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class D_QueryIteratorParallel {
    private static final String accessToken = "eyJh...yx7Q";
    private static final String sqlCmd1 = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final String sqlCmd2 = "SELECT * FROM `<instance_id>.firewall.threat` LIMIT 100";
    private static final Function<Boolean, Map.Entry<String, String>> cred = new Function<Boolean, Map.Entry<String, String>>() {

        @Override
        public Entry<String, String> apply(Boolean force) {
            if (force != null && force) {
                return new SimpleImmutableEntry<String, String>(Constants.USFQDN, accessToken);
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