
/**
 * This snippet shows how to use the High-Level QueryServiceClient
 * Iterator interface to execute multiple SQL queries in parallel to multiple
 * data lake instances.
 * 
 * In this case we're using one QueryServiceClient object and passing
 * credentials in the iterators method
 */

import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Function;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.CredentialTuple;
import com.paloaltonetworks.cortex.data_lake.Http2Fetch;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class E_QueryIteratorMultipleDataLake {
    private static final String accessToken1 = "eyJh...yx7Q";
    private static final String accessToken2 = "eyJh...4tgR";
    private static final String sqlCmd = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final Function<String, Function<Boolean, Map.Entry<String, String>>> cred = (
            token) -> new Function<Boolean, Map.Entry<String, String>>() {

                @Override
                public Entry<String, String> apply(Boolean force) {
                    if (force != null && force) {
                        return new SimpleImmutableEntry<String, String>(Constants.USFQDN, token);
                    } else {
                        return null;
                    }
                }
            };

    public static void main(String[] args) throws Exception {
        /**
         * Each credentials must have a unique data lake identifier
         */
        CredentialTuple credTuple1 = new CredentialTuple("datalake1", cred.apply(accessToken1));
        CredentialTuple credTuple2 = new CredentialTuple("datalake2", cred.apply(accessToken2));

        /**
         * Notice we're not providing a default credentials object
         */
        Http2Fetch client = new Http2Fetch();

        /**
         * Each credential tuple must be initialitated on its own and in sequence
         * (parallel init could trigger race conditions)
         */
        client.init(credTuple1);
        client.init(credTuple2);

        QueryServiceClient qsc = new QueryServiceClient(client, null);

        /**
         * Thread One
         */
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (var item : qsc.iterable(sqlCmd, credTuple1))
                    System.out.println(item);
            }
        }).start();

        /**
         * Thread Two
         */
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (var item : qsc.iterable(sqlCmd, credTuple2))
                    System.out.println(item);
            }
        }).start();

    }
}