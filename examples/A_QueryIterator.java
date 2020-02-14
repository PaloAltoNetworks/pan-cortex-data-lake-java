
/**
 * This snippet shows how to use the High-Level QueryServiceClient iterator
 * interface to execute a SQL query and navigate its results.
 */

import java.util.Map;
import java.util.Map.Entry;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.Credentials;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class A_QueryIterator {
    private static final String accessToken = "eyJh...yx7Q";
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
                        return A_QueryIterator.accessToken;
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
        QueryServiceClient qsc = new QueryServiceClient(cred);
        for (var item : qsc.iterable(sqlCmd))
            System.out.println(item);
    }
}