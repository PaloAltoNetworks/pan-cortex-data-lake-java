
/**
 * This snippet shows how to use the High-Level QueryServiceClient
 * Stream interface to execute a SQL query and navigate its results.
 * 
 * Provides optional values to modify the default page size
 */

import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.function.Function;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class C_QueryStreamCustom {
    private static final String accessToken = "eyJh...yx7Q";
    private static final String sqlCmd = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final Function<Boolean, Map.Entry<String, String>> cred = new Function<Boolean, Map.Entry<String, String>>() {

        @Override
        public Entry<String, String> apply(Boolean force) {
            if (force != null && force) {
                return new SimpleEntry<String, String>(Constants.USFQDN, accessToken);
            } else {
                return null;
            }
        }
    };

    public static void main(String[] args) throws Exception {
        QueryServiceClient qsc = new QueryServiceClient(cred);
        qsc.stream(sqlCmd, Integer.valueOf(50), null, null, null).forEach((item) -> System.out.println(item));
    }
}