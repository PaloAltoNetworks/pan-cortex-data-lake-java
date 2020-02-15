
/**
 * This snippet shows how to use the High-Level QueryServiceClient
 * Stream interface to execute a SQL query and navigate its results.
 * 
 * Notice that the Stream interface features a spliterator that will
 * automatically enable multithreading processing of large responses.
 */

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;

public class B_QueryStream {
    private static final String accessToken = "eyJh...yx7Q";
    private static final String sqlCmd = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";
    private static final Function<Boolean, Map.Entry<String, String>> cred = new Function<Boolean, Map.Entry<String, String>>() {

        @Override
        public Entry<String, String> apply(Boolean force) {
            if (force != null && force) {
                return new Map.Entry<String, String>() {

                    @Override
                    public String getKey() {
                        return Constants.USFQDN;
                    }

                    @Override
                    public String getValue() {
                        return B_QueryStream.accessToken;
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
        qsc.stream(sqlCmd).forEach((item) -> System.out.println(item));
    }
}