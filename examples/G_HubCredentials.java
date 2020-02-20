
/**
 * This snippet shows how to leverage a Credentials object from the
 * <code>com.paloaltonetworks.cortex.hub</code> package.
 */

import java.util.Map;
import java.util.function.Function;

import com.paloaltonetworks.cortex.data_lake.Constants;
import com.paloaltonetworks.cortex.data_lake.QueryServiceClient;
import com.paloaltonetworks.cortex.hub.HubCredentialProviderSimple;

public class G_HubCredentials {
    private static final String clientId = "<oauth2_client_id>";
    private static final String clientSecret = "<oauth2_client_secret>";
    private static final String refreshToken = "<oauth2_refresh_token>";
    private static final String sqlCmd = "SELECT * FROM `<instance_id>.firewall.traffic` LIMIT 100";

    public static void main(String[] args) throws Exception {
        Function<Boolean, Map.Entry<String, String>> cred = HubCredentialProviderSimple.factory(clientId, clientSecret,
                refreshToken, Constants.USFQDN);
        QueryServiceClient qsc = new QueryServiceClient(cred);
        for (var item : qsc.iterable(sqlCmd))
            System.out.println(item);
    }
}