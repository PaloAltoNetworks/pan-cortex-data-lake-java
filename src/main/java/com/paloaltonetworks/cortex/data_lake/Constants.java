/**
 * Constants
 * 
 * Copyright 2015-2020 Palo Alto Networks, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paloaltonetworks.cortex.data_lake;

import java.util.Map;

//TODO: BASE_FQDN is not final one

/**
 * SDK-wide constants
 */
public final class Constants {
    final static String BASE_FQDN = "dev-cortex-global-01-mtls.paloaltonetworks-app.com";
    final static String EP_SCHEMA = "/schema/v2/schemas/";
    final static String EP_QUERY = "/query/v2/";
    public final static Map<String, String> APIEPMAP = Map.of("europe", "api.nl.cdl.paloaltonetworks.com", "americas",
            "api.us.cdl.paloaltonetworks.com");
    final static String IDP_TOKEN_URL = "https://api.paloaltonetworks.com/api/oauth2/RequestToken";
    final static String IDP_REVOKE_URL = "https://api.paloaltonetworks.com/api/oauth2/RevokeToken";
    final static String IDP_AUTH_URL = "https://identity.paloaltonetworks.com/as/authorization.oauth2";
    final static String DEV_TOKEN_PROVIDER = "https://app.developers.paloaltonetworks.com/request_token";
    final static String SCOPE_LS_READ = "logging-service:read";
}