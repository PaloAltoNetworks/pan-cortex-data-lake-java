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

/**
 * library-wide constants
 */
public final class Constants {
    public final static String USFQDN = "cortex-prd1-api.us.cdl.paloaltonetworks.com";
    // final static String USFQDN = "api.us.cdl.paloaltonetworks.com";
    public final static String EUFQDN = "api.nl.cdl.paloaltonetworks.com";
    final static String EP_QUERY = "/query/v2/";
    public final static Map<String, String> APIEPMAP = Map.of("europe", EUFQDN, "americas", USFQDN);
}