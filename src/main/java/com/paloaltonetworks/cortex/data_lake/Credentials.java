/**
 * Credentials
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
 * Generic interface that any JWT credential object must conform to.
 * 
 * The Key in the map entry returned will be considered the Cortex Data Lake API
 * entry point FQDN (region) the token in valid for. The Value in the map entry
 * will be considered the access_token itself.
 * 
 * A null response can be used by the provider to signal the consumer that no
 * new access_token has been produced (refresh operation) so last cached value
 * is still valid.
 * 
 * The provider must return a non-null response if the force flag is set to True
 */
public interface Credentials {
    /**
     * Returns a entry point (key) and access_token (value) pair
     * 
     * @param force to request a non-null response
     * @return a key/value pair if last provided value is no longer valid
     *         (refreshed) or if force is set to true
     */
    public Map.Entry<String, String> GetToken(Boolean force);
}