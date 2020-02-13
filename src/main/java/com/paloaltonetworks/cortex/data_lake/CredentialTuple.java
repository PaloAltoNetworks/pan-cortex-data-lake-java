/**
 * CredentialTuple
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
 * Credentials context metadata
 */
public class CredentialTuple {
    String dlid;
    String entryPoint;
    Credentials cred;

    /**
     * Creates a credential metadata context
     * 
     * @param dlid unique Data Lake identifier
     * @param cred Credentials object
     */
    public CredentialTuple(String dlid, Credentials cred) {
        this.dlid = dlid;
        this.cred = cred;
        Map.Entry<String, String> kv = cred.GetToken(true);
        entryPoint = kv.getKey();
    }
}