/**
 * Tools
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

import java.util.ArrayList;
import java.util.Map;
import static java.net.URLEncoder.encode;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

class Tools {
    static String querify(Map<String, String> params) {
        ArrayList<String> paramList = new ArrayList<String>(params.size());
        params.forEach((k, v) -> {
            try {
                paramList.add(encode(k, "UTF-8") + "=" + encode(v, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        });
        return String.join("&", paramList);
    }

    static byte[] shaone(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            return md.digest(text.getBytes());
        } catch (Exception e) {
            return null;
        }
    }
}