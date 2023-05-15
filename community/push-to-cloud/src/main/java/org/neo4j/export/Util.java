/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.neo4j.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

public final class Util {

    private Util() {}

    public static URL safeUrl(String urlString) {
        try {
            return new URL(urlString);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Malformed URL '" + urlString + "'", e);
        }
    }

    public static String base64Encode(String username, char[] password) {
        String plainToken = username + ':' + String.valueOf(password);
        return Base64.getEncoder().encodeToString(plainToken.getBytes());
    }

    /**
     * Use the Jackson JSON parser because Neo4j Server depends on this library already and therefore already exists in the environment. This means that this
     * command can parse JSON w/o any additional external dependency and doesn't even need to depend on java 8, where the Rhino script engine has built-in JSON
     * parsing support.
     */
    public static <T> T parseJsonUsingJacksonParser(String json, Class<T> type) throws IOException {
        return new ObjectMapper().readValue(json, type);
    }
}
