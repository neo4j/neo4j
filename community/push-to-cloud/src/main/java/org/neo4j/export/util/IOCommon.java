/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.export.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.UploadCommand;

public final class IOCommon {

    private IOCommon() {}

    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

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

    public static String SerializeWithJackson(Object pojo) throws IOException {
        return new ObjectMapper().writeValueAsString(pojo);
    }

    public static void safeSkip(InputStream sourceStream, long position) throws IOException {
        long toSkip = position;
        while (toSkip > 0) {
            toSkip -= sourceStream.skip(position);
        }
    }

    public static long getFileSize(UploadCommand.Source src, ExecutionContext ctx) {
        long fileSize;
        try {
            fileSize = src.fs().getFileSize(src.path());
        } catch (IOException e) {
            ctx.err().println(String.format("Failed to determine size of file at location: %s", src.path()));
            throw new CommandFailedException(e.getMessage());
        }
        return fileSize;
    }
}
