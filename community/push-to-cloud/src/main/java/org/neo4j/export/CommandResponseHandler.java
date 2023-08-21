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
package org.neo4j.export;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.compress.utils.IOUtils.toByteArray;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;

public class CommandResponseHandler {

    private final ExecutionContext ctx;

    public CommandResponseHandler(ExecutionContext ctx) {
        this.ctx = ctx;
    }

    public CommandFailedException unexpectedResponse(
            boolean verbose, HttpURLConnection connection, String requestDescription) throws IOException {
        debugErrorResponse(verbose, connection);
        return new CommandFailedException(format(
                "Unexpected response code %d from request: %s", connection.getResponseCode(), requestDescription));
    }

    public void debugErrorResponse(boolean verbose, HttpURLConnection connection) throws IOException {
        if (verbose) {
            String responseString;
            try (InputStream responseData = connection.getErrorStream()) {
                responseString = new String(toByteArray(responseData), UTF_8);
            } catch (IOException e) {
                throw new IOException(format("Failed to read response from server: %s", e.getMessage()));
            }
            debugResponse(responseString, connection, true);
        }
    }

    public void debugResponse(String responseBody, HttpURLConnection connection, boolean error) throws IOException {
        debug(true, error ? "=== Unexpected response ===" : "=== Response ===");
        debug(true, "Response message: " + connection.getResponseMessage());
        debug(true, "Response headers:");
        connection.getHeaderFields().forEach((key, value1) -> {
            for (String value : value1) {
                debug(true, "  " + key + ": " + value);
            }
        });
        debug(true, "Response data: " + responseBody);
    }

    public void debug(boolean verbose, String string) {
        if (verbose) {
            ctx.out().println(string);
        }
    }
}
