/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.http.cypher.format.output.eventsource.LineDelimitedEventSourceJoltMessageBodyWriter;
import org.neo4j.server.http.cypher.format.output.eventsource.SequentialEventSourceJoltMessageBodyWriter;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

class JoltV1ResultFormatIT extends AbstractRestFunctionalTestBase {
    private final HTTP.Builder http = HTTP.withBaseUri(container().getBaseUri())
            .withHeaders(HttpHeaders.ACCEPT, LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE);

    private String commitResource;

    private static Stream<String> lineDelimitedMimeTypes() {
        return Stream.of(
                LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE,
                LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V1);
    }

    private static Stream<String> sequentialMimeTypes() {
        return Stream.of(
                SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE,
                SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V1);
    }

    @BeforeEach
    void setUp() {
        // begin
        Response begin = http.POST(txUri());

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin);
        try {
            commitResource = begin.get("info").get("commit").asText();
        } catch (JsonParseException e) {
            fail("Exception caught when setting up test: " + e.getMessage());
        }
        assertThat(commitResource).isEqualTo(begin.location() + "/commit");
    }

    @ParameterizedTest
    @CsvSource({"true", "TRUE"})
    void shouldReturnJoltInStrictFormat(String booleanString) {
        // See https://github.com/eclipse/jetty.project/issues/5446
        var cacheBuster = ";cacheBuster=" + ThreadLocalRandom.current().nextLong();
        // execute and commit
        var response = http.withHeaders(
                        HttpHeaders.ACCEPT,
                        LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE + ";strict="
                                + booleanString + cacheBuster)
                .POST(commitResource, queryAsJsonRow("RETURN 1, 5.5, true"));

        assertThat(response.status()).isEqualTo(200);
        splitAndVerify(
                response.rawContent(),
                "\n",
                "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}",
                "{\"data\":[{\"Z\":\"1\"},{\"R\":\"5.5\"},{\"?\":\"true\"}]}",
                "{\"summary\":{}}",
                "{\"info\":{" + lineDeprecationNotification() + "," + "\"commit\":\"" + commitResource
                        + "\",\"lastBookmarks\":[");
    }

    @ParameterizedTest
    @CsvSource({"true", "TRUE"})
    void shouldReturnJoltInStrictRecordSeparatedFormat(String booleanString) {
        // See https://github.com/eclipse/jetty.project/issues/5446
        var cacheBuster = ";cacheBuster=" + ThreadLocalRandom.current().nextLong();
        // execute and commit
        var response = http.withHeaders(
                        HttpHeaders.ACCEPT,
                        SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE + ";strict="
                                + booleanString + cacheBuster)
                .POST(commitResource, queryAsJsonRow("RETURN 1, 5.5, true"));

        assertThat(response.status()).isEqualTo(200);
        splitAndVerify(
                response.rawContent(),
                "\u001E",
                "",
                "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}\n",
                "{\"data\":[{\"Z\":\"1\"},{\"R\":\"5.5\"},{\"?\":\"true\"}]}\n",
                "{\"summary\":{}}\n",
                "{\"info\":{" + seqDeprecationNotification() + ",\"commit\":\"" + commitResource
                        + "\",\"lastBookmarks\":[");
    }

    @ParameterizedTest
    @MethodSource("lineDelimitedMimeTypes")
    void shouldReturnJoltInSparseFormat(String mimeType) {
        // execute and commit
        var response = http.withHeaders(HttpHeaders.ACCEPT, mimeType)
                .POST(commitResource, queryAsJsonRow("RETURN 1, 5.5, true"));

        assertThat(response.status()).isEqualTo(200);
        splitAndVerify(
                response.rawContent(),
                "\n",
                "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}",
                "{\"data\":[1,{\"R\":\"5.5\"},true]}",
                "{\"summary\":{}}",
                "{\"info\":{" + lineDeprecationNotification() + ",\"commit\":\"" + commitResource
                        + "\",\"lastBookmarks\":[");
    }

    @ParameterizedTest
    @MethodSource("sequentialMimeTypes")
    void shouldReturnJoltInRecordSeparatedFormat(String mimeTypes) {
        // execute and commit
        var response = http.withHeaders(HttpHeaders.ACCEPT, mimeTypes)
                .POST(commitResource, queryAsJsonRow("RETURN 1, 5.5, true"));

        assertThat(response.status()).isEqualTo(200);

        splitAndVerify(
                response.rawContent(),
                "\u001E",
                "",
                "{\"header\":{\"fields\":[\"1\",\"5.5\",\"true\"]}}\n",
                "{\"data\":[1,{\"R\":\"5.5\"},true]}\n",
                "{\"summary\":{}}\n",
                "{\"info\":{" + seqDeprecationNotification() + "," + "\"commit\":\"" + commitResource
                        + "\",\"lastBookmarks\":");
    }

    private static HTTP.RawPayload queryAsJsonRow(String query) {
        return quotedJson("{ 'statements': [ { 'statement': '" + query + "' } ] }");
    }

    private static String seqDeprecationNotification() {
        return "\"notifications\":[{\"code\":\"Neo.ClientNotification.Request.DeprecatedFormat\","
                + "\"severity\":\"WARNING\",\"title\":\"The client made a request for a format "
                + "which has been deprecated.\",\"description\":\"The requested format has been deprecated. "
                + "('application/vnd.neo4j.jolt+json-seq' and 'application/vnd.neo4j.jolt-v1+json-seq' have been deprecated and will be removed in a "
                + "future version. Please use 'application/vnd.neo4j.jolt-v2+json-seq'.)\"}]";
    }

    private static String lineDeprecationNotification() {
        return "\"notifications\":[{\"code\":\"Neo.ClientNotification.Request.DeprecatedFormat\","
                + "\"severity\":\"WARNING\",\"title\":\"The client made a request for a format "
                + "which has been deprecated.\",\"description\":\"The requested format has been deprecated. "
                + "('application/vnd.neo4j.jolt' and 'application/vnd.neo4j.jolt-v1' have been deprecated and will be removed in a "
                + "future version. Please use 'application/vnd.neo4j.jolt-v2'.)\"}]";
    }

    public static void splitAndVerify(String input, String separator, String... expectedOutput) {
        var splitContent = input.split(separator);

        for (int i = 0; i < expectedOutput.length - 1; i++) {
            assertThat(splitContent[i]).isEqualTo(expectedOutput[i]);
        }

        // final one we do a partial match because the bookmark is generated
        assertThat(splitContent[expectedOutput.length - 1]).startsWith(expectedOutput[expectedOutput.length - 1]);
    }
}
