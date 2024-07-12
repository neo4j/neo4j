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
package org.neo4j.genai.util.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.util.Hashing;

class AwsSignatureV4HeaderGeneratorTest {

    @Nested
    class Canonical {

        @Test
        void missingHeaders() {
            final var noHost = Multimaps.mutable.list.<String, String>of();
            assertThatThrownBy(
                            () -> AwsSignatureV4HeaderGenerator.Canonical.canonicalHeaders(noHost), "headers missing")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("HTTP Host header is required");
        }

        @ParameterizedTest
        @MethodSource
        void headers(Multimap<String, String> headers, String expectedSignedHeaders, String expectedCanonicalHeaders) {
            final var canonicalHeaders = AwsSignatureV4HeaderGenerator.Canonical.canonicalHeaders(headers);

            final var signedHeaderString = AwsSignatureV4HeaderGenerator.Canonical.signedHeaders(canonicalHeaders);
            assertThat(signedHeaderString).as("signed headers").isEqualTo(expectedSignedHeaders);

            final var canonicalHeaderStringBuilder = new StringBuilder();
            AwsSignatureV4HeaderGenerator.Canonical.addCanonicalHeadersString(
                    canonicalHeaderStringBuilder, canonicalHeaders);
            final var canonicalHeaderString = canonicalHeaderStringBuilder.toString();
            assertThat(canonicalHeaderString).as("canonical headers").isEqualTo(expectedCanonicalHeaders);
        }

        private static Stream<Arguments> headers() {
            return Stream.of(
                    Arguments.of(Multimaps.mutable.list.of("Host", "host"), "host", "host:host\n"),
                    Arguments.of(
                            Multimaps.mutable.list.of(
                                    "Host", "host",
                                    "MultiCaseKey", "become lower case"),
                            "host;multicasekey",
                            "host:host\nmulticasekey:become lower case\n"),
                    Arguments.of(
                            Multimaps.mutable.list.of(
                                    "Host", "host",
                                    "Sorted", "alphabetically",
                                    "Keys", "are"),
                            "host;keys;sorted",
                            "host:host\nkeys:are\nsorted:alphabetically\n"),
                    Arguments.of(
                            Multimaps.mutable.list.of(
                                    "Host", "host",
                                    "  \rKeys  are\nwhitespace \t  compressed   ", "but not trimmed"),
                            " keys are whitespace compressed ;host",
                            " keys are whitespace compressed :but not trimmed\nhost:host\n"),
                    Arguments.of(
                            Multimaps.mutable.list.of(
                                    "Host", "host",
                                    "key", "  values\nare\ttrimmed\r  and whitespace  \r\n  compressed  "),
                            "host;key",
                            "host:host\nkey:values are trimmed and whitespace compressed\n"),
                    Arguments.of(
                            Multimaps.mutable.list.of(
                                    "MultiValue", "order",
                                    "Host", "host",
                                    "MultiValue", "kept"),
                            "host;multivalue",
                            "host:host\nmultivalue:order,kept\n"));
        }

        @Test
        void scope() {
            final var date = "19700101";
            final var region = "fake-region";
            final var scope = AwsSignatureV4HeaderGenerator.Canonical.scope(date, region);
            assertThat(scope).isEqualTo("19700101/fake-region/bedrock/aws4_request");
        }

        @Test
        void request() {
            // dummy request with easy~ish to follow fake inputs

            final var canonicalURIComponents = new URLUtils.CanonicalURIComponents(
                    URI.create("https://sub.host.tld/path/to/other/../resource?param=value&param=other&exists"));
            final var hashedPayload = "973153f86ec2da1748e63f0cf85b89835b42f8ee8018c549868a1308a19f6ca3";
            final var datetime = "19700101T000000Z";
            final var scope = "19700101/fake-region/bedrock/aws4_request";

            final var request =
                    """
                    POST
                    /path/to/resource
                    exists=&param=other&param=value
                    content-type:text/plain
                    host:sub.host.tld

                    content-type;host
                    973153f86ec2da1748e63f0cf85b89835b42f8ee8018c549868a1308a19f6ca3""";
            final var hashedRequestPrefix =
                    """
                    AWS4-HMAC-SHA256
                    19700101T000000Z
                    19700101/fake-region/bedrock/aws4_request
                    """;
            final var hashedRequest = hashedRequestPrefix
                    + AwsSignatureV4HeaderGenerator.HEX_FORMAT.formatHex(
                            Hashing.sha256(request.getBytes(StandardCharsets.UTF_8)));

            final var canonicalRequest = new AwsSignatureV4HeaderGenerator.Canonical(
                    canonicalURIComponents,
                    hashedPayload,
                    Multimaps.mutable.list.of("Host", canonicalURIComponents.host(), "Content-Type", "text/plain"));

            assertThat(canonicalRequest.signedHeaders()).as("signed headers").isEqualTo("content-type;host");
            assertThat(canonicalRequest.hashedCanonicalRequest(datetime, scope))
                    .as("hashed canonical request")
                    .isEqualTo(hashedRequest);
        }
    }

    @Test
    void knownRequest() {
        // following request headers were part of a valid, and authorized request
        final var region = "us-east-1";
        final var endpoint =
                URI.create("https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.titan-embed-text-v1/invoke");
        final var body =
                "{\"inputText\":\"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\"}";
        final var requestProperties = headers(
                "Host", "bedrock-runtime.us-east-1.amazonaws.com",
                "User-Agent", "Neo4j-GenAIProcedures/1.0.0",
                "Content-Type", "application/json",
                "Accept", "application/json");
        final var datetime = ZonedDateTime.of(2023, 11, 21, 16, 41, 21, 0, ZoneOffset.UTC);

        // following credentials have explicitly been revoked
        final var accessKeyId = "AKIAXZRNW77LNNSMHOZL";
        final var secretAccessKey = "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";

        final var expectedHeaders = Multimaps.mutable.list.withAll(requestProperties);
        expectedHeaders.put("X-Amz-Date", "20231121T164121Z");
        expectedHeaders.put("X-Amz-Content-Sha256", "e3279a0a20442f4d6519874f10dd8af7eab838b7503b11265859b02629651cd5");
        expectedHeaders.put(
                "Authorization",
                "AWS4-HMAC-SHA256"
                        + " Credential=AKIAXZRNW77LNNSMHOZL/20231121/us-east-1/bedrock/aws4_request,"
                        + " SignedHeaders=accept;content-type;host;user-agent;x-amz-content-sha256;x-amz-date,"
                        + " Signature=ac68953c731e4e29486fba6d97685dffd1ed87b457ba538082845f6d9315e2c2");

        final var headers = new AwsSignatureV4HeaderGenerator(region, endpoint, body, requestProperties)
                .generate(datetime, accessKeyId, secretAccessKey);

        assertThat(headers.keyValuePairsView())
                .containsExactlyInAnyOrderElementsOf(expectedHeaders.keyValuePairsView());
    }

    private static Multimap<String, String> headers(String... args) {
        if (args == null || (args.length & 1) != 0) {
            throw new IllegalArgumentException("require even number of arguments");
        }

        final var requestProperties = Multimaps.mutable.list.<String, String>empty();
        for (int i = 0; i < args.length; i += 2) {
            final var key = args[i];
            final var value = args[i + 1];
            requestProperties.put(key, value);
        }
        return requestProperties;
    }
}
