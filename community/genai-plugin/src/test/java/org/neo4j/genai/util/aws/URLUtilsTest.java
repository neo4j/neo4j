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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.neo4j.genai.util.aws.URLUtils.CanonicalURIComponents;
import org.neo4j.genai.util.aws.URLUtils.URLDecoder;
import org.neo4j.genai.util.aws.URLUtils.URLEncoder;

class URLUtilsTest {
    private static final Pattern AWS_RESERVED = Pattern.compile("[^\\p{Alnum}-._~]");
    private static final HexFormat HEX_FORMAT = HexFormat.of().withPrefix("%").withUpperCase();

    private static final byte[] PRINTABLE_ASCII_BYTES;
    private static final String PRINTABLE_ASCII;
    private static final String FULLY_PERCENT_ENCODED_PRINTABLE_ASCII;

    static {
        final byte start = 0x20;
        final byte end = 0x7e;
        PRINTABLE_ASCII_BYTES = new byte[1 + end - start];
        for (int i = 0; i < PRINTABLE_ASCII_BYTES.length; i++) {
            PRINTABLE_ASCII_BYTES[i] = (byte) (start + i);
        }

        PRINTABLE_ASCII = new String(PRINTABLE_ASCII_BYTES, StandardCharsets.UTF_8);

        FULLY_PERCENT_ENCODED_PRINTABLE_ASCII = hexFormat(PRINTABLE_ASCII_BYTES);
    }

    private static String hexFormat(String string) {
        return HEX_FORMAT.formatHex(string.getBytes(StandardCharsets.UTF_8));
    }

    private static String hexFormat(byte... bytes) {
        return HEX_FORMAT.formatHex(bytes);
    }

    @Nested
    class Decoder {

        @ParameterizedTest
        @NullAndEmptySource
        void decodeEmpty(String empty) {
            assertThat(URLDecoder.decode(empty)).isNullOrEmpty();
        }

        @ParameterizedTest
        @MethodSource
        void decode(String encoded, String decoded) {
            assertThat(URLDecoder.decode(encoded)).isEqualTo(decoded);
        }

        private static Stream<Arguments> decode() {
            return Stream.of(
                    Arguments.of("plain", "plain"),
                    Arguments.of("with+%24ome sym/bols&char%2Acters~", "with $ome sym/bols&char*cters~"),
                    Arguments.of(
                            "with%2B%C3%85L%C2%A3%20sym%2Fbols%26char%2Acters%7E", "with+ÅL£ sym/bols&char*cters~"),
                    Arguments.of(FULLY_PERCENT_ENCODED_PRINTABLE_ASCII, PRINTABLE_ASCII));
        }
    }

    @Nested
    final class RegularValueEncoder extends Encoder {
        RegularValueEncoder() {
            super(false);
        }

        @Override
        String encode(String value) {
            return URLEncoder.encode(value);
        }

        @Test
        void encodeSlash() {
            assertThat(encode("/slashes/are/encoded")).isEqualTo("%2Fslashes%2Fare%2Fencoded");
        }
    }

    @Nested
    final class PathValueEncoder extends Encoder {
        PathValueEncoder() {
            super(true);
        }

        @Override
        String encode(String value) {
            return URLEncoder.encodePath(value);
        }

        @Test
        void encodeSlash() {
            assertThat(encode("/slashes/are/not/encoded")).isEqualTo("/slashes/are/not/encoded");
        }
    }

    abstract static sealed class Encoder permits RegularValueEncoder, PathValueEncoder {
        private static final Pattern PERCENT_ENCODED = Pattern.compile("%\\p{XDigit}{2}");
        private final boolean path;

        Encoder(boolean path) {
            this.path = path;
        }

        abstract String encode(String value);

        @ParameterizedTest
        @NullAndEmptySource
        void encodeEmpty(String empty) {
            assertThat(encode(empty)).isNullOrEmpty();
        }

        @ParameterizedTest
        @MethodSource
        void encode(String decoded, String encoded) {
            assertThat(encode(decoded)).isEqualTo(encoded);
        }

        private static Stream<Arguments> encode() {
            return Stream.of(
                    Arguments.of("plain", "plain"),
                    Arguments.of("spaces are encoded", "spaces%20are%20encoded"),
                    Arguments.of("pluses+are+not+spaces", "pluses%2Bare%2Bnot%2Bspaces"),
                    Arguments.of("asterisks*are*encoded", "asterisks%2Aare%2Aencoded"),
                    Arguments.of("tildes~are~not~encoded", "tildes~are~not~encoded"));
        }

        @Test
        void encodeRequirements() {
            final var encoded = encode(PRINTABLE_ASCII);
            final var matches = PERCENT_ENCODED.matcher(encoded);
            while (matches.find()) {
                assertThat(matches.group())
                        .as("uppercase hexadecimal representation")
                        .isUpperCase();
            }

            assertThat(encoded)
                    .doesNotContain("+", "*", "%7E", path ? "%2F" : "/")
                    .contains("%20", "%2A", "~", path ? "/" : "%2F");
        }
    }

    @Test
    void reencode() {
        final var reencoded = URLUtils.reencode(FULLY_PERCENT_ENCODED_PRINTABLE_ASCII);

        // 'quick' dumb encoding for sanity check
        final var matcher = AWS_RESERVED.matcher(PRINTABLE_ASCII);
        final var sb = new StringBuilder(3 * PRINTABLE_ASCII.length());
        while (matcher.find()) {
            matcher.appendReplacement(sb, hexFormat(matcher.group()));
        }
        matcher.appendTail(sb);

        assertThat(reencoded).isEqualTo(sb.toString());
    }

    @Nested
    class CanonicalURL {
        @ParameterizedTest
        @MethodSource
        void path(String path, String canonicalPath) throws URISyntaxException {
            final var uri = new URI("https", "host", path, null, null);
            final var canonicalURI = new CanonicalURIComponents(uri);
            assertThat(canonicalURI.path()).isEqualTo(canonicalPath);
        }

        private static Stream<Arguments> path() {
            return Stream.of(
                    Arguments.of(null, "/"),
                    Arguments.of("", "/"),
                    Arguments.of("/", "/"),
                    Arguments.of("//", "/"),
                    Arguments.of("/path", "/path"),
                    Arguments.of("/longer/path", "/longer/path"),
                    Arguments.of("/path/with spaces", "/path/with%20spaces"),
                    Arguments.of("/path/with-dashes", "/path/with-dashes"),
                    Arguments.of("/path/with_underscores", "/path/with_underscores"),
                    Arguments.of("/path/should/./be/normalized", "/path/should/be/normalized"),
                    Arguments.of("/path/should/be/../../normalized", "/path/normalized"));
        }

        @ParameterizedTest
        @MethodSource
        void query(String query, String canonicalQuery) throws URISyntaxException {
            final var uri = new URI("https", "host", null, query, null);
            final var canonicalURI = new CanonicalURIComponents(uri);
            assertThat(canonicalURI.query()).isEqualTo(canonicalQuery);
        }

        private static Stream<Arguments> query() {
            return Stream.of(
                    Arguments.of(null, ""),
                    Arguments.of("", ""),
                    Arguments.of("param=value", "param=value"),
                    Arguments.of("noValue", "noValue="),
                    Arguments.of("query=key&ordered=by", "ordered=by&query=key"),
                    Arguments.of("multi=value&multi=ordered", "multi=ordered&multi=value"),
                    Arguments.of("param=value with spaces", "param=value%20with%20spaces"),
                    Arguments.of("param_with $ymbols=value", "param_with%20%24ymbols=value"),
                    Arguments.of("slashes/are=percent/encoded", "slashes%2Fare=percent%2Fencoded"));
        }
    }
}
