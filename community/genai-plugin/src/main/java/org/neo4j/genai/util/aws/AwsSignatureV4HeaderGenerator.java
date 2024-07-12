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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.commons.text.StringSubstitutor;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.genai.util.Hashing;
import org.neo4j.genai.util.aws.URLUtils.CanonicalURIComponents;

/**
 * Based on
 * <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">Create a signed AWS API request</a>
 */
public class AwsSignatureV4HeaderGenerator {
    static final HexFormat HEX_FORMAT = HexFormat.of();
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.ROOT);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT);
    private static final String SCOPE_TEMPLATE = "${date}/${region}/bedrock/aws4_request";
    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String AUTHORIZATION_TEMPLATE = ALGORITHM
            + " Credential=${accessKeyId}/${scope},"
            + " SignedHeaders=${signedHeaders},"
            + " Signature=${signature}";

    private final String region;
    private final CanonicalURIComponents canonicalEndpointComponents;
    private final String body;
    private final MutableMultimap<String, String> headers;

    public AwsSignatureV4HeaderGenerator(
            String region, URI endpoint, String body, Multimap<String, String> requestProperties) {
        this.canonicalEndpointComponents = new CanonicalURIComponents(endpoint);
        this.region = region;
        this.body = body;
        this.headers = Multimaps.mutable.list.withAll(requestProperties);
    }

    public Multimap<String, String> generate(String accessKeyId, String secretAccessKey) {
        return generate(ZonedDateTime.now(ZoneOffset.UTC), accessKeyId, secretAccessKey);
    }

    Multimap<String, String> generate(TemporalAccessor time, String accessKeyId, String secretAccessKey) {
        final var datetime = DATE_TIME_FORMATTER.format(time);
        final var date = DATE_FORMATTER.format(time);
        headers.put("X-Amz-Date", datetime);

        final var hashedPayload = HEX_FORMAT.formatHex(Hashing.sha256(body.getBytes(StandardCharsets.UTF_8)));
        headers.put("X-Amz-Content-Sha256", hashedPayload);

        final var canonical = new Canonical(canonicalEndpointComponents, hashedPayload, headers);
        final var scope = Canonical.scope(date, region);
        final var hashedCanonicalRequest = canonical.hashedCanonicalRequest(datetime, scope);
        final var signature = signature(hashedCanonicalRequest, secretAccessKey, date, region);
        final var authorization = StringSubstitutor.replace(
                AUTHORIZATION_TEMPLATE,
                Map.of(
                        "accessKeyId",
                        accessKeyId,
                        "scope",
                        scope,
                        "signedHeaders",
                        canonical.signedHeaders(),
                        "signature",
                        signature));
        headers.put("Authorization", authorization);

        return headers;
    }

    // following the naming of:
    // https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-canonical-request
    static class Canonical {
        private static final Pattern WHITESPACE = Pattern.compile("\\s+");
        private final String canonicalRequest;
        private final String signedHeaders;

        Canonical(
                CanonicalURIComponents canonicalEndpointComponents,
                String hashedPayload,
                Multimap<String, String> headers) {
            final var httpMethod = "POST";
            final var canonicalURI = canonicalEndpointComponents.path();
            final var canonicalQueryString = canonicalEndpointComponents.query();
            final var canonicalHeaders = canonicalHeaders(headers);
            this.signedHeaders = signedHeaders(canonicalHeaders);

            final var request = new StringBuilder();
            request.append(httpMethod)
                    .append('\n')
                    .append(canonicalURI)
                    .append('\n')
                    .append(canonicalQueryString)
                    .append('\n');
            addCanonicalHeadersString(request, canonicalHeaders);
            request.append('\n').append(signedHeaders).append('\n').append(hashedPayload);

            this.canonicalRequest = request.toString();
        }

        String hashedCanonicalRequest(String datetime, String scope) {
            final var hash = HEX_FORMAT.formatHex(Hashing.sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
            return ALGORITHM + '\n' + datetime + '\n' + scope + '\n' + hash;
        }

        String signedHeaders() {
            return signedHeaders;
        }

        static String scope(String date, String region) {
            return StringSubstitutor.replace(SCOPE_TEMPLATE, Map.of("date", date, "region", region));
        }

        static SortedMap<String, List<String>> canonicalHeaders(Multimap<String, String> headers) {
            final var orderedHeaders = new TreeMap<String, List<String>>();
            headers.forEachKeyValue((key, value) -> {
                final var lowerCaseKey = compressWhitespace(key.toLowerCase(Locale.ROOT));
                final var trimmedValue = compressWhitespace(value).trim();
                orderedHeaders
                        .computeIfAbsent(lowerCaseKey, k -> new ArrayList<>())
                        .add(trimmedValue);
            });

            if (!orderedHeaders.containsKey("host")) {
                throw new IllegalArgumentException("HTTP Host header is required");
            }

            return orderedHeaders;
        }

        private static String compressWhitespace(String value) {
            return WHITESPACE.matcher(value).replaceAll(" ");
        }

        static void addCanonicalHeadersString(StringBuilder request, SortedMap<String, List<String>> canonicalHeaders) {
            canonicalHeaders.forEach((key, values) -> {
                request.append(key).append(':');

                boolean first = true;
                for (final var value : values) {
                    if (first) {
                        first = false;
                    } else {
                        request.append(',');
                    }
                    request.append(value);
                }

                request.append('\n');
            });
        }

        static String signedHeaders(SortedMap<String, ?> headers) {
            final var sb = new StringBuilder();
            boolean first = true;
            for (final var key : headers.keySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(';');
                }
                sb.append(key);
            }
            return sb.toString();
        }
    }

    private static String signature(String payload, String secretAccessKey, String date, String region) {
        final var dateKey = Hashing.hmacSha256(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), date);
        final var regionKey = Hashing.hmacSha256(dateKey, region);
        final var serviceKey = Hashing.hmacSha256(regionKey, "bedrock");
        final var signingKey = Hashing.hmacSha256(serviceKey, "aws4_request");
        return HEX_FORMAT.formatHex(Hashing.hmacSha256(signingKey, payload));
    }
}
