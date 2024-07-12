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

import static java.lang.String.CASE_INSENSITIVE_ORDER;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.util.VisibleForTesting;

public class URLUtils {
    private URLUtils() {}

    private static final Pattern QUERY_SEPARATOR = Pattern.compile("&");
    private static final Pattern QUERY_PARAM_VALUE_SEPARATOR = Pattern.compile("=");

    /**
     * Reencode given URI into Canonical URI components according to AWS specifications.
     * AWS requires a very specific encoding for URLs, primarily for consistency for hashing purposes.
     * AWS documentation suggests implementing your own:
     * <blockquote cite="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">
     *     The standard UriEncode functions provided by your development platform may not work because of differences in
     *     implementation and related ambiguity in the underlying RFCs.
     *     We recommend that you write your own custom UriEncode function to ensure that your encoding will work.
     *     <cite><a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">AWS API Canonical URL</a></cite>
     * </blockquote>
     */
    record CanonicalURIComponents(
            String scheme, String userInfo, String host, int port, String path, String query, String fragment) {
        CanonicalURIComponents(URI uri) {
            this(
                    uri.getScheme(),
                    URLEncoder.encode(uri.getUserInfo()),
                    uri.getHost(),
                    uri.getPort(),
                    reencodedPathFrom(uri),
                    reencodedQueryFrom(uri),
                    URLEncoder.encode(uri.getFragment()));
        }
    }

    private static String reencodedPathFrom(URI uri) {
        final var path = uri.normalize().getPath();
        return (path != null && !path.isEmpty()) ? URLEncoder.encodePath(path) : "/";
    }

    private static String reencodedQueryFrom(URI uri) {
        final var rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }

        final var queries = new TreeMap<String, List<String>>(CASE_INSENSITIVE_ORDER);
        for (final var query : QUERY_SEPARATOR.split(rawQuery)) {
            final var pv = QUERY_PARAM_VALUE_SEPARATOR.split(query);
            final var param = pv[0];
            final var value = (pv.length > 1) ? pv[1] : "";
            queries.computeIfAbsent(reencode(param), k -> new ArrayList<>()).add(reencode(value));
        }

        final var sb = new StringBuilder();
        boolean first = true;
        for (final var entry : queries.entrySet()) {
            final var param = entry.getKey();
            final var values = entry.getValue();
            Collections.sort(values);

            for (final var value : values) {
                if (first) {
                    first = false;
                } else {
                    sb.append(QUERY_SEPARATOR);
                }

                sb.append(param).append(QUERY_PARAM_VALUE_SEPARATOR).append(value);
            }
        }
        return sb.toString();
    }

    static String reencode(String param) {
        return URLEncoder.encode(URLDecoder.decode(param));
    }

    static class URLEncoder {
        private static final Pattern ENCODED_CHARACTERS = Pattern.compile(
                Stream.of("+", "*", "%7E", "%2F").map(Pattern::quote).collect(Collectors.joining("|")));

        private URLEncoder() {}

        static String encode(String value) {
            return encode(value, false);
        }

        static String encodePath(String value) {
            return encode(value, true);
        }

        private static String encode(String value, boolean path) {
            if (value == null) {
                return null;
            }

            if (value.isEmpty()) {
                return "";
            }

            final var encoded = java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
            final var matcher = ENCODED_CHARACTERS.matcher(encoded);
            final var sb = new StringBuilder(value.length());
            while (matcher.find()) {
                matcher.appendReplacement(sb, replacement(matcher, path));
            }
            matcher.appendTail(sb);
            return sb.toString();
        }

        // AWS's requirements of the encoding
        private static String replacement(Matcher matcher, boolean path) {
            final var match = matcher.group();
            return switch (match) {
                case "+" -> "%20";
                case "*" -> "%2A";
                case "%7E" -> "~";
                case "%2F" -> path ? "/" : "%2F";
                default -> throw new IllegalStateException("Unexpected value: '%s'".formatted(match));
            };
        }
    }

    @VisibleForTesting
    static class URLDecoder {
        private URLDecoder() {}

        static String decode(String value) {
            if (value == null) {
                return null;
            }

            if (value.isEmpty()) {
                return "";
            }

            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        }
    }
}
