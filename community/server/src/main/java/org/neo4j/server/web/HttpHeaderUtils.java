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
package org.neo4j.server.web;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.InternalLog;

public class HttpHeaderUtils {

    public static final String MAX_EXECUTION_TIME_HEADER = "max-execution-time";
    public static final String ACCESS_MODE_HEADER = "access-mode";
    public static final String BOOKMARKS_HEADER = "bookmarks";

    public static final Map<String, String> CHARSET = Map.of("charset", UTF_8.name());

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpHeaderUtils() {}

    public static MediaType mediaTypeWithCharsetUtf8(MediaType mediaType) {
        Map<String, String> parameters = mediaType.getParameters();
        if (parameters.isEmpty()) {
            return new MediaType(mediaType.getType(), mediaType.getSubtype(), CHARSET);
        }
        if (parameters.containsKey("charset")) {
            return mediaType;
        }
        Map<String, String> paramsWithCharset = new HashMap<>(parameters);
        paramsWithCharset.putAll(CHARSET);
        return new MediaType(mediaType.getType(), mediaType.getSubtype(), paramsWithCharset);
    }

    /**
     * Retrieve custom transaction timeout in milliseconds from numeric {@link #MAX_EXECUTION_TIME_HEADER} request
     * header.
     * If header is not set returns -1.
     * @param request http request
     * @param errorLog errors log for header parsing errors
     * @return custom timeout if header set, -1 otherwise or when value is not a valid number.
     */
    public static long getTransactionTimeout(HttpServletRequest request, InternalLog errorLog) {
        String headerValue = request.getHeader(MAX_EXECUTION_TIME_HEADER);
        return getTransactionTimeout(headerValue, errorLog);
    }

    /**
     * Retrieve custom transaction timeout in milliseconds from numeric {@link #MAX_EXECUTION_TIME_HEADER} request
     * header.
     * If header is not set returns -1.
     * @param headers http headers
     * @param errorLog errors log for header parsing errors
     * @return custom timeout if header set, -1 otherwise or when value is not a valid number.
     */
    public static long getTransactionTimeout(HttpHeaders headers, InternalLog errorLog) {
        String headerValue = headers.getHeaderString(MAX_EXECUTION_TIME_HEADER);
        return getTransactionTimeout(headerValue, errorLog);
    }

    public static Optional<Boolean> getAccessMode(HttpHeaders headers) {
        String headerValue = headers.getHeaderString(ACCESS_MODE_HEADER);

        if (headerValue == null) {
            return Optional.empty();
        } else {
            try {
                return Optional.of(
                        AccessMode.valueOf(headerValue.toUpperCase(Locale.ROOT)).equals(AccessMode.READ));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(
                        String.format(
                                "The header '%s' should be either 'WRITE' or 'READ' but found '%s'.",
                                ACCESS_MODE_HEADER, headerValue),
                        ex);
            }
        }
    }

    public static List<String> getBookmarks(HttpHeaders headers) {
        String headerValue = headers.getHeaderString(BOOKMARKS_HEADER);

        if (headerValue == null || headerValue.length() == 0) {
            return Collections.emptyList();
        }

        try {
            return Arrays.asList(MAPPER.readValue(headerValue, String[].class));
        } catch (IllegalStateException ex) {
            throw new IllegalArgumentException("Only Fabric bookmarks are supported.", ex);
        } catch (IllegalArgumentException | JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Invalid bookmarks header. `bookmarks` must be an array of non-empty string values.", ex);
        }
    }

    private static long getTransactionTimeout(String headerValue, InternalLog errorLog) {
        if (headerValue != null) {
            try {
                return Long.parseLong(headerValue);
            } catch (NumberFormatException e) {
                errorLog.error(
                        String.format(
                                "Fail to parse `%s` header with value: '%s'. Should be a positive number.",
                                MAX_EXECUTION_TIME_HEADER, headerValue),
                        e);
            }
        }
        return GraphDatabaseSettings.UNSPECIFIED_TIMEOUT;
    }

    /**
     * Validates given HTTP header name. Does not allow blank names and names with control characters, like '\n' (LF) and '\r' (CR).
     * Can be used to detect and neutralize CRLF in HTTP headers.
     *
     * @param name the HTTP header name, like 'Accept' or 'Content-Type'.
     * @return {@code true} when given name represents a valid HTTP header, {@code false} otherwise.
     */
    public static boolean isValidHttpHeaderName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        boolean isBlank = true;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isISOControl(c)) {
                return false;
            }
            if (!Character.isWhitespace(c)) {
                isBlank = false;
            }
        }
        return !isBlank;
    }
}
