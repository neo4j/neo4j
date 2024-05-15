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
package org.neo4j.server.rest.dbms;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

public class AuthorizationHeaders {

    public enum Scheme {
        BASIC,
        BEARER
    }

    public record ParsedHeader(Scheme scheme, String[] values) {}

    private AuthorizationHeaders() {}

    /**
     * Extract the encoded Authorization header
     */
    public static ParsedHeader decode(String authorizationHeader) {
        String[] parts = authorizationHeader.trim().split(" ");
        String tokenSegment = parts[parts.length - 1];
        Scheme authScheme;

        try {
            authScheme = Scheme.valueOf(parts[0].toUpperCase());
        } catch (IllegalArgumentException ex) {
            return null;
        }

        switch (authScheme) {
            case BASIC -> {
                if (tokenSegment.isBlank()) {
                    return null;
                }

                String decoded = decodeBase64(tokenSegment);
                if (decoded.isEmpty()) {
                    return null;
                }

                String[] userAndPassword = decoded.split(":", 2);
                if (userAndPassword.length != 2) {
                    return null;
                }

                return new ParsedHeader(Scheme.BASIC, userAndPassword);
            }

            case BEARER -> {
                if (parts.length != 2) {
                    return null;
                }
                return new ParsedHeader(Scheme.BEARER, new String[] {parts[1]});
            }
            default -> {
                return null;
            }
        }
    }

    private static String decodeBase64(String base64) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(base64);
            return new String(decodedBytes, UTF_8);
        } catch (IllegalArgumentException e) {
            return StringUtils.EMPTY;
        }
    }
}
