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
package org.neo4j.server.queryapi.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.server.queryapi.exception.InvalidAuthenticationHeaderException;
import org.neo4j.server.queryapi.exception.QueryApiException;

/**
 * Factory for creating {@link AuthToken} instances for Query API.
 */
public class AuthTokenFactory {

    private enum Scheme {
        BASIC,
        BEARER
    }

    private static final String REALM_HEADER = "Neo4j-Realm";
    public static final String BEARER_SCHEME = "Bearer";

    /**
     * Creates an {@link AuthToken} from HTTP request.
     * @param request The HTTP request
     * @return The non-null {@link AuthToken}
     * @throws org.neo4j.server.queryapi.exception.QueryApiException On malformed authtoken
     */
    public static AuthToken fromHttpServletRequest(HttpServletRequest request) throws QueryApiException {
        var authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
        var realm = request.getHeader(REALM_HEADER);

        if (authHeader == null || authHeader.isEmpty()) {
            return AuthTokens.none();
        }

        var parts = authHeader.trim().split(" ");
        var tokenSegment = parts[parts.length - 1];
        AuthTokenFactory.Scheme authScheme;

        try {
            authScheme = Scheme.valueOf(parts[0].toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new InvalidAuthenticationHeaderException();
        }

        switch (authScheme) {
            case BASIC:
                if (tokenSegment.isBlank()) {
                    throw new InvalidAuthenticationHeaderException();
                }

                String decoded = decodeBase64(tokenSegment);
                if (decoded.isEmpty()) {
                    throw new InvalidAuthenticationHeaderException();
                }

                String[] userAndPassword = decoded.split(":", 2);
                if (userAndPassword.length != 2) {
                    throw new InvalidAuthenticationHeaderException();
                }
                return AuthTokens.basic(userAndPassword[0], userAndPassword[1], realm);
            case BEARER:
                if (parts.length != 2) {
                    throw new InvalidAuthenticationHeaderException();
                }
                return AuthTokens.custom("", parts[1], realm, BEARER_SCHEME);
            default:
                throw new InvalidAuthenticationHeaderException();
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
