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
package org.neo4j.server.security.auth;

import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;

public class SecurityTestUtils {
    private SecurityTestUtils() {}

    public static Map<String, Object> authToken(String username, String password) {
        return newBasicAuthToken(username, password);
    }

    public static byte[] password(String passwordString) {
        return passwordString != null ? passwordString.getBytes(StandardCharsets.UTF_8) : null;
    }

    public static SystemGraphCredential credentialFor(String passwordString) {
        return SystemGraphCredential.createCredentialForPassword(password(passwordString), new SecureHasher());
    }
}
