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
package org.neo4j.kernel.impl.security;

import java.util.Set;

/**
 * Controls authorization and authentication for an individual user.
 * A user persisted in the system graph will always have an id.
 */
public record User(
        String name,
        String id,
        SensitiveCredential credential,
        boolean passwordChangeRequired,
        boolean suspended,
        Set<Auth> auth) {
    public static final String PASSWORD_CHANGE_REQUIRED = "password_change_required";
    private static final Set<Auth> EMPTY_AUTH_SET = Set.of();

    public User(String name, String id, Credential credential, boolean passwordChangeRequired, boolean suspended) {
        this(name, id, new SensitiveCredential(credential), passwordChangeRequired, suspended, EMPTY_AUTH_SET);
    }

    public record SensitiveCredential(Credential value) {
        @Override
        public String toString() {
            return (value != null) ? "*****" : "null";
        }
    }

    public record Auth(String provider, String id) {}
}
