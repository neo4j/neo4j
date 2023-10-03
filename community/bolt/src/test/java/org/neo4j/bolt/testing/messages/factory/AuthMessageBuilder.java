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
package org.neo4j.bolt.testing.messages.factory;

public interface AuthMessageBuilder<T extends AuthMessageBuilder<T>> extends WireMessageBuilder<T> {
    default T withBasicScheme() {
        return withScheme("basic");
    }

    default T withBasicAuth(String principal, String credentials) {
        return withBasicScheme().withPrincipal(principal).withCredentials(credentials);
    }

    default T withScheme(String scheme) {
        getMeta().put("scheme", scheme);
        return (T) this;
    }

    default T withPrincipal(String principal) {
        getMeta().put("principal", principal);
        return (T) this;
    }

    default T withBadPrincipal(Object principal) {
        getMeta().put("principal", principal);
        return (T) this;
    }

    default T withCredentials(String credentials) {
        getMeta().put("credentials", credentials);
        return (T) this;
    }

    default T withRealm(String realm) {
        getMeta().put("realm", realm);
        return (T) this;
    }
}
