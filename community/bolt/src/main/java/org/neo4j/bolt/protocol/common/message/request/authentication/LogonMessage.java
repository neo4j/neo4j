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
package org.neo4j.bolt.protocol.common.message.request.authentication;

import java.util.Map;
import java.util.Objects;

public final class LogonMessage implements AuthenticationMessage {
    public static final byte SIGNATURE = 0x6A;

    private final Map<String, Object> authToken;

    public LogonMessage(Map<String, Object> authToken) {
        this.authToken = authToken;
    }

    @Override
    public Map<String, Object> authToken() {
        return authToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogonMessage that = (LogonMessage) o;
        return Objects.equals(authToken, that.authToken());
    }

    @Override
    public int hashCode() {
        return Objects.hash(authToken);
    }

    @Override
    public String toString() {
        return "LogonMessage{" + "authToken=" + authToken + '}';
    }
}
