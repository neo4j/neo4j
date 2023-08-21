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
package org.neo4j.shell.test;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

import java.net.URI;
import java.util.Optional;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.cli.Encryption;

public final class Util {
    private Util() {}

    public static String[] asArray(String... arguments) {
        return arguments;
    }

    public static ConnectionConfig testConnectionConfig(String uri) {
        return testConnectionConfig(uri, Encryption.DEFAULT);
    }

    public static ConnectionConfig testConnectionConfig(String uri, Encryption encryption) {
        return new ConnectionConfig(URI.create(uri), "user", "pass", encryption, ABSENT_DB_NAME, Optional.empty());
    }

    public static class NotImplementedYetException extends RuntimeException {
        public NotImplementedYetException(String message) {
            super(message);
        }
    }
}
