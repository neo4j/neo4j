/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup;

class BootFailureException extends RuntimeException {
    private final int exitCode;

    BootFailureException(String msg) {
        this(msg, 1);
    }

    BootFailureException(String msg, int exitCode) {
        this(msg, exitCode, null);
    }

    BootFailureException(String msg, Throwable cause) {
        this(msg, 1, cause);
    }

    BootFailureException(String msg, int exitCode, Throwable cause) {
        super(msg, cause);
        this.exitCode = exitCode;
    }

    int getExitCode() {
        return exitCode;
    }
}
