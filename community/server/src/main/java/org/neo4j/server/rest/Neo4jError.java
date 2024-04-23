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
package org.neo4j.server.rest;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * This is an initial move towards unified errors - it should not live here in the server, but should probably
 * exist in the kernel or similar, where it can be shared across surfaces other than the server.
 * <p>
 * It's put in place here in order to enforce that the {@link org.neo4j.server.http.cypher.CypherResource}
 * is strictly tied down towards what errors it handles and returns to the client, to create a waterproof abstraction
 * between the runtime-exception landscape that lives below, and the errors we send to the user.
 * <p>
 * This way, we make it easy to transition this service over to a unified error code based error scheme.
 */
public class Neo4jError {
    private final Status status;
    private final Throwable cause;

    public Neo4jError(Status status, String message) {
        this(status, new RuntimeException(message));
    }

    public Neo4jError(Status status, Throwable cause) {
        if (status == null) {
            throw new IllegalArgumentException("statusCode must not be null");
        }
        if (cause == null) {
            throw new IllegalArgumentException("cause must not be null");
        }

        this.status = status;
        this.cause = cause;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, cause=\"%s\"]", getClass().getSimpleName(), status.code(), cause);
    }

    public Throwable cause() {
        return cause;
    }

    public Status status() {
        return status;
    }

    public String getMessage() {
        return cause.getMessage();
    }
}
