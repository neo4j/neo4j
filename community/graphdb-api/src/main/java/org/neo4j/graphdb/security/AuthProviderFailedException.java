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
package org.neo4j.graphdb.security;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Thrown when a request for authentication or authorization against an external server failed.
 *
 * <p>This is not used for expected failures like wrong principal, credentials or authorization information,
 * but for failures where such information could not be established because of an external server problem,
 * misconfiguration etc.
 */
public class AuthProviderFailedException extends RuntimeException implements Status.HasStatus, HasGqlStatusInfo {
    private static final Status statusCode = Status.Security.AuthProviderFailed;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public AuthProviderFailedException(String message) {
        super(message);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public AuthProviderFailedException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message));
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    public AuthProviderFailedException(String message, Throwable cause) {
        super(message, cause);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public AuthProviderFailedException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    /** The Neo4j status code associated with this exception type. */
    @Override
    public Status status() {
        return statusCode;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
