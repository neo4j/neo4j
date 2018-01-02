/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb.security;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * Thrown when the database is asked to perform an action that is not authorized based on the AccessMode settings.
 *
 * For instance, if attempting to write with READ_ONLY rights.
 */
public class AuthorizationViolationException extends RuntimeException implements Status.HasStatus
{
    public static final String PERMISSION_DENIED = "Permission denied.";

    private Status statusCode = Status.Security.Forbidden;

    public AuthorizationViolationException( String msg, Status statusCode )
    {
        super( msg );
        this.statusCode = statusCode;
    }

    public AuthorizationViolationException( String msg )
    {
        super( msg );
    }

    public AuthorizationViolationException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

    /** The Neo4j status code associated with this exception type. */
    @Override
    public Status status()
    {
        return statusCode;
    }
}
