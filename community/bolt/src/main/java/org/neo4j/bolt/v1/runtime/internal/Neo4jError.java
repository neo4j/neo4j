/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.UUID;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * An error object, represents something having gone wrong that is to be signaled to the user. This is, by design, not
 * using the java exception system.
 */
public class Neo4jError
{
    private final Status status;
    private final String message;
    private final Throwable cause;
    private final UUID reference;

    public Neo4jError( Status status, String message, Throwable cause )
    {
        this.status = status;
        this.message = message;
        this.cause = cause;
        this.reference = UUID.randomUUID();
    }

    public Neo4jError( Status status, String message )
    {
        this(status, message, null);
    }

    public Status status()
    {
        return status;
    }

    public String message()
    {
        return message;
    }

    public Throwable cause()
    {
        return cause;
    }

    public UUID reference()
    {
        return reference;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Neo4jError that = (Neo4jError) o;

        if ( status != null ? !status.equals( that.status ) : that.status != null )
        {
            return false;
        }
        return !(message != null ? !message.equals( that.message ) : that.message != null);

    }

    @Override
    public int hashCode()
    {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Neo4jError{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", cause=" + cause +
                ", reference=" + reference +
                '}';
    }

    public static Status codeFromString( String codeStr )
    {
        String[] parts = codeStr.split( "\\." );
        if ( parts.length != 4 )
        {
            return Status.General.UnknownFailure;
        }

        String category = parts[2];
        String error = parts[3];

        // Note: the input string may contain arbitrary input data, using reflection would open network attack vector
        switch ( category )
        {
        case "Schema":
            return Status.Schema.valueOf( error );
        case "LegacyIndex":
            return Status.LegacyIndex.valueOf( error );
        case "General":
            return Status.General.valueOf( error );
        case "Statement":
            return Status.Statement.valueOf( error );
        case "Transaction":
            return Status.Transaction.valueOf( error );
        case "Request":
            return Status.Request.valueOf( error );
        case "Network":
            return Status.Network.valueOf( error );
        case "Security":
            return Status.Security.valueOf( error );
        default:
            return Status.General.UnknownFailure;
        }
    }

    public static Neo4jError from( Throwable any )
    {
        for( Throwable cause = any; cause != null; cause = cause.getCause() )
        {
            if ( cause instanceof Status.HasStatus )
            {
                return new Neo4jError( ((Status.HasStatus) cause).status(), any.getMessage(), any );
            }
        }

        // In this case, an error has "slipped out", and we don't have a good way to handle it. This indicates
        // a buggy code path, and we need to try to convince whoever ends up here to tell us about it.


        return new Neo4jError( Status.General.UnknownFailure, any.getMessage(), any );
    }

}
