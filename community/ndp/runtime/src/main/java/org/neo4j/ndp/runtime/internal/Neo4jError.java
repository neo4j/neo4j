/**
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
package org.neo4j.ndp.runtime.internal;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * An error object, represents something having gone wrong that is to be signaled to the user. This is, by design, not
 * using the java exception system.
 */
public class Neo4jError
{
    private final Status status;
    private final String message;

    public Neo4jError(Status status, String message)
    {
        this.status = status;
        this.message = message;
    }

    public Status status()
    {
        return status;
    }

    public String message()
    {
        return message;
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

        return message.equals( that.message ) && status.equals( that.status );

    }

    @Override
    public int hashCode()
    {
        int result = status.hashCode();
        result = 31 * result + message.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Neo4jError{" +
               "status=" + status +
               ", message='" + message + '\'' +
               '}';
    }

    public static Status codeFromString( String codeStr )
    {
        String[] parts = codeStr.split( "\\." );
        if(parts.length != 4)
        {
            return Status.General.UnknownFailure;
        }

        String category = parts[2];
        String error = parts[3];

        // Note: the input string may contain arbitrary input data, using reflection would open network attack vector
        switch(category)
        {
        case "Schema":
            return Status.Schema.valueOf(error);
        case "LegacyIndex":
            return Status.LegacyIndex.valueOf(error);
        case "General":
            return Status.General.valueOf(error);
        case "Statement":
            return Status.Statement.valueOf(error);
        case "Transaction":
            return Status.Transaction.valueOf(error);
        case "Request":
            return Status.Request.valueOf(error);
        case "Network":
            return Status.Network.valueOf(error);
        default:
            return Status.General.UnknownFailure;
        }
    }
}
