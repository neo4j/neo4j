/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.error;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This is an initial move towards unified errors - it should not live here in the server, but should probably
 * exist in the kernel or similar, where it can be shared across surfaces other than the server.
 * <p/>
 * It's put in place here in order to enforce that the {@link org.neo4j.server.rest.web.TransactionalService}
 * is strictly tied down towards what errors it handles and returns to the client, to create a waterproof abstraction
 * between the runtime-exception landscape that lives below, and the errors we send to the user.
 * <p/>
 * This way, we make it easy to transition this service over to a unified error code based error scheme.
 */
public class Neo4jError
{
    private final StatusCode statusCode;
    private final String message;
    private final Throwable cause;

    public Neo4jError( StatusCode statusCode )
    {
        this( statusCode, null, null );
    }

    public Neo4jError( StatusCode statusCode, Throwable cause )
    {
        this( statusCode, null, cause );
    }

    public Neo4jError( StatusCode statusCode, String message )
    {
        this( statusCode, message, null );
    }

    public Neo4jError( StatusCode statusCode, String message, Throwable cause )
    {
        if ( statusCode == null  )
            throw new IllegalArgumentException( "statusCode must not be null" );

        this.statusCode = statusCode;
        this.cause = cause;
        this.message = buildMessage( statusCode, message, cause );
    }

    public StatusCode getStatusCode()
    {
        return statusCode;
    }

    public String getMessage()
    {
        return message;
    }

    public boolean shouldSerializeStackTrace()
    {
        return statusCode.getDescription().includeStackTrace();
    }

    public String getStackTraceAsString()
    {
        if ( cause == null )
            return "";
        else
        {
            StringWriter stringWriter = new StringWriter(  );
            PrintWriter printWriter = new PrintWriter( stringWriter );
            cause.printStackTrace( printWriter );
            return stringWriter.toString();
        }
    }

    private String buildMessage( StatusCode statusCode, String message, Throwable cause )
    {
        StringBuilder builder = new StringBuilder( statusCode.getDefaultMessage() );
        if ( message != null ) {
            builder.append( " Details: ");
            builder.append( message );
            return builder.toString();
        }
        if ( cause != null )
        {
            builder.append( " Cause: ");
            builder.append( cause.getMessage() );
            return builder.toString();
        }
        return builder.toString();
    }
}
