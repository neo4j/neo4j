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
package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Collection;

public class ExceptionRepresentation extends MappingRepresentation
{
    private final Throwable exception;

    public ExceptionRepresentation( Throwable exception )
    {
        super( RepresentationType.EXCEPTION );
        this.exception = exception;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        String message = exception.getMessage();
        if ( message != null )
        {
            serializer.putString( "message", message );
        }
        serializer.putString( "exception", exception.getClass().getSimpleName() );
        serializer.putString( "fullname", exception.getClass().getName() );
        StackTraceElement[] trace = exception.getStackTrace();
        if ( trace != null )
        {
            Collection<String> lines = new ArrayList<String>( trace.length );
            for ( StackTraceElement element : trace )
            {
                if (element.toString().matches( ".*(jetty|jersey|sun\\.reflect|mortbay|javax\\.servlet).*" )) continue;
                lines.add( element.toString() );
            }
            serializer.putList( "stacktrace", ListRepresentation.string( lines ) );
        }

        Throwable cause = exception.getCause();
        if(cause != null)
        {
            serializer.putMapping( "cause", new ExceptionRepresentation( cause ) );
        }
    }
}
