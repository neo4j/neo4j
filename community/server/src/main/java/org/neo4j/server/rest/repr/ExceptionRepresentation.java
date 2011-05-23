/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

class ExceptionRepresentation extends MappingRepresentation
{
    private final Throwable exception;
    private String message;

    /**
     * @param exception The exception that caused the response. Contents will be rendered in the response body
     * @param message A descriptive message for the response body that describes the reason for the fault.
     */
    ExceptionRepresentation( Throwable exception )
    {
        super( RepresentationType.EXCEPTION );
        this.exception = exception;
        this.message = exception.getMessage();
    }

    /**
     * @param exception The exception that caused the response. Contents will be rendered in the response body
     * @param message A descriptive message for the response body that describes the reason for the fault.
     */
    ExceptionRepresentation( Throwable exception, String message )
    {
        this( exception );
        this.message = message;
    }

    
    @Override
    protected void serialize( MappingSerializer serializer )
    {
        if(message != null) {
            serializer.putString( "message", message );
        } 
        serializer.putString( "exception", exception.toString() );
        StackTraceElement[] trace = exception.getStackTrace();
        if ( trace != null )
        {
            String[] lines = new String[trace.length];
            for ( int i = 0; i < lines.length; i++ )
            {
                lines[i] = trace[i].toString();
            }
            serializer.putList( "stacktrace", ListRepresentation.strings( lines ) );
        }
    }
}
