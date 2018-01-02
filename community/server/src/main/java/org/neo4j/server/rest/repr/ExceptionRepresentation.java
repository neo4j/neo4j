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
package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class ExceptionRepresentation extends MappingRepresentation
{
    private final List<Neo4jError> errors = new LinkedList<>();
    private boolean includeLegacyRepresentation;

    public ExceptionRepresentation( Throwable exception )
    {
        this( exception, true );
    }
    public ExceptionRepresentation( Throwable exception, boolean includeLegacyRepresentation )
    {
        super( RepresentationType.EXCEPTION );
        this.errors.add( new Neo4jError( statusCode( exception ), exception ) );
        this.includeLegacyRepresentation = includeLegacyRepresentation;
    }

    public ExceptionRepresentation( Neo4jError ... errors )
    {
        super( RepresentationType.EXCEPTION );
        for ( Neo4jError exception : errors )
        {
            this.errors.add( exception );
        }
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        // For legacy reasons, this actually serializes into two separate formats - the old format, which simply
        // serializes a single exception, and the new format which serializes multiple errors and provides simple
        // status codes.
        if(includeLegacyRepresentation)
        {
            renderWithLegacyFormat( errors.get( 0 ).cause(), serializer );
        }

        renderWithStatusCodeFormat( serializer );
    }

    private void renderWithStatusCodeFormat( MappingSerializer serializer )
    {
        serializer.putList( "errors", ErrorEntryRepresentation.list( errors ) );
    }

    private void renderWithLegacyFormat( Throwable exception, MappingSerializer serializer )
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
            serializer.putList( "stackTrace", ListRepresentation.string( lines ) );
        }

        Throwable cause = exception.getCause();
        if(cause != null)
        {
            serializer.putMapping( "cause", new ExceptionRepresentation( cause ) );
        }
    }

    private static class ErrorEntryRepresentation extends MappingRepresentation
    {
        private final Neo4jError error;

        public ErrorEntryRepresentation( Neo4jError error )
        {
            super( "error-entry" );
            this.error = error;
        }

        @Override
        protected void serialize( MappingSerializer serializer )
        {
            serializer.putString( "code", error.status().code().serialize() );
            serializer.putString( "message", error.getMessage() );
            if(error.shouldSerializeStackTrace())
            {
                serializer.putString( "stackTrace", error.getStackTraceAsString() );
            }
        }

        public static ListRepresentation list( Collection<Neo4jError> errors )
        {
            return new ListRepresentation( "error-list", new IterableWrapper<ErrorEntryRepresentation, Neo4jError>( errors )
            {
                @Override
                protected ErrorEntryRepresentation underlyingObjectToObject( Neo4jError error )
                {
                    return new ErrorEntryRepresentation( error );
                }
            } );
        }
    }

    private static Status statusCode( Throwable current )
    {
        while(current != null)
        {
            if(current instanceof Status.HasStatus)
            {
                return ((Status.HasStatus)current).status();
            }
            current = current.getCause();
        }
        return Status.General.UnknownFailure;
    }
}
