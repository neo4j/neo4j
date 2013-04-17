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

import java.lang.annotation.Annotation;

/*
 * Put in place as an enum to enforce all error codes remaining collected in one location.
 * Note: These codes will be exposed to the user through our API, although for now they will
 * remain undocumented. There is a discussion to be had about these codes and how we should
 * categorize and pick them.
 *
 * The categories below are an initial proposal, we should have a real discussion about this before
 * anything is documented.
 */
public enum StatusCode
{
    // informal naming convention:
    // *_ERROR:    Transaction is rolled back / aborted
    // INVALID_*:  No change to transaction state (i.e. request is just rejected)

    //
    // 3xxxxx Communication protocol errors
    //
    @StatusCodeDescription( message="Error during communication with the client" )
    COMMUNICATION_ERROR( 30000 ),

    //
    // 4xxxxx User errors
    //
    @StatusCodeDescription( message="Invalid request was not understood by the server." )
    INVALID_REQUEST( 40000 ),
    @StatusCodeDescription( message="Unable to deserialize request due to invalid request format." )
    INVALID_REQUEST_FORMAT( 40001 ),

    @StatusCodeDescription( message="The transaction you asked for cannot be found. "
                                   +"Maybe the transaction has timed out and was rolled back." )
    INVALID_TRANSACTION_ID( 40010 ),
    @StatusCodeDescription( message="The requested transaction is being used concurrently by another request." )
    INVALID_CONCURRENT_TRANSACTION_ACCESS( 40011 ),

    @StatusCodeDescription( message="Error when executing statement." )
    STATEMENT_EXECUTION_ERROR( 42000 ),
    @StatusCodeDescription( message="Syntax error in statement." )
    STATEMENT_SYNTAX_ERROR( 42001 ),
    @StatusCodeDescription( message="Parameter missing in statement." )
    STATEMENT_MISSING_PARAMETER_ERROR( 42002 ),

    //
    // 5xxxxx Database errors
    //
    @StatusCodeDescription( message="Internal database error. Please refer to the attached stack trace for details.",
                            includeStackTrace=true )
    INTERNAL_DATABASE_ERROR( 50000 ),
    @StatusCodeDescription( message="Internal error when executing statement.",
                            includeStackTrace=true )
    INTERNAL_STATEMENT_EXECUTION_ERROR( 50001 ),

    @StatusCodeDescription( message="Unable to start transaction, and unable to determine cause of failure. "
                                  +"Please refer to the database logs for details.",
                            includeStackTrace=true )
    INTERNAL_BEGIN_TRANSACTION_ERROR( 53010 ),
    @StatusCodeDescription( message="Unable to roll back transaction, and unable to determine cause of failure. "
                                   +"Please refer to the database logs for details.",
                            includeStackTrace=true )
    INTERNAL_ROLLBACK_TRANSACTION_ERROR( 53011 ),
    @StatusCodeDescription( message="It was not possible to commit your transaction. "
                                   +"Please refer to the database logs for details.",
                            includeStackTrace=true )
    INTERNAL_COMMIT_TRANSACTION_ERROR( 53012 );

    private final int code;
    private final StatusCodeDescription description;

    StatusCode( int code )
    {
        this.code = code;
        try
        {
            Annotation[] annotations = StatusCode.class.getField( name() ).getDeclaredAnnotations();
            StatusCodeDescription description = null;
            for (Annotation annotation : annotations)
                if ( annotation instanceof  StatusCodeDescription )
                {
                    if ( description == null )
                        description = StatusCodeDescription.class.cast(annotation);
                    else
                        throw new AssertionError( "Duplicate StatusCodeDescription for field " + name() );
                }
            this.description = description;
        }
        catch ( NoSuchFieldException e )
        {
            throw new AssertionError( e );
        }
    }

    public int getCode()
    {
        return code;
    }

    public String getDefaultMessage()
    {
        return getDescription().message();
    }

    public StatusCodeDescription getDescription()
    {
        return description;
    }
}
