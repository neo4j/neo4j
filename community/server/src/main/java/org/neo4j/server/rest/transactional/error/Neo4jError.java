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
public abstract class Neo4jError extends Exception
{

    /*
     * Put in place as an enum to enforce all error codes remaining collected in one location.
     * Note: These codes will be exposed to the user through our API, although for now they will
     * remain undocumented. There is a discussion to be had about these codes and how we should
     * categorize and pick them.
     *
     * The categories below are an initial proposal, we should have a real discussion about this before
     * anything is documented.
     */
    public enum Code
    {
        // 00000-09999 : User errors - Invalid syntax, impossible statements
        INVALID_REQUEST( 100 ),

        STATEMENT_MISSING_PARAMETER( 1001 ),

        // 10000-19999 : Data errors - constraint violations, resources not found
        INVALID_TRANSACTION_ID( 10010 ),
        CONCURRENT_TRANSACTION_ACCESS( 10011 ),

        // 20000-29999 : Database errors - Database shut down, unable to tell what went wrong
        UNKNOWN_DATABASE_ERROR( 20000 ),
        UNKNOWN_COMMIT_ERROR( 20001 ),
        UNKNOWN_ROLLBACK_ERROR( 20002 ),

        UNABLE_TO_START_TRANSACTION( 20010 ),

        UNKNOWN_STATEMENT_ERROR( 20100 ),

        // 30000-39999 : Cluster errors - Election fraud, unable to join cluster, remote locks timed out

        // 40000-49999 : Infrastructure errors - Network failures, memory issues, disk space et cetera
        CLIENT_COMMUNICATION_ERROR( 40100 );

        private final long code;

        private Code( long code )
        {
            this.code = code;
        }

        public long getCode()
        {
            return code;
        }
    }

    private final Code errorCode;

    public Neo4jError( Code errorCode, String message, Throwable cause )
    {
        super( message, cause );
        this.errorCode = errorCode;
    }

    public Code getErrorCode()
    {
        return errorCode;
    }

}
