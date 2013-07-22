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
    NETWORK_ERROR(
            30000, StackTraceStrategy.SEND_TO_CLIENT ),

    //
    // 4xxxxx User errors
    //
    INVALID_REQUEST(
            40000 ),
    INVALID_REQUEST_FORMAT(
            40001 ),

    INVALID_TRANSACTION_ID(
            40010 ),
    INVALID_CONCURRENT_TRANSACTION_ACCESS(
            40011 ),

    STATEMENT_EXECUTION_ERROR(
            42000 ),
    STATEMENT_SYNTAX_ERROR(
            42001 ),
    STATEMENT_MISSING_PARAMETER(
            42002 ),

    COULD_NOT_CREATE_INDEX(
            42101 ),
    COULD_NOT_DROP_INDEX(
            42102 ),
    COULD_NOT_CREATE_CONSTRAINT(
            42103 ),
    COULD_NOT_DROP_CONSTRAINT(
            42104 ),

    //
    // 5xxxxx Database errors
    //
    INTERNAL_DATABASE_ERROR(
            50000,
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_STATEMENT_EXECUTION_ERROR(
            50001,
            StackTraceStrategy.SEND_TO_CLIENT ),

    INTERNAL_BEGIN_TRANSACTION_ERROR(
            53010,
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_ROLLBACK_TRANSACTION_ERROR(
            53011,
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_COMMIT_TRANSACTION_ERROR(
            53012,
            StackTraceStrategy.SEND_TO_CLIENT );

    private final int code;
    private final StackTraceStrategy stackTraceStrategy;

    enum StackTraceStrategy
    {
        SWALLOW, SEND_TO_CLIENT
    }

    StatusCode( int code )
    {
        this (code, StackTraceStrategy.SWALLOW );
    }

    StatusCode( int code, StackTraceStrategy stackTraceStrategy )
    {
        this.code = code;
        this.stackTraceStrategy = stackTraceStrategy;
    }

    public int getCode()
    {
        return code;
    }

    public boolean includeStackTrace()
    {
        return StackTraceStrategy.SEND_TO_CLIENT == stackTraceStrategy;
    }
}
