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
            30000, "Failed to transfer request or response.",
            StackTraceStrategy.SEND_TO_CLIENT ),

    //
    // 4xxxxx User errors
    //
    INVALID_REQUEST(
            40000, "Invalid request was not understood by the server." ),
    INVALID_REQUEST_FORMAT(
            40001, "Unable to deserialize request due to invalid request format." ),

    INVALID_TRANSACTION_ID(
            40010, "Unrecognized transaction id. Transaction may have timed out and been rolled back." ),
    INVALID_CONCURRENT_TRANSACTION_ACCESS(
            40011, "The requested transaction is being used concurrently by another request." ),

    STATEMENT_EXECUTION_ERROR(
            42000, "Error when executing statement." ),
    STATEMENT_SYNTAX_ERROR(
            42001, "Syntax error in statement." ),
    STATEMENT_MISSING_PARAMETER_ERROR(
            42002, "Parameter missing in statement." ),

    //
    // 5xxxxx Database errors
    //
    INTERNAL_DATABASE_ERROR(
            50000, "Internal database error. Please refer to the attached stack trace for details.",
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_STATEMENT_EXECUTION_ERROR(
            50001, "Internal error when executing statement.",
            StackTraceStrategy.SEND_TO_CLIENT ),

    INTERNAL_BEGIN_TRANSACTION_ERROR(
            53010, "Unable to start transaction, and unable to determine cause of failure. ",
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_ROLLBACK_TRANSACTION_ERROR(
            53011, "Unable to roll back transaction, and unable to determine cause of failure. ",
            StackTraceStrategy.SEND_TO_CLIENT ),
    INTERNAL_COMMIT_TRANSACTION_ERROR(
            53012, "It was not possible to commit your transaction. ",
            StackTraceStrategy.SEND_TO_CLIENT );

    private final int code;
    private final String defaultMessage;
    private final StackTraceStrategy stackTraceStrategy;

    enum StackTraceStrategy
    {
        SWALLOW, SEND_TO_CLIENT
    }

    StatusCode( int code, String defaultMessage )
    {
        this (code, defaultMessage, StackTraceStrategy.SWALLOW );
    }

    StatusCode( int code, String defaultMessage, StackTraceStrategy stackTraceStrategy )
    {
        this.code = code;
        this.defaultMessage = defaultMessage;
        this.stackTraceStrategy = stackTraceStrategy;
    }

    public int getCode()
    {
        return code;
    }

    public String getDefaultMessage()
    {
        return defaultMessage;
    }

    public boolean includeStackTrace()
    {
        return StackTraceStrategy.SEND_TO_CLIENT == stackTraceStrategy;
    }
}
