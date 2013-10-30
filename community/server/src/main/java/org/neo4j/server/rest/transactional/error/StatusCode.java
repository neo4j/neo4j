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

import static java.lang.String.format;
import static org.neo4j.server.rest.transactional.error.StatusCode.Classification.*;

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
    //
    // Communication protocol errors
    //
    NETWORK_ERROR( TransientError, "Network", "UnknownFailure" ),

    //
    // User errors
    //
    INVALID_REQUEST( ClientError, "Request", "Invalid" ),
    INVALID_REQUEST_FORMAT( ClientError, "Request", "InvalidFormat" ),

    INVALID_TRANSACTION_ID( ClientError, "Transaction", "UnknownId" ),
    INVALID_CONCURRENT_TRANSACTION_ACCESS( ClientError, "Transaction", "ConcurrentRequest" ),

    STATEMENT_EXECUTION_ERROR( ClientError, "Statement", "ExecutionFailure" ),
    STATEMENT_SYNTAX_ERROR( ClientError, "Statement", "InvalidSyntax" ),
    STATEMENT_MISSING_PARAMETER( ClientError, "Statement", "ParameterMissing" ),

    //
    // Database errors
    //
    INTERNAL_DATABASE_ERROR( DatabaseError, "General", "Unknown" ),
    INTERNAL_STATEMENT_EXECUTION_ERROR( DatabaseError, "Statement", "ExecutionFailure" ),

    INTERNAL_BEGIN_TRANSACTION_ERROR( DatabaseError, "Transaction", "CouldNotBegin" ),
    INTERNAL_ROLLBACK_TRANSACTION_ERROR( DatabaseError, "Transaction", "CouldNotRollback" ),
    INTERNAL_COMMIT_TRANSACTION_ERROR( DatabaseError, "Transaction", "CouldNotCommit" ),
    ;

    public final Classification classification;
    private final String code;

    public enum Classification
    {
        /**
         * The Client sent a bad request - changing the request might yield a successful outcome.
         */
        ClientError( StackTraceStrategy.SWALLOW, TransactionEffect.NONE ),
        /**
         * The database failed to service the request.
         */
        DatabaseError( StackTraceStrategy.SEND_TO_CLIENT, TransactionEffect.ROLLBACK ),
        /**
         * The database cannot service the request right now, retrying later might yield a successful outcome.
         */
        TransientError( StackTraceStrategy.SEND_TO_CLIENT, TransactionEffect.NONE ),
        ;
        final boolean includeStackTrace;
        final boolean rollbackTransaction; // TODO: make use of this!!!

        private Classification( StackTraceStrategy stackTraceStrategy, TransactionEffect transactionEffect )
        {
            this.includeStackTrace = stackTraceStrategy == StackTraceStrategy.SEND_TO_CLIENT;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }
    }

    private enum StackTraceStrategy
    {
        SWALLOW, SEND_TO_CLIENT,
    }

    private enum TransactionEffect
    {
        ROLLBACK, NONE,
    }

    StatusCode( Classification classification, String category, String subcategory )
    {
        this.classification = classification;
        this.code = format( "Neo.%s.%s.%s", classification, category, subcategory );
    }

    public String getCode()
    {
        return code;
    }

    public boolean includeStackTrace()
    {
        return classification.includeStackTrace;
    }
}
