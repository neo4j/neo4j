/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.http.cypher;

import java.net.URI;

import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.logging.Log;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.api.OutputFormatException;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.rest.Neo4jError;

/**
 * A representation of a typical Cypher endpoint invocation that executed submitted statements and produces response body.
 * <p>
 * Each invocation represented by this class has the following logical structure:
 * <ul>
 *     <li>Pre-statements transaction logic</li>
 *     <li>Execute statements</li>
 *     <li>Post-statements transaction logic</li>
 *     <li>Send transaction information</li>
 * </ul>
 * <p>
 * The only exception from this pattern is when Pre-statements transaction logic fails. The invocation has the following logical structure in this case:
 * <ul>
 *     <li>Pre-statements transaction logic</li>
 *     <li>Send transaction information</li>
 * </ul>
 * <p>
 * What exactly gets executed in Pre-statements and Post-statements transaction logic phases depends on the endpoint in which context is this invocation used.
 */
class Invocation
{
    private final Log log;
    private final TransactionHandle transactionHandle;
    private final InputEventStream inputEventStream;
    private final boolean finishWithCommit;
    private final URI commitUri;

    private OutputEventStream outputEventStream;
    private boolean hasPrevious;
    private Neo4jError neo4jError;
    private RuntimeException outputError;
    private TransactionNotificationState transactionNotificationState = TransactionNotificationState.NO_TRANSACTION;

    Invocation( Log log, TransactionHandle transactionHandle, URI commitUri, InputEventStream inputEventStream, boolean finishWithCommit )
    {
        this.log = log;
        this.transactionHandle = transactionHandle;
        this.commitUri = commitUri;
        this.inputEventStream = inputEventStream;
        this.finishWithCommit = finishWithCommit;
    }

    /**
     * Executes the invocation.
     *
     * @param outputEventStream the output event stream used to produce the output of this invocation.
     */
    void execute( OutputEventStream outputEventStream )
    {
        this.outputEventStream = outputEventStream;
        if ( !executePreStatementsTransactionLogic() )
        {
            // there is no point going on if pre-statement transaction logic failed
            sendTransactionStateInformation();
            return;
        }
        executeStatements();
        executePostStatementsTransactionLogic();
        sendTransactionStateInformation();
        if ( outputError != null )
        {
            throw outputError;
        }
    }

    private boolean executePreStatementsTransactionLogic()
    {
        try
        {
            transactionHandle.ensureActiveTransaction();
            transactionNotificationState = TransactionNotificationState.OPEN;
        }
        catch ( AuthorizationViolationException se )
        {
            handleNeo4jError( se.status(), se );
            return false;
        }
        catch ( Exception e )
        {

            if ( !transactionHandle.hasTransactionContext() )
            {
                log.error( "Failed to start transaction", e );
                handleNeo4jError( Status.Transaction.TransactionStartFailed, e );
            }
            else
            {
                log.error( "Failed to resume transaction", e );
                handleNeo4jError( Status.Transaction.TransactionNotFound, e );
            }

            return false;
        }

        return true;
    }

    private void executePostStatementsTransactionLogic()
    {

        if ( outputError != null && transactionHandle.isImplicit() )
        {
            try
            {
                transactionHandle.rollback();
                transactionNotificationState = TransactionNotificationState.ROLLED_BACK;
            }
            catch ( Exception e )
            {
                log.error( "Failed to Rollback of implicit transaction after output error", e );
                transactionNotificationState = TransactionNotificationState.UNKNOWN;
            }
            return;
        }

        if ( neo4jError != null && neo4jError.status().code().classification().rollbackTransaction() )
        {
            try
            {
                transactionHandle.rollback();
                transactionNotificationState = TransactionNotificationState.ROLLED_BACK;
            }
            catch ( Exception e )
            {
                log.error( "Failed to roll back transaction.", e );
                handleNeo4jError( Status.Transaction.TransactionRollbackFailed, e );
                transactionNotificationState = TransactionNotificationState.UNKNOWN;
            }
            return;
        }

        if ( outputError == null && finishWithCommit )
        {
            try
            {
                transactionHandle.commit();
                transactionNotificationState = TransactionNotificationState.COMMITTED;
            }
            catch ( Exception e )
            {
                if ( e.getCause() instanceof Status.HasStatus )
                {
                    handleNeo4jError( ((Status.HasStatus) e.getCause()).status(), e );
                }
                else
                {
                    log.error( "Failed to commit transaction.", e );
                    handleNeo4jError( Status.Transaction.TransactionCommitFailed, e );
                }

                transactionNotificationState = TransactionNotificationState.UNKNOWN;
            }

            return;
        }

        transactionHandle.suspendTransaction();
    }

    private void executeStatements()
    {
        try
        {
            while ( outputError == null )
            {
                Statement statement = readStatement();
                if ( statement == null )
                {
                    return;
                }

                executeStatement( statement );
            }
        }
        catch ( InputFormatException e )
        {
            handleNeo4jError( Status.Request.InvalidFormat, e );
        }
        catch ( KernelException | Neo4jException | AuthorizationViolationException | WriteOperationsNotAllowedException e )
        {
            handleNeo4jError( e.status(), e );
        }
        catch ( DeadlockDetectedException e )
        {
            handleNeo4jError( Status.Transaction.DeadlockDetected, e );
        }
        catch ( Exception e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof Status.HasStatus )
            {
                handleNeo4jError( ((Status.HasStatus) cause).status(), cause );
            }
            else
            {
                handleNeo4jError( Status.Statement.ExecutionFailed, e );
            }
        }
    }

    private Statement readStatement()
    {
        try
        {
            return inputEventStream.read();
        }
        catch ( ConnectionException e )
        {
            // if input is broken on IO level, we assume the output is broken, too
            handleOutputError( e );
        }

        return null;
    }

    private void executeStatement( Statement statement ) throws Exception
    {
        boolean periodicCommit = transactionHandle.isPeriodicCommit( statement.getStatement() );
        if ( periodicCommit )
        {

            if ( hasPrevious || readStatement() != null )
            {
                throw new QueryExecutionKernelException(
                        new InvalidSemanticsException( "Cannot execute another statement with PERIODIC COMMIT statement in the same transaction", null ) );
            }

            transactionHandle.closeTransactionForPeriodicCommit();
        }

        hasPrevious = true;

        Result result;
        try
        {
            result = transactionHandle.executeStatement( statement, periodicCommit );
        }
        finally
        {
            if ( periodicCommit )
            {
                transactionHandle.reopenAfterPeriodicCommit();
            }
        }

        writeResult( result, statement );
    }

    private void writeResult( Result result, Statement statement )
    {
        try
        {
            outputEventStream.writeStatementStart( statement, result.columns() );

            result.accept( row ->
            {
                outputEventStream.writeRecord( result.columns(), row::get );
                return true;
            } );

            outputEventStream.writeStatementEnd( result.getQueryExecutionType(), result.getQueryStatistics(), result.getExecutionPlanDescription(),
                    result.getNotifications() );
        }
        catch ( ConnectionException | OutputFormatException e )
        {
            handleOutputError( e );
        }
    }

    private void handleOutputError( RuntimeException e )
    {
        if ( outputError != null )
        {
            return;
        }

        outputError = e;
        // since the error cannot be send to the client over the broken output, at least log it
        log.error( "An error has occurred while sending a response", e );
    }

    private void handleNeo4jError( Status status, Throwable cause )
    {
        neo4jError = new Neo4jError( status, cause );

        try
        {
            outputEventStream.writeFailure( status, neo4jError.getMessage() );
        }
        catch ( ConnectionException | OutputFormatException e )
        {
            handleOutputError( e );
        }
    }

    private void sendTransactionStateInformation()
    {
        if ( outputError != null )
        {
            return;
        }

        try
        {
            outputEventStream.writeTransactionInfo( transactionNotificationState, commitUri, transactionHandle.getExpirationTimestamp() );
        }
        catch ( ConnectionException | OutputFormatException e )
        {
            handleOutputError( e );
        }
    }
}
