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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.transactional.error.InternalBeginTransactionError;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.QuerySessionProvider;
import org.neo4j.server.rest.web.TransactionUriScheme;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.server.rest.transactional.TransactionHandle.StatementExecutionStrategy.EXECUTE_STATEMENT;
import static org.neo4j.server.rest.transactional.TransactionHandle.StatementExecutionStrategy.EXECUTE_STATEMENT_USING_PERIODIC_COMMIT;
import static org.neo4j.server.rest.transactional.TransactionHandle.StatementExecutionStrategy.SKIP_EXECUTE_STATEMENT;

/**
 * Encapsulates executing statements in a transaction, committing the transaction, or rolling it back.
 *
 * Constructing a {@link TransactionHandle} does not immediately ask the kernel to create a
 * {@link org.neo4j.kernel.api.KernelTransaction}; instead a {@link org.neo4j.kernel.api.KernelTransaction} is
 * only created when the first statements need to be executed.
 *
 * At the end of each statement-executing method, the {@link org.neo4j.kernel.api.KernelTransaction} is either
 * suspended (ready to be resumed by a later operation), or committed, or rolled back.
 *
 * If you acquire instances of this class from {@link TransactionHandleRegistry}, it will prevent concurrent access to
 * the same instance. Therefore the implementation assumes that a single instance will only be accessed from
 * a single thread.
 *
 * All of the public methods on this class are "single-shot"; once you have called one method, the handle returns itself
 * to the registry. If you want to use it again, you'll need to acquire it back from the registry to ensure exclusive use.
 */
public class TransactionHandle implements TransactionTerminationHandle
{
    private final TransitionalPeriodTransactionMessContainer txManagerFacade;
    private final QueryExecutionEngine engine;
    private final TransactionRegistry registry;
    private final TransactionUriScheme uriScheme;
    private final Log log;
    private final long id;
    private final QuerySessionProvider sessionFactory;
    private TransitionalTxManagementKernelTransaction context;

    public TransactionHandle( TransitionalPeriodTransactionMessContainer txManagerFacade, QueryExecutionEngine engine,
                              TransactionRegistry registry, TransactionUriScheme uriScheme, LogProvider logProvider,
                              QuerySessionProvider sessionFactory )
    {
        this.txManagerFacade = txManagerFacade;
        this.engine = engine;
        this.registry = registry;
        this.uriScheme = uriScheme;
        this.log = logProvider.getLog( getClass() );
        this.id = registry.begin( this );
        this.sessionFactory = sessionFactory;
    }

    public URI uri()
    {
        return uriScheme.txUri( id );
    }

    public void execute( StatementDeserializer statements, ExecutionResultSerializer output, HttpServletRequest request )
    {
        List<Neo4jError> errors = new LinkedList<>();
        try
        {
            output.transactionCommitUri( uriScheme.txCommitUri( id ) );
            ensureActiveTransaction();
            execute( statements, output, errors, request );
        }
        catch ( InternalBeginTransactionError e )
        {
            errors.add( e.toNeo4jError() );
        }
        finally
        {
            output.errors( errors );
            output.finish();
        }
    }

    @Override
    public boolean terminate()
    {
        if ( context != null )
        {
            context.terminate();
        }
        return true;
    }

    public void commit( StatementDeserializer statements, ExecutionResultSerializer output, boolean pristine, HttpServletRequest request )
    {
        List<Neo4jError> errors = new LinkedList<>();
        try
        {
            try
            {
                StatementExecutionStrategy executionStrategy = selectExecutionStrategy( statements, pristine, errors );

                switch ( executionStrategy ) {
                    case EXECUTE_STATEMENT_USING_PERIODIC_COMMIT:
                        // If there is an open transaction at this point this will cause an immediate error
                        // as soon as Cypher tries to execute the initial PERIODIC COMMIT statement
                        executePeriodicCommitStatement(statements, output, errors, request);
                        break;

                    case EXECUTE_STATEMENT:
                        ensureActiveTransaction();
                        // If any later statement is an PERIODIC COMMIT query, executeStatements will fail
                        // as Cypher does refuse to execute PERIODIC COMMIT queries in an open transaction
                        executeStatements( statements, output, errors, request );
                        closeContextAndCollectErrors( errors );
                        break;

                    case SKIP_EXECUTE_STATEMENT:
                        addToCollection( statements.errors(), errors );
                        break;
                }
            }
            finally
            {
                registry.forget( id );
            }
        }
        catch ( InternalBeginTransactionError e )
        {
            errors.add( e.toNeo4jError() );
        }
        finally
        {
            output.errors( errors );
            output.finish();
        }
    }

    private StatementExecutionStrategy selectExecutionStrategy( StatementDeserializer statements, boolean pristine, List<Neo4jError> errors )
    {
        // PERIODIC COMMIT queries may only be used when directly committing a pristine (newly created)
        // transaction and when the first statement is an PERIODIC COMMIT statement.
        //
        // In that case we refrain from opening a transaction and leave management of
        // transactions to Cypher. If there are any further statements they will all be
        // executed in a separate transaction (Once you PERIODIC COMMIT all bets are off).
        //
        try
        {
            if ( pristine )
            {
                Statement peek = statements.peek();
                if ( peek == null ) /* JSON parse error */
                {
                    return SKIP_EXECUTE_STATEMENT;
                }
                else if ( engine.isPeriodicCommit( peek.statement() ) )
                {
                    return EXECUTE_STATEMENT_USING_PERIODIC_COMMIT;
                }
                else
                {
                    return EXECUTE_STATEMENT;
                }
            }
            else
            {
                return EXECUTE_STATEMENT;
            }
        }
        catch ( CypherException e )
        {
            errors.add( new Neo4jError( e.status(), e ) );
            throw e;
        }
    }

    public void rollback( ExecutionResultSerializer output )
    {
        List<Neo4jError> errors = new LinkedList<>();
        try
        {
            ensureActiveTransaction();
            rollback( errors );
        }
        catch ( InternalBeginTransactionError e )
        {
            errors.add( e.toNeo4jError() );
        }
        finally
        {
            output.errors( errors );
            output.finish();
        }
    }

    public void forceRollback() throws TransactionFailureException
    {
        context.resumeSinceTransactionsAreStillThreadBound();
        context.rollback();
    }

    private void ensureActiveTransaction() throws InternalBeginTransactionError
    {
        if ( context == null )
        {
            try
            {
                context = txManagerFacade.newTransaction();
            }
            catch ( RuntimeException e )
            {
                log.error( "Failed to start transaction.", e );
                throw new InternalBeginTransactionError( e );
            }
        }
        else
        {
            context.resumeSinceTransactionsAreStillThreadBound();
        }
    }

    private void execute( StatementDeserializer statements, ExecutionResultSerializer output,
                          List<Neo4jError> errors, HttpServletRequest request )
    {
        executeStatements( statements, output, errors, request );

        if ( Neo4jError.shouldRollBackOn( errors ) )
        {
            rollback( errors );
        }
        else
        {
            context.suspendSinceTransactionsAreStillThreadBound();
            long lastActiveTimestamp = registry.release( id, this );
            output.transactionStatus( lastActiveTimestamp );
        }
    }

    private void closeContextAndCollectErrors( List<Neo4jError> errors )
    {
        if ( errors.isEmpty() )
        {
            try
            {
                context.commit();
            }
            catch ( Exception e )
            {
                log.error( "Failed to commit transaction.", e );
                errors.add( new Neo4jError( Status.Transaction.CouldNotCommit, e ) );
            }
        }
        else
        {
            try
            {
                context.rollback();
            }
            catch ( Exception e )
            {
                log.error( "Failed to rollback transaction.", e );
                errors.add( new Neo4jError( Status.Transaction.CouldNotRollback, e ) );
            }
        }
    }

    private void rollback( List<Neo4jError> errors )
    {
        try
        {
            context.rollback();
        }
        catch ( Exception e )
        {
            log.error( "Failed to rollback transaction.", e );
            errors.add( new Neo4jError( Status.Transaction.CouldNotRollback, e ) );
        }
        finally
        {
            registry.forget( id );
        }
    }

    private void executeStatements( StatementDeserializer statements, ExecutionResultSerializer output,
                                    List<Neo4jError> errors, HttpServletRequest request )
    {
        try
        {
            while ( statements.hasNext() )
            {
                Statement statement = statements.next();
                try
                {
                    Result result = engine.executeQuery( statement.statement(), statement.parameters(),
                            sessionFactory.create( request ) );
                    output.statementResult( result, statement.includeStats(), statement.resultDataContents() );
                    output.notifications( result.getNotifications() );
                }
                catch ( KernelException | CypherException e )
                {
                    errors.add( new Neo4jError( e.status(), e ) );
                    break;
                }
                catch ( QueryExecutionException e )
                {
                    if ( e.getCause() instanceof Status.HasStatus )
                    {
                        errors.add( new Neo4jError( ((Status.HasStatus) e.getCause()).status(), e ) );
                    }
                    else
                    {
                        errors.add( new Neo4jError( Status.Statement.ExecutionFailure, e ) );
                    }
                    break;
                }
                catch( DeadlockDetectedException e )
                {
                    errors.add( new Neo4jError( Status.Transaction.DeadlockDetected, e ));
                }
                catch ( IOException e )
                {
                    errors.add( new Neo4jError( Status.Network.UnknownFailure, e ) );
                    break;
                }
                catch ( Exception e )
                {
                    errors.add( new Neo4jError( Status.Statement.ExecutionFailure, e ) );
                    break;
                }
            }

            addToCollection( statements.errors(), errors );
        }
        catch ( Throwable e )
        {
            errors.add( new Neo4jError( Status.General.UnknownFailure, e ) );
        }
    }


    private void executePeriodicCommitStatement(
           StatementDeserializer statements, ExecutionResultSerializer output, List<Neo4jError> errors, HttpServletRequest request )
    {
        try
        {
            try
            {
                Statement statement = statements.next();
                if ( statements.hasNext() )
                {
                    throw new QueryExecutionKernelException(
                            new InvalidSemanticsException( "Cannot execute another statement after executing " +
                                                           "PERIODIC COMMIT statement in the same transaction" ) );
                }

                Result result = engine.executeQuery( statement.statement(), statement.parameters(), sessionFactory
                        .create(request) );
                ensureActiveTransaction();
                output.statementResult( result, statement.includeStats(), statement.resultDataContents() );
                output.notifications( result.getNotifications() );
                closeContextAndCollectErrors(errors);
            }
            catch ( KernelException | CypherException e )
            {
                errors.add( new Neo4jError( e.status(), e ) );
            }
            catch( DeadlockDetectedException e )
            {
                errors.add( new Neo4jError( Status.Transaction.DeadlockDetected, e ));
            }
            catch ( IOException e )
            {
                errors.add( new Neo4jError( Status.Network.UnknownFailure, e ) );
            }
            catch ( Exception e )
            {
                errors.add( new Neo4jError( Status.Statement.ExecutionFailure, e ) );
            }

            addToCollection( statements.errors(), errors );
        }
        catch ( Throwable e )
        {
            errors.add( new Neo4jError( Status.General.UnknownFailure, e ) );
        }
    }

    enum StatementExecutionStrategy
    {
        EXECUTE_STATEMENT_USING_PERIODIC_COMMIT,
        EXECUTE_STATEMENT,
        SKIP_EXECUTE_STATEMENT
    }
}
