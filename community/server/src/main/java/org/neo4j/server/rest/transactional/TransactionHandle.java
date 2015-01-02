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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.InternalBeginTransactionError;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.TransactionUriScheme;

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
public class TransactionHandle
{
    private final TransitionalPeriodTransactionMessContainer txManagerFacade;
    private final ExecutionEngine engine;
    private final TransactionRegistry registry;
    private final TransactionUriScheme uriScheme;
    private final StringLogger log;
    private final long id;
    private TransitionalTxManagementKernelTransaction context;

    public TransactionHandle( TransitionalPeriodTransactionMessContainer txManagerFacade, ExecutionEngine engine,
                              TransactionRegistry registry, TransactionUriScheme uriScheme, StringLogger log )
    {
        this.txManagerFacade = txManagerFacade;
        this.engine = engine;
        this.registry = registry;
        this.uriScheme = uriScheme;
        this.log = log;
        this.id = registry.begin();
    }

    public URI uri()
    {
        return uriScheme.txUri( id );
    }

    public void execute( StatementDeserializer statements, ExecutionResultSerializer output )
    {
        List<Neo4jError> errors = new LinkedList<>();
        try
        {
            output.transactionCommitUri( uriScheme.txCommitUri( id ) );
            ensureActiveTransaction();
            execute( statements, output, errors );
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

    public void commit( StatementDeserializer statements, ExecutionResultSerializer output )
    {
        List<Neo4jError> errors = new LinkedList<>();
        try
        {
            ensureActiveTransaction();
            commit( statements, output, errors );
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
                          List<Neo4jError> errors )
    {
        executeStatements( statements, output, errors );

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

    private void commit( StatementDeserializer statements, ExecutionResultSerializer output,
                         List<Neo4jError> errors )
    {
        try
        {
            executeStatements( statements, output, errors );

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
        finally
        {
            registry.forget( id );
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
                                    List<Neo4jError> errors )
    {
        try
        {
            while ( statements.hasNext() )
            {
                Statement statement = statements.next();
                ExecutionResult result;
                try
                {
                    result = engine.execute( statement.statement(), statement.parameters() );
                    output.statementResult( result, statement.includeStats(), statement.resultDataContents() );
                }
                catch ( CypherException e )
                {
                    errors.add( new Neo4jError( e.status(), e ) );
                    break;
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

            Iterator<Neo4jError> deserializationErrors = statements.errors();
            while ( deserializationErrors.hasNext() )
            {
                errors.add( deserializationErrors.next() );
            }
        }
        catch ( Exception e )
        {
            errors.add( new Neo4jError( Status.General.UnknownFailure, e ) );
        }
    }
}
