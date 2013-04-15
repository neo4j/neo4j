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
package org.neo4j.server.rest.transactional;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cypher.ParameterNotFoundException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.MissingParameterError;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.UnableToStartTransactionError;
import org.neo4j.server.rest.transactional.error.UnknownCommitError;
import org.neo4j.server.rest.transactional.error.UnknownDatabaseError;
import org.neo4j.server.rest.transactional.error.UnknownRollbackError;
import org.neo4j.server.rest.transactional.error.UnknownStatementError;

/**
 * Encapsulates executing statements in a transaction, committing the transaction, or rolling it back.
 *
 * Constructing a {@link TransactionHandle} does not immediately ask the kernel to create a
 * {@link org.neo4j.kernel.api.TransactionContext}; instead a {@link org.neo4j.kernel.api.TransactionContext} is
 * only created when the first statements need to be executed.
 *
 * At the end of each statement-executing method, the {@link org.neo4j.kernel.api.TransactionContext} is either
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
    private final KernelAPI kernel;
    private final ExecutionEngine engine;
    private final TransactionRegistry registry;
    private final StringLogger log;
    private final long id;
    private TransitionalTxManagementTransactionContext context;

    public TransactionHandle( KernelAPI kernel, ExecutionEngine engine, TransactionRegistry registry, StringLogger log )
    {
        this.kernel = kernel;
        this.engine = engine;
        this.registry = registry;
        this.log = log;
        this.id = registry.begin();
    }

    public long getId()
    {
        return id;
    }

    public void execute( StatementDeserializer statements, TransactionFacade.ResultHandler results )
    {
        List<Neo4jError> errors = new LinkedList<Neo4jError>();
        try
        {
            results.prologue( id );
            ensureActiveTransaction();
            execute( statements, results, errors );
        }
        catch ( UnableToStartTransactionError neo4jError )
        {
            errors.add( neo4jError );
        }
        finally
        {
            results.epilogue( errors.iterator() );
        }
    }

    public void commit( StatementDeserializer statements, TransactionFacade.ResultHandler results )
    {
        List<Neo4jError> errors = new LinkedList<Neo4jError>();
        try
        {
            results.prologue();
            ensureActiveTransaction();
            commit( statements, results, errors );
        }
        catch ( UnableToStartTransactionError neo4jError )
        {
            errors.add( neo4jError );
        }
        finally
        {
            results.epilogue( errors.iterator() );
        }
    }

    public void rollback( TransactionFacade.ResultHandler results )
    {
        List<Neo4jError> errors = new LinkedList<Neo4jError>();
        try
        {
            results.prologue();
            ensureActiveTransaction();
            rollback( errors );
        }
        catch ( UnableToStartTransactionError neo4jError )
        {
            errors.add( neo4jError );
        }
        finally
        {
            results.epilogue( errors.iterator() );
        }
    }

    public void forceRollback()
    {
        context.resumeSinceTransactionsAreStillThreadBound();
        context.rollback();
    }

    private void ensureActiveTransaction() throws UnableToStartTransactionError
    {
        if ( context == null )
        {
            try
            {
                context = (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();
            }
            catch ( RuntimeException e )
            {
                log.error( "Failed to start transaction.", e );
                throw new UnableToStartTransactionError( e );
            }
        }
        else
        {
            context.resumeSinceTransactionsAreStillThreadBound();
        }
    }

    private void execute( StatementDeserializer statements, TransactionFacade.ResultHandler results,
                          List<Neo4jError> errors )
    {
        executeStatements( statements, results, errors );

        if ( errors.isEmpty() )
        {
            context.suspendSinceTransactionsAreStillThreadBound();
            registry.release( id, this );
        }
        else
        {
            rollback( errors );
        }
    }

    private void commit( StatementDeserializer statements, TransactionFacade.ResultHandler results,
                         List<Neo4jError> errors )
    {
        try
        {
            executeStatements( statements, results, errors );

            if ( errors.isEmpty() )
            {
                try
                {
                    context.commit();
                }
                catch ( RuntimeException e )
                {
                    log.error( "Failed to commit transaction.", e );
                    errors.add( new UnknownCommitError( e ) );
                }
            }
            else
            {
                try
                {
                    context.rollback();
                }
                catch ( RuntimeException e )
                {
                    log.error( "Failed to rollback transaction.", e );
                    errors.add( new UnknownRollbackError( e ) );
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
        catch ( RuntimeException e )
        {
            log.error( "Failed to rollback transaction.", e );
            errors.add( new UnknownRollbackError( e ) );
        }
        finally
        {
            registry.forget( id );
        }
    }

    private void executeStatements( StatementDeserializer statements, TransactionFacade.ResultHandler results,
                                    List<Neo4jError> errors )
    {
        try
        {
            while ( statements.hasNext() )
            {
                Statement statement = statements.next();
                try
                {
                    ExecutionResult result = engine.execute( statement.statement(), statement.parameters() );
                    // NOTE: The TransactionContext et cetera used up until this method, and then blatantly ignored,
                    // is meant to be passed on to a new internal cypher API, like so:

                    // ctx = tx.newStatement()
                    // cypher.execute( ctx, statement, resultVisitor );
                    // ctx.close()
                    results.visitStatementResult( result );
                }
                catch ( ParameterNotFoundException e )
                {
                    errors.add( new MissingParameterError( e.getMessage(), e ) );
                    break;
                }
                catch ( RuntimeException e )
                {
                    errors.add( new UnknownStatementError( statement.statement(), e ) );
                    break;
                }
                catch ( Neo4jError e )
                {
                    errors.add( e );
                    break;
                }
            }

            Iterator<Neo4jError> deserializationErrors = statements.errors();
            while ( deserializationErrors.hasNext() )
            {
                errors.add( deserializationErrors.next() );
            }
        }
        catch ( RuntimeException e )
        {
            errors.add( new UnknownDatabaseError( e ) );
        }
    }

}
