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
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.MissingParameterError;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.UnableToStartTransactionError;
import org.neo4j.server.rest.transactional.error.UnknownCommitError;
import org.neo4j.server.rest.transactional.error.UnknownDatabaseError;
import org.neo4j.server.rest.transactional.error.UnknownRollbackError;
import org.neo4j.server.rest.transactional.error.UnknownStatementError;

/**
 * Transactional actions contains the business logic for executing statements against Neo4j across long-running
 * transactions.
 * <p/>
 * The idiom for the public methods here is:
 * <p/>
 * response.begin()
 * try {
 * // Do internal calls, saving errors into a common error list
 * } catch ( Neo4jError e )
 * {
 * errors.add(e);
 * } finally
 * {
 * response.finish(errors)
 * }
 * <p/>
 * This is done to ensure we stick to the contract of the response handler, which is important, because if we skimp on
 * it, clients may be left waiting for results that never arrive.
 */
public class TransactionalActions
{
    private final KernelAPI kernel;
    private final ExecutionEngine engine;
    private final TransactionRegistry registry;
    private final StringLogger log;

    /**
     * In order to support streaming results back to the user, while at the same time ensuring proper closing of
     * resources, no public method (other than {@link #newTransaction()}, which we may want to move somewhere else)
     * in this implementation returns a result. Instead, results are reported to a handler that you pass in, with the
     * guarantee that when the method returns, all results have been reported to the handler.
     */
    public static interface ResultHandler
    {
        /**
         * Will always get called once, and is always the first method to get called. This method is not allowed
         * to throw exceptions. If there are network errors or similar, the handler should take appropriate action,
         * but never fail this method.
         */
        void prologue( long txId );

        /**
         * Will always get called once, and is always the first method to get called. This method is not allowed
         * to throw exceptions. If there are network errors or similar, the handler should take appropriate action,
         * but never fail this method.
         */
        void prologue( );

        /**
         * Will get called at most once per statement. This method is *only* allowed to throw ActionFailedException,
         * throwing anything but that may lead to resource leakage.
         */
        void visitStatementResult( ExecutionResult result ) throws Neo4jError;

        /**
         * Will always get called once, and will always be the last method to get called. This method is not allowed
         * to throw exceptions. If there are network errors or similar, the handler should take appropriate action,
         * but never fail this method.
         */
        void epilogue( Iterator<Neo4jError> errors );
    }

    public TransactionalActions( TransitionalPeriodTransactionMessContainer mess, ExecutionEngine engine,
                                 TransactionRegistry registry, StringLogger log )
    {
        this.kernel = mess;
        this.engine = engine;
        this.registry = registry;
        this.log = log;
    }

    public Transaction newTransaction() throws Neo4jError
    {
        TransactionContext transactionContext = newTransactionContext();
        return new Transaction( registry.begin(), transactionContext );
    }

    public Transaction findTransaction( long txId ) throws Neo4jError
    {
        return new Transaction( txId, registry.resume( txId ) );
    }

    public class Transaction
    {
        private final long id;
        private final TransactionContext context;

        public Transaction( long id, TransactionContext context ) throws Neo4jError
        {
            this.id = id;
            this.context = context;
        }

        public long getId()
        {
            return id;
        }

        public void execute( StatementDeserializer statements, ResultHandler results )
        {
            List<Neo4jError> errors = new LinkedList<Neo4jError>();
            try
            {
                results.prologue( id );
                execute( context, statements, results, errors );
            }
            finally
            {
                results.epilogue( errors.iterator() );
            }
        }

        public void commit( StatementDeserializer statements, ResultHandler results )
        {
            List<Neo4jError> errors = new LinkedList<Neo4jError>();
            try
            {
                results.prologue();
                commit( context, statements, results, errors );
            }
            finally
            {
                results.epilogue( errors.iterator() );
            }
        }

        public void rollback( ResultHandler results )
        {
            List<Neo4jError> errors = new LinkedList<Neo4jError>();
            try
            {
                results.prologue();
                rollback( context, errors );
            }
            finally
            {
                results.epilogue( errors.iterator() );
            }
        }

        private void execute( TransactionContext ctx, StatementDeserializer statements, ResultHandler results,
                              List<Neo4jError> errors )
        {
            executeStatements( ctx, statements, results, errors );

            if ( errors.isEmpty() )
            {
                registry.suspend( id, ctx );
            }
            else
            {
                rollback( ctx, errors );
            }
        }

        private void commit( TransactionContext ctx, StatementDeserializer statements, ResultHandler results,
                             List<Neo4jError> errors )
        {
            try
            {
                executeStatements( ctx, statements, results, errors );

                if ( errors.isEmpty() )
                {
                    try
                    {
                        ctx.commit();
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
                        ctx.rollback();
                    }
                    catch ( RuntimeException e )
                    {
                        errors.add( new UnknownDatabaseError( e ) );
                    }
                }
            }
            finally
            {
                registry.finish( id );
            }
        }

        private void rollback( TransactionContext ctx, List<Neo4jError> errors )
        {
            try
            {
                ctx.rollback();
            }
            catch ( RuntimeException e )
            {
                log.error( "Failed to rollback transaction.", e );
                errors.add( new UnknownRollbackError( e ) );
            }
            finally
            {
                registry.finish( id );
            }
        }

        private void executeStatements( TransactionContext tx, StatementDeserializer statements, ResultHandler results,
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

    //
    // -- Internals
    //

    private TransactionContext newTransactionContext() throws Neo4jError
    {
        try
        {
            return kernel.newTransactionContext();
        }
        catch ( RuntimeException e )
        {
            log.error( "Failed to start transaction.", e );
            throw new UnableToStartTransactionError( e );
        }
    }


}
