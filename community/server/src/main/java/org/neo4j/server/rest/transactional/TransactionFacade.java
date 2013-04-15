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

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.Neo4jError;

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
public class TransactionFacade
{
    private final KernelAPI kernel;
    private final ExecutionEngine engine;
    private final TransactionRegistry registry;
    private final StringLogger log;

    /**
     * In order to support streaming results back to the user, while at the same time ensuring proper closing of
     * resources, no public method (other than {@link #newTransactionHandle()}, which we may want to move somewhere
     * else)
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
        void prologue();

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

    public TransactionFacade( TransitionalPeriodTransactionMessContainer mess, ExecutionEngine engine,
                              TransactionRegistry registry, StringLogger log )
    {
        this.kernel = mess;
        this.engine = engine;
        this.registry = registry;
        this.log = log;
    }

    public TransactionHandle newTransactionHandle() throws Neo4jError
    {
        return new TransactionHandle( kernel, engine, registry, log );
    }

    public TransactionHandle findTransactionHandle( long txId ) throws Neo4jError
    {
        return registry.acquire( txId );
    }

}
