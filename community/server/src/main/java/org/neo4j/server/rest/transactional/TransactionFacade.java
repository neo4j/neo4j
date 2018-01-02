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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.transactional.error.TransactionLifecycleException;
import org.neo4j.server.rest.web.QuerySessionProvider;
import org.neo4j.server.rest.web.TransactionUriScheme;

/**
 * Transactional actions contains the business logic for executing statements against Neo4j across long-running
 * transactions.
 * <p>
 * The idiom for the public methods here is:
 * <p>
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
 * <p>
 * This is done to ensure we stick to the contract of the response handler, which is important, because if we skimp on
 * it, clients may be left waiting for results that never arrive.
 */
public class TransactionFacade
{
    private final TransitionalPeriodTransactionMessContainer kernel;
    private final QueryExecutionEngine engine;
    private final TransactionRegistry registry;
    private final LogProvider logProvider;

    public TransactionFacade( TransitionalPeriodTransactionMessContainer kernel, QueryExecutionEngine engine,
                              TransactionRegistry registry, LogProvider logProvider )
    {
        this.kernel = kernel;
        this.engine = engine;
        this.registry = registry;
        this.logProvider = logProvider;
    }

    public TransactionHandle newTransactionHandle( TransactionUriScheme uriScheme ) throws TransactionLifecycleException
    {
        return new TransactionHandle( kernel, engine, registry, uriScheme, logProvider, QuerySessionProvider.provider );
    }

    public TransactionHandle findTransactionHandle( long txId ) throws TransactionLifecycleException
    {
        return registry.acquire( txId );
    }

    public TransactionHandle terminate( long txId ) throws TransactionLifecycleException
    {
        return registry.terminate( txId );
    }

    public StatementDeserializer deserializer( InputStream input )
    {
        return new StatementDeserializer( input );
    }

    public ExecutionResultSerializer serializer( OutputStream output, URI baseUri )
    {
        return new ExecutionResultSerializer( output, baseUri, logProvider );
    }
}
