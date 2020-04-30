/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.executor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.stream.InputDataStreamImpl;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Rx2SyncStream;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class FabricKernelTransaction
{
    private final ExecutingQuery parentQuery;
    private final ExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final InternalTransaction internalTransaction;
    private final FabricConfig config;
    private final Set<TransactionalContext> openExecutionContexts = ConcurrentHashMap.newKeySet();

    FabricKernelTransaction( ExecutingQuery parentQuery, ExecutionEngine queryExecutionEngine, TransactionalContextFactory transactionalContextFactory,
            InternalTransaction internalTransaction, FabricConfig config )
    {
        this.parentQuery = parentQuery;
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionalContextFactory = transactionalContextFactory;
        this.internalTransaction = internalTransaction;
        this.config = config;
    }

    public StatementResult run( FullyParsedQuery query, MapValue params, Flux<Record> input )
    {
        String queryText = "Internal query for Fabric query id:" + parentQuery.id();
        var executionContext = transactionalContextFactory.newContext( internalTransaction, queryText, params );
        openExecutionContexts.add( executionContext );
        //Query is a sub-part of the parent fabric query that is already parsed and planned. The parent fabric query is monitored by the fabric executor.
        var result = StatementResults.create( subscriber -> execute( query, params, executionContext, convert( input ), subscriber ) );
        return new ContextClosingResultInterceptor( result, executionContext );
    }

    private QueryExecution execute( FullyParsedQuery query, MapValue params, TransactionalContext executionContext, InputDataStream input,
            QuerySubscriber subscriber )
    {
        try
        {
            return queryExecutionEngine.executeQuery( query, params, executionContext, true, input, subscriber );
        }
        catch ( QueryExecutionKernelException e )
        {
            // all exception thrown from execution engine are wrapped in QueryExecutionKernelException,
            // let's see if there is something better hidden in it
            if ( e.getCause() == null )
            {
                throw Exceptions.transform( Status.Statement.ExecutionFailed, e );
            }
            else
            {
                throw Exceptions.transform( Status.Statement.ExecutionFailed, e.getCause() );
            }
        }
    }

    private InputDataStream convert( Flux<Record> input )
    {
        return new InputDataStreamImpl( new Rx2SyncStream( input, config.getDataStream().getBatchSize() )
        );
    }

    public void commit()
    {
        synchronized ( internalTransaction )
        {
            if ( internalTransaction.isOpen() )
            {
                closeContexts();
                internalTransaction.commit();
            }
        }
    }

    public void rollback()
    {
        synchronized ( internalTransaction )
        {
            if ( internalTransaction.isOpen() )
            {
                closeContexts();
                internalTransaction.rollback();
            }
        }
    }

    private void closeContexts()
    {
        openExecutionContexts.forEach( TransactionalContext::close );
    }

    public void terminate()
    {
        internalTransaction.terminate();
    }

    /**
     * This is a hack to be able to get an InternalTransaction for the TestFabricTransaction tx wrapper
     */
    @Deprecated
    public InternalTransaction getInternalTransaction()
    {
        return internalTransaction;
    }

    private class ContextClosingResultInterceptor implements StatementResult
    {
        private final StatementResult wrappedResult;
        private final TransactionalContext executionContext;

        ContextClosingResultInterceptor( StatementResult wrappedResult, TransactionalContext executionContext )
        {
            this.wrappedResult = wrappedResult;
            this.executionContext = executionContext;
        }

        @Override
        public Flux<String> columns()
        {
            return wrappedResult.columns();
        }

        @Override
        public Flux<Record> records()
        {
            // We care only about the case when the statement completes successfully.
            // All contexts will be closed upon a failure in the rollback
            return wrappedResult.records().doOnComplete( () ->
            {
                openExecutionContexts.remove( executionContext );
                executionContext.close();
            } );
        }

        @Override
        public Mono<Summary> summary()
        {
            return wrappedResult.summary();
        }
    }
}
