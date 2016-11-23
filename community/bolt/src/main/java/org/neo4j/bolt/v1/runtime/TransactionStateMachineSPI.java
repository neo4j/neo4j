/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.v1.runtime.TransactionStateMachine.BoltResultHandle;
import org.neo4j.bolt.v1.runtime.cypher.CypherAdapterStream;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySource;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.kernel.api.KernelTransaction.Type.implicit;

class TransactionStateMachineSPI implements TransactionStateMachine.SPI
{
    private final GraphDatabaseAPI db;
    private final ThreadToStatementContextBridge txBridge;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionIdTracker transactionIdTracker;
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private final TransactionalContextFactory contextFactory;
    private final GraphDatabaseQueryService queryService;
    private final Clock clock;

    TransactionStateMachineSPI( GraphDatabaseAPI db,
                                ThreadToStatementContextBridge txBridge,
                                QueryExecutionEngine queryExecutionEngine,
                                TransactionIdStore transactionIdStoreSupplier,
                                AvailabilityGuard availabilityGuard,
                                GraphDatabaseQueryService queryService,
                                Clock clock )
    {
        this.db = db;
        this.txBridge = txBridge;
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionIdTracker = new TransactionIdTracker( transactionIdStoreSupplier, availabilityGuard );
        this.contextFactory = Neo4jTransactionalContextFactory.create( queryService, locker );
        this.queryService = queryService;
        this.clock = clock;
    }

    @Override
    public void awaitUpToDate( long oldestAcceptableTxId, Duration timeout ) throws TransactionFailureException
    {
        transactionIdTracker.awaitUpToDate( oldestAcceptableTxId, timeout );
    }

    @Override
    public long newestEncounteredTxId()
    {
        return transactionIdTracker.newestEncounteredTxId();
    }

    @Override
    public KernelTransaction beginTransaction( SecurityContext securityContext )
    {
        db.beginTransaction( KernelTransaction.Type.explicit, securityContext );
        return txBridge.getKernelTransactionBoundToThisThread( false );
    }

    @Override
    public void bindTransactionToCurrentThread( KernelTransaction tx )
    {
        txBridge.bindTransactionToCurrentThread( tx );
    }

    @Override
    public void unbindTransactionFromCurrentThread()
    {
        txBridge.unbindTransactionFromCurrentThread();
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return queryExecutionEngine.isPeriodicCommit( query );
    }

    @Override
    public BoltResultHandle executeQuery( String querySource,
            SecurityContext securityContext,
            String statement,
            Map<String,Object> params, ThrowingAction<KernelException> onFail ) throws QueryExecutionKernelException
    {
        InternalTransaction internalTransaction = queryService.beginTransaction( implicit, securityContext );
        QuerySource sourceDetails = new QuerySource( "bolt-session", querySource );
        TransactionalContext transactionalContext =
                contextFactory.newContext( sourceDetails, internalTransaction, statement, params );

        return new BoltResultHandle()
        {
            @Override
            public BoltResult start() throws KernelException
            {
                try
                {
                    Result run = queryExecutionEngine.executeQuery( statement, params, transactionalContext );
                    return new CypherAdapterStream( run, clock );
                }
                catch ( KernelException e )
                {
                    onFail.apply();
                    throw new QueryExecutionKernelException( e );
                }
                catch ( Throwable e )
                {
                    onFail.apply();
                    throw e;
                }
            }

            @Override
            public void terminate()
            {
                transactionalContext.terminate();
            }
        };
    }
}
