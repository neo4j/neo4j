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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

public class TransactionStateMachineV1SPI implements TransactionStateMachineSPI
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private final GraphDatabaseAPI db;
    private final BoltChannel boltChannel;
    private final ThreadToStatementContextBridge txBridge;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionIdTracker transactionIdTracker;
    private final TransactionalContextFactory contextFactory;
    private final Duration txAwaitDuration;
    private final Clock clock;

    public TransactionStateMachineV1SPI( GraphDatabaseAPI db, BoltChannel boltChannel, Duration txAwaitDuration, Clock clock )
    {
        this.db = db;
        this.boltChannel = boltChannel;
        this.txBridge = resolveDependency( db, ThreadToStatementContextBridge.class );
        this.queryExecutionEngine = resolveDependency( db, QueryExecutionEngine.class );
        this.transactionIdTracker = newTransactionIdTracker( db );
        this.contextFactory = newTransactionalContextFactory( db );
        this.txAwaitDuration = txAwaitDuration;
        this.clock = clock;
    }

    @Override
    public void awaitUpToDate( long oldestAcceptableTxId ) throws TransactionFailureException
    {
        transactionIdTracker.awaitUpToDate( oldestAcceptableTxId, txAwaitDuration );
    }

    @Override
    public long newestEncounteredTxId()
    {
        return transactionIdTracker.newestEncounteredTxId();
    }

    @Override
    public KernelTransaction beginTransaction( LoginContext loginContext, Duration txTimeout, Map<String,Object> txMetadata )
    {
        beginTransaction( explicit, loginContext, txTimeout, txMetadata );
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
    public BoltResultHandle executeQuery( LoginContext loginContext, String statement, MapValue params, Duration txTimeout,
            Map<String,Object> txMetadata )
    {
        InternalTransaction internalTransaction = beginTransaction( implicit, loginContext, txTimeout, txMetadata );
        TransactionalContext transactionalContext = contextFactory.newContext( boltChannel.info(), internalTransaction, statement, params );
        return newBoltResultHandle( statement, params, transactionalContext );
    }

    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, TransactionalContext transactionalContext )
    {
        return new BoltResultHandleV1( statement, params, transactionalContext );
    }

    private InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, Duration txTimeout, Map<String, Object> txMetadata )
    {
        InternalTransaction tx;
        if ( txTimeout == null )
        {
            tx = db.beginTransaction( type, loginContext );
        }
        else
        {
            tx = db.beginTransaction( type, loginContext, txTimeout.toMillis(), TimeUnit.MILLISECONDS );
        }

        if ( txMetadata != null )
        {
            tx.setMetaData( txMetadata );
        }
        return tx;
    }

    private static TransactionIdTracker newTransactionIdTracker( GraphDatabaseAPI db )
    {
        Supplier<TransactionIdStore> transactionIdStoreSupplier = db.getDependencyResolver().provideDependency( TransactionIdStore.class );
        AvailabilityGuard guard = resolveDependency( db, DatabaseAvailabilityGuard.class );
        return new TransactionIdTracker( transactionIdStoreSupplier, guard );
    }

    private static TransactionalContextFactory newTransactionalContextFactory( GraphDatabaseAPI db )
    {
        GraphDatabaseQueryService queryService = resolveDependency( db, GraphDatabaseQueryService.class );
        return Neo4jTransactionalContextFactory.create( queryService, locker );
    }

    private static <T> T resolveDependency( GraphDatabaseAPI db, Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }

    public class BoltResultHandleV1 implements BoltResultHandle
    {
        private final String statement;
        private final MapValue params;
        private final TransactionalContext transactionalContext;

        public BoltResultHandleV1( String statement, MapValue params, TransactionalContext transactionalContext )
        {
            this.statement = statement;
            this.params = params;
            this.transactionalContext = transactionalContext;
        }

        @Override
        public BoltResult start() throws KernelException
        {
            try
            {
                Result result = queryExecutionEngine.executeQuery( statement, params, transactionalContext );
                if ( result instanceof QueryResultProvider )
                {
                    return newBoltResult( (QueryResultProvider) result, clock );
                }
                else
                {
                    throw new IllegalStateException( format( "Unexpected query execution result. Expected to get instance of %s but was %s.",
                                                              QueryResultProvider.class.getName(), result.getClass().getName() ) );
                }
            }
            catch ( KernelException e )
            {
                close( false );
                throw new QueryExecutionKernelException( e );
            }
            catch ( Throwable e )
            {
                close( false );
                throw e;
            }
        }

        protected BoltResult newBoltResult( QueryResultProvider result, Clock clock )
        {
            return new CypherAdapterStream( result.queryResult(), clock );
        }

        @Override
        public void close( boolean success )
        {
            transactionalContext.close( success );
        }

        @Override
        public void terminate()
        {
            transactionalContext.terminate();
        }
    }
}
