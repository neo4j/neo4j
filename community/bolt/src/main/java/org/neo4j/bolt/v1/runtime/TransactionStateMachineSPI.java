/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.function.Supplier;

import org.neo4j.bolt.v1.runtime.TransactionStateMachine.BoltResultHandle;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

class TransactionStateMachineSPI implements TransactionStateMachine.SPI
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private final GraphDatabaseAPI db;
    private final ThreadToStatementContextBridge txBridge;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionIdTracker transactionIdTracker;
    private final TransactionalContextFactory contextFactory;
    private final Duration txAwaitDuration;
    private final Clock clock;

    TransactionStateMachineSPI( GraphDatabaseAPI db, AvailabilityGuard availabilityGuard, Duration txAwaitDuration, Clock clock )
    {
        this.db = db;
        this.txBridge = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        this.queryExecutionEngine = db.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        this.transactionIdTracker = newTransactionIdTracker( db, availabilityGuard );
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
    public KernelTransaction beginTransaction( LoginContext loginContext )
    {
        db.beginTransaction( KernelTransaction.Type.explicit, loginContext );
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
    public BoltResultHandle executeQuery( BoltQuerySource querySource,
            LoginContext loginContext, String statement, MapValue params )
    {
        InternalTransaction internalTransaction = db.beginTransaction( implicit, loginContext );
        ClientConnectionInfo sourceDetails = new BoltConnectionInfo( querySource.principalName,
                querySource.clientName,
                querySource.connectionDescriptor.clientAddress(),
                querySource.connectionDescriptor.serverAddress() );
        TransactionalContext transactionalContext =
                contextFactory.newContext( sourceDetails, internalTransaction, statement, params );

        return new BoltResultHandle()
        {

            @Override
            public BoltResult start() throws KernelException
            {
                try
                {
                    Result result = queryExecutionEngine.executeQuery( statement, params, transactionalContext );
                    if ( result instanceof QueryResultProvider )
                    {
                        return new CypherAdapterStream( ((QueryResultProvider) result).queryResult(), clock );
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

        };
    }

    private static TransactionIdTracker newTransactionIdTracker( GraphDatabaseAPI db, AvailabilityGuard guard )
    {
        Supplier<TransactionIdStore> transactionIdStoreSupplier = db.getDependencyResolver().provideDependency( TransactionIdStore.class );
        return new TransactionIdTracker( transactionIdStoreSupplier, guard );
    }

    private static TransactionalContextFactory newTransactionalContextFactory( GraphDatabaseAPI db )
    {
        GraphDatabaseQueryService queryService = db.getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class );
        return Neo4jTransactionalContextFactory.create( queryService, locker );
    }
}
