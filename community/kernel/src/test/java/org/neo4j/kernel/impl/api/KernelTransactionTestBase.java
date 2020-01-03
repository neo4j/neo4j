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
package org.neo4j.kernel.impl.api;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.pool.Pool;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.values.storable.Value;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;

public class KernelTransactionTestBase
{
    protected final StorageEngine storageEngine = mock( StorageEngine.class );
    protected final NeoStores neoStores = mock( NeoStores.class );
    protected final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    protected final StorageReader readLayer = mock( StorageReader.class );
    protected final TransactionHooks hooks = new TransactionHooks();
    protected final AuxiliaryTransactionStateManager auxTxStateManager = new KernelAuxTransactionStateManager();
    protected final ExplicitIndexTransactionState explicitIndexState = mock( ExplicitIndexTransactionState.class );
    protected final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
    protected final CapturingCommitProcess commitProcess = new CapturingCommitProcess();
    protected final TransactionHeaderInformation headerInformation = mock( TransactionHeaderInformation.class );
    protected final TransactionHeaderInformationFactory headerInformationFactory =  mock( TransactionHeaderInformationFactory.class );
    protected final SchemaWriteGuard schemaWriteGuard = mock( SchemaWriteGuard.class );
    protected final FakeClock clock = Clocks.fakeClock();
    protected final Pool<KernelTransactionImplementation> txPool = mock( Pool.class );
    protected final StatementOperationParts statementOperations = mock( StatementOperationParts.class );
    protected CollectionsFactory collectionsFactory;

    private final long defaultTransactionTimeoutMillis = Config.defaults().get( GraphDatabaseSettings.transaction_timeout ).toMillis();

    @Before
    public void before() throws Exception
    {
        collectionsFactory = Mockito.spy( new TestCollectionsFactory() );
        when( headerInformation.getAdditionalHeader() ).thenReturn( new byte[0] );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
        when( storageEngine.newReader() ).thenReturn( readLayer );
        doAnswer( invocation -> ((Collection<StorageCommand>) invocation.getArgument(0) ).add( new Command
                .RelationshipCountsCommand( 1, 2,3, 4L ) ) )
            .when( storageEngine ).createCommands(
                    anyCollection(),
                    any( ReadableTransactionState.class ),
                    any( StorageReader.class ), any( ResourceLocker.class ),
                    anyLong(),
                    any( TxStateVisitor.Decorator.class ) );
    }

    public KernelTransactionImplementation newTransaction( long transactionTimeoutMillis )
    {
        return newTransaction( 0, AUTH_DISABLED, transactionTimeoutMillis );
    }

    public KernelTransactionImplementation newTransaction( LoginContext loginContext )
    {
        return newTransaction( 0, loginContext );
    }

    public KernelTransactionImplementation newTransaction( LoginContext loginContext, Locks.Client locks )
    {
        return newTransaction( 0, loginContext, locks, defaultTransactionTimeoutMillis );
    }

    public KernelTransactionImplementation newTransaction( long lastTransactionIdWhenStarted, LoginContext loginContext )
    {
        return newTransaction( lastTransactionIdWhenStarted, loginContext, defaultTransactionTimeoutMillis );
    }

    public KernelTransactionImplementation newTransaction( long lastTransactionIdWhenStarted, LoginContext loginContext,
            long transactionTimeoutMillis )
    {
        return newTransaction( lastTransactionIdWhenStarted, loginContext, new NoOpClient(), transactionTimeoutMillis );
    }

    public KernelTransactionImplementation newTransaction( long lastTransactionIdWhenStarted, LoginContext loginContext,
            Locks.Client locks, long transactionTimeout )
    {
        KernelTransactionImplementation tx = newNotInitializedTransaction();
        StatementLocks statementLocks = new SimpleStatementLocks( locks );
        SecurityContext securityContext = loginContext.authorize( s -> -1, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        tx.initialize( lastTransactionIdWhenStarted, BASE_TX_COMMIT_TIMESTAMP,statementLocks, Type.implicit,
                securityContext, transactionTimeout, 1L );
        return tx;
    }

    public KernelTransactionImplementation newNotInitializedTransaction()
    {
        return new KernelTransactionImplementation( Config.defaults(), statementOperations, schemaWriteGuard, hooks, null, null, headerInformationFactory,
                commitProcess, transactionMonitor, auxTxStateManager, txPool, clock, new AtomicReference<>( CpuClock.NOT_AVAILABLE ),
                new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ), TransactionTracer.NULL, LockTracer.NONE, PageCursorTracerSupplier.NULL, storageEngine,
                new CanWrite(), AutoIndexing.UNSUPPORTED, mock( ExplicitIndexStore.class ), EmptyVersionContextSupplier.EMPTY, () -> collectionsFactory,
                new StandardConstraintSemantics(), mock( SchemaState.class ), mock( IndexingService.class ), mockedTokenHolders(), new Dependencies() );
    }

    public class CapturingCommitProcess implements TransactionCommitProcess
    {
        private long txId = TransactionIdStore.BASE_TX_ID;
        public TransactionRepresentation transaction;

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent,
                            TransactionApplicationMode mode )
        {
            assert transaction == null : "Designed to only allow one transaction";
            assert batch.next() == null : "Designed to only allow one transaction";
            transaction = batch.transactionRepresentation();
            return ++txId;
        }
    }

    private class TestCollectionsFactory implements CollectionsFactory
    {

        @Override
        public MutableLongSet newLongSet()
        {
            return OnHeapCollectionsFactory.INSTANCE.newLongSet();
        }

        @Override
        public MutableLongDiffSetsImpl newLongDiffSets()
        {
            return OnHeapCollectionsFactory.INSTANCE.newLongDiffSets();
        }

        @Override
        public MutableLongObjectMap<Value> newValuesMap()
        {
            return new LongObjectHashMap<>();
        }

        @Override
        public MemoryTracker getMemoryTracker()
        {
            return OnHeapCollectionsFactory.INSTANCE.getMemoryTracker();
        }

        @Override
        public void release()
        {
            // nop
        }
    }
}
