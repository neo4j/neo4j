/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.collection.pool.Pool;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.newapi.DefaultCursors;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.TransactionTracer.NULL;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

public class KernelTransactionFactory
{
    public static class Instances
    {
        public KernelTransactionImplementation transaction;
        public StorageEngine storageEngine;
        public StoreReadLayer storeReadLayer;
        public StorageStatement storageStatement;

        public Instances( KernelTransactionImplementation transaction, StorageEngine storageEngine,
                StoreReadLayer storeReadLayer, StorageStatement storageStatement )
        {
            this.transaction = transaction;
            this.storageEngine = storageEngine;
            this.storeReadLayer = storeReadLayer;
            this.storageStatement = storageStatement;
        }
    }

    private KernelTransactionFactory()
    {
    }

    static Instances kernelTransactionWithInternals( LoginContext loginContext )
    {
        TransactionHeaderInformation headerInformation = new TransactionHeaderInformation( -1, -1, new byte[0] );
        TransactionHeaderInformationFactory headerInformationFactory = mock( TransactionHeaderInformationFactory.class );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );

        StorageEngine storageEngine = mock( StorageEngine.class );
        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        StorageStatement storageStatement = mock( StorageStatement.class );
        when( storeReadLayer.newStatement() ).thenReturn( storageStatement );
        when( storageEngine.storeReadLayer() ).thenReturn( storeReadLayer );

        KernelTransactionImplementation transaction = new KernelTransactionImplementation(
                mock( StatementOperationParts.class ),
                mock( SchemaWriteGuard.class ),
                new TransactionHooks(),
                mock( ConstraintIndexCreator.class ), new Procedures(), headerInformationFactory,
                mock( TransactionRepresentationCommitProcess.class ), mock( TransactionMonitor.class ),
                mock( Supplier.class ),
                mock( Pool.class ),
                Clocks.systemClock(), new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ), NULL,
                LockTracer.NONE,
                PageCursorTracerSupplier.NULL,
                storageEngine, new CanWrite(), new DefaultCursors(), AutoIndexing.UNSUPPORTED,
                mock( ExplicitIndexStore.class ), EmptyVersionContextSupplier.EMPTY, ON_HEAP, new StandardConstraintSemantics(),
                mock( SchemaState.class), mock( IndexingService.class ), mock( IndexProviderMap.class ) );

        StatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );

        transaction.initialize( 0, 0, statementLocks, KernelTransaction.Type.implicit,
                loginContext.authorize( s -> -1 ), 0L, 1L );

        return new Instances( transaction, storageEngine, storeReadLayer, storageStatement );
    }

    static KernelTransaction kernelTransaction( LoginContext loginContext )
    {
        return kernelTransactionWithInternals( loginContext ).transaction;
    }
}
