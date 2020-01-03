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
package org.neo4j.kernel.api;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.pool.Pool;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.TransactionTracer.NULL;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;

public class KernelTransactionFactory
{
    public static class Instances
    {
        public KernelTransactionImplementation transaction;

        public Instances( KernelTransactionImplementation transaction )
        {
            this.transaction = transaction;
        }
    }

    private KernelTransactionFactory()
    {
    }

    private static Instances kernelTransactionWithInternals( LoginContext loginContext )
    {
        TransactionHeaderInformation headerInformation = new TransactionHeaderInformation( -1, -1, new byte[0] );
        TransactionHeaderInformationFactory headerInformationFactory = mock( TransactionHeaderInformationFactory.class );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );

        StorageEngine storageEngine = mock( StorageEngine.class );
        StorageReader storageReader = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( storageReader );

        KernelTransactionImplementation transaction =
                new KernelTransactionImplementation( Config.defaults(), mock( StatementOperationParts.class ), mock( SchemaWriteGuard.class ),
                        new TransactionHooks(), mock( ConstraintIndexCreator.class ), new Procedures(), headerInformationFactory,
                        mock( TransactionRepresentationCommitProcess.class ), mock( TransactionMonitor.class ), mock( AuxiliaryTransactionStateManager.class ),
                        mock( Pool.class ), Clocks.nanoClock(), new AtomicReference<>( CpuClock.NOT_AVAILABLE ),
                        new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ), NULL, LockTracer.NONE, PageCursorTracerSupplier.NULL, storageEngine,
                        new CanWrite(), AutoIndexing.UNSUPPORTED, mock( ExplicitIndexStore.class ), EmptyVersionContextSupplier.EMPTY, ON_HEAP,
                        new StandardConstraintSemantics(), mock( SchemaState.class ), mock( IndexingService.class ), mockedTokenHolders(), new Dependencies() );

        StatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );

        transaction.initialize( 0, 0, statementLocks, KernelTransaction.Type.implicit,
                loginContext.authorize( s -> -1, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), 0L, 1L );

        return new Instances( transaction );
    }

    static KernelTransaction kernelTransaction( LoginContext loginContext )
    {
        return kernelTransactionWithInternals( loginContext ).transaction;
    }
}
