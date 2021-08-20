/*
 * Copyright (c) "Neo4j"
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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.memory.MemoryPools;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.time.Clocks;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

public final class KernelTransactionFactory
{
    public static class Instances
    {
        public KernelTransactionImplementation transaction;

        Instances( KernelTransactionImplementation transaction )
        {
            this.transaction = transaction;
        }
    }

    private KernelTransactionFactory()
    {
    }

    private static Instances kernelTransactionWithInternals( LoginContext loginContext )
    {
        StorageEngine storageEngine = mock( StorageEngine.class );
        StorageReader storageReader = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( storageReader );
        when( storageEngine.newCommandCreationContext( any() ) ).thenReturn( mock( CommandCreationContext.class ) );
        when( storageEngine.createStorageCursors( any() ) ).thenReturn( StoreCursors.NULL );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( mock( GraphDatabaseFacade.class ) );
        KernelTransactionImplementation transaction =
                new KernelTransactionImplementation( Config.defaults(), mock( DatabaseTransactionEventListeners.class ),
                                                     mock( ConstraintIndexCreator.class ), mock( GlobalProcedures.class ),
                                                     mock( InternalTransactionCommitProcess.class ), mock( TransactionMonitor.class ),
                                                     mock( Pool.class ), Clocks.nanoClock(), new AtomicReference<>( CpuClock.NOT_AVAILABLE ),
                                                     mock( DatabaseTracers.class, RETURNS_MOCKS ), storageEngine,
                                                     any -> CanWrite.INSTANCE, EmptyVersionContextSupplier.EMPTY, ON_HEAP,
                                                     new StandardConstraintSemantics(), mock( SchemaState.class ), mockedTokenHolders(),
                                                     mock( IndexingService.class ),
                                                     mock( IndexStatisticsStore.class ), dependencies, from( DEFAULT_DATABASE_NAME, UUID.randomUUID() ),
                                                     LeaseService.NO_LEASES, MemoryPools.NO_TRACKING, DatabaseReadOnlyChecker.writable(),
                                                     TransactionExecutionMonitor.NO_OP, CommunitySecurityLog.NULL_LOG, () -> KernelVersion.LATEST,
                                                     mock( DbmsRuntimeRepository.class ), new NoOpClient(), mock( KernelTransactions.class ) );

        transaction.initialize( 0, 0, KernelTransaction.Type.IMPLICIT,
                loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME, CommunitySecurityLog.NULL_LOG ), 0L, 1L, EMBEDDED_CONNECTION );

        return new Instances( transaction );
    }

    private static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }

    static KernelTransaction kernelTransaction( LoginContext loginContext )
    {
        return kernelTransactionWithInternals( loginContext ).transaction;
    }
}
