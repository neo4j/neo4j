/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.TransactionTimeout.NO_TIMEOUT;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard.EMPTY_GUARD;
import static org.neo4j.kernel.impl.locking.NoLocksClient.NO_LOCKS_CLIENT;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.time.Clocks;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;

public final class KernelTransactionFactory {
    public static class Instances {
        public KernelTransactionImplementation transaction;

        Instances(KernelTransactionImplementation transaction) {
            this.transaction = transaction;
        }
    }

    private KernelTransactionFactory() {}

    private static Instances kernelTransactionWithInternals(LoginContext loginContext) {
        StorageEngine storageEngine = mock(StorageEngine.class, RETURNS_MOCKS);
        StorageReader storageReader = mock(StorageReader.class);
        when(storageEngine.newReader()).thenReturn(storageReader);
        when(storageEngine.newCommandCreationContext(anyBoolean())).thenReturn(mock(CommandCreationContext.class));
        when(storageEngine.createStorageCursors(any())).thenReturn(StoreCursors.NULL);

        var locks = mock(LockManager.class);
        when(locks.newClient()).thenReturn(NO_LOCKS_CLIENT);
        TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        KernelVersionProvider kernelVersionProvider = LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
        KernelTransactionImplementation transaction = new KernelTransactionImplementation(
                Config.defaults(),
                mock(DatabaseTransactionEventListeners.class),
                mock(ConstraintIndexCreator.class),
                mock(InternalTransactionCommitProcess.class),
                mock(TransactionMonitor.class),
                mock(Pool.class),
                Clocks.nanoClock(),
                new AtomicReference<>(CpuClock.NOT_AVAILABLE),
                mock(DatabaseTracers.class, RETURNS_MOCKS),
                storageEngine,
                any -> CanWrite.INSTANCE,
                new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER),
                ON_HEAP,
                new StandardConstraintSemantics(),
                mock(SchemaState.class),
                mockedTokenHolders(),
                mock(ElementIdMapper.class),
                mock(IndexingService.class),
                mock(IndexStatisticsStore.class),
                dependenciesOf(mock(GraphDatabaseFacade.class)),
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                LeaseService.NO_LEASES,
                MemoryPools.NO_TRACKING,
                DatabaseReadOnlyChecker.writable(),
                TransactionExecutionMonitor.NO_OP,
                CommunitySecurityLog.NULL_LOG,
                locks,
                new TransactionCommitmentFactory(transactionIdStore),
                mock(KernelTransactions.class),
                TransactionIdGenerator.EMPTY,
                mock(DbmsRuntimeVersionProvider.class),
                kernelVersionProvider,
                mock(LogicalTransactionStore.class),
                mock(ServerIdentity.class),
                ApplyEnrichmentStrategy.NO_ENRICHMENT,
                mock(DatabaseHealth.class),
                NullLogProvider.getInstance(),
                TransactionValidatorFactory.EMPTY_VALIDATOR_FACTORY,
                EMPTY_GUARD,
                storageEngine.getOpenOptions().contains(MULTI_VERSIONED));

        transaction.initialize(
                0,
                KernelTransaction.Type.IMPLICIT,
                loginContext.authorize(
                        LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME, CommunitySecurityLog.NULL_LOG),
                NO_TIMEOUT,
                1L,
                EMBEDDED_CONNECTION,
                mock(ProcedureView.class));

        return new Instances(transaction);
    }

    private static TokenHolders mockedTokenHolders() {
        return new TokenHolders(mock(TokenHolder.class), mock(TokenHolder.class), mock(TokenHolder.class));
    }

    static KernelTransaction kernelTransaction(LoginContext loginContext) {
        return kernelTransactionWithInternals(loginContext).transaction;
    }
}
