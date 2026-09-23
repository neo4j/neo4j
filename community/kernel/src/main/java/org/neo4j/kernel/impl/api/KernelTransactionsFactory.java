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
package org.neo4j.kernel.impl.api;

import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseMonitors;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.chunk.TransactionRollbackProcess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.ExceptionHandlerService;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.wal.TransactionCommitmentFactory;

public interface KernelTransactionsFactory {
    KernelTransactions create(
            Config config,
            LockManager lockManager,
            ConstraintIndexCreator constraintIndexCreator,
            TransactionCommitProcess transactionCommitProcess,
            TransactionRollbackProcess rollbackProcess,
            DatabaseTransactionEventListeners eventListeners,
            TransactionMonitor transactionMonitor,
            AvailabilityGuard databaseAvailabilityGuard,
            StorageEngine storageEngine,
            GlobalProcedures globalProcedures,
            DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider,
            TransactionIdStore transactionIdStore,
            KernelVersionProvider kernelVersionProvider,
            ServerIdentity serverIdentity,
            SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef,
            AccessCapabilityFactory accessCapabilityFactory,
            CursorContextFactory contextFactory,
            ConstraintSemantics constraintSemantics,
            SchemaState schemaState,
            TokenHolders tokenHolders,
            ElementIdMapper elementIdMapper,
            NamedDatabaseId namedDatabaseId,
            IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore,
            Dependencies databaseDependencies,
            DatabaseTracers tracers,
            LeaseService leaseService,
            ScopedMemoryPool transactionMemoryPool,
            DatabaseReadOnlyChecker readOnlyDatabaseChecker,
            TransactionExecutionMonitor transactionExecutionMonitor,
            IdController.IdFreeCondition externalIdReuseCondition,
            TransactionCommitmentFactory commitmentFactory,
            TransactionIdSequence transactionIdSequence,
            TransactionIdGenerator transactionIdGenerator,
            DatabaseHealth databaseHealth,
            TransactionValidatorFactory transactionValidatorFactory,
            ExceptionHandlerService exceptionHandlerService,
            InternalLogProvider internalLogProvider,
            TopologyGraphDbmsModel.HostedOnMode mode,
            DatabaseMonitors databaseMonitors,
            RaftUpgradeBarrier raftUpgradeBarrier);
}
