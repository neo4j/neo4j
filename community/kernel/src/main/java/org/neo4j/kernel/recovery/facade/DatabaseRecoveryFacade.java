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
package org.neo4j.kernel.recovery.facade;

import static org.neo4j.kernel.recovery.IncompleteTransactionAction.ROLLBACK;
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.STOP;
import static org.neo4j.kernel.recovery.facade.RecoveryFacadeMonitor.EMPTY_MONITOR;

import java.io.IOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.recovery.RecoveryMode;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;

public class DatabaseRecoveryFacade implements RecoveryFacade {
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final DatabaseTracers tracers;
    private final Config config;
    private final MemoryTracker memoryTracker;
    private final InternalLogProvider logProvider;
    private final KernelVersionProvider emptyLogsFallbackKernelVersion;

    public DatabaseRecoveryFacade(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            MemoryTracker memoryTracker,
            InternalLogProvider logProvider,
            KernelVersionProvider emptyLogsFallbackKernelVersion) {
        this.fs = fs;
        this.pageCache = pageCache;
        this.tracers = tracers;
        this.config = config;
        this.memoryTracker = memoryTracker;
        this.logProvider = logProvider;
        this.emptyLogsFallbackKernelVersion = emptyLogsFallbackKernelVersion;
    }

    @Override
    public void performRecovery(
            DatabaseLayout layout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor recoveryFacadeMonitor,
            RecoveryMode recoveryMode,
            ChunkedTransactionTracker chunkedTransactionTracker,
            boolean rollbackIncompleteTransactions,
            boolean forceFailOnCorruptedLogs)
            throws IOException {
        recovery(
                layout,
                recoveryCriteria,
                recoveryFacadeMonitor,
                recoveryMode,
                false,
                rollbackIncompleteTransactions,
                forceFailOnCorruptedLogs,
                chunkedTransactionTracker);
    }

    @Override
    public void performRecovery(DatabaseLayout databaseLayout, ChunkedTransactionTracker chunkedTransactionTracker)
            throws IOException {
        performRecovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                EMPTY_MONITOR,
                RecoveryMode.FULL,
                chunkedTransactionTracker,
                false,
                false);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout,
            RecoveryFacadeMonitor monitor,
            RecoveryMode mode,
            boolean forceFailOnCorruptedLogs)
            throws IOException {
        performRecovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                monitor,
                mode,
                new ChunkedTransactionTracker(),
                true,
                forceFailOnCorruptedLogs);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            boolean rollbackIncompleteTransactions)
            throws IOException {
        performRecovery(
                databaseLayout,
                recoveryCriteria,
                monitor,
                RecoveryMode.FULL,
                new ChunkedTransactionTracker(),
                rollbackIncompleteTransactions,
                false);
    }

    @Override
    public void forceRecovery(
            DatabaseLayout databaseLayout,
            RecoveryFacadeMonitor monitor,
            RecoveryMode recoveryMode,
            boolean rollbackIncompleteTransactions)
            throws IOException {
        recovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                monitor,
                RecoveryMode.FULL,
                true,
                rollbackIncompleteTransactions,
                false,
                new ChunkedTransactionTracker());
    }

    private void recovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            RecoveryMode mode,
            boolean force,
            boolean rollbackIncompleteTransactions,
            boolean forceFailOnCorruptedLogs,
            ChunkedTransactionTracker chunkedTransactionTracker)
            throws IOException {
        monitor.recoveryStarted();
        var recoveryContext = Recovery.contextWithNoLogTail(
                fs,
                pageCache,
                tracers,
                config,
                databaseLayout,
                memoryTracker,
                IOController.DISABLED,
                logProvider,
                emptyLogsFallbackKernelVersion);
        if (force) {
            recoveryContext.force();
        }
        if (forceFailOnCorruptedLogs) {
            // this forces recovery to fail on corrupted logs, regardless of the configuration. There are
            // clustering components that may never accept truncating corrupt log tails.
            recoveryContext.forceFailOnCorruptedLogs();
        }
        Recovery.performRecovery(recoveryContext
                .recoveryPredicate(recoveryCriteria.toPredicate())
                .incompleteTransactionAction(rollbackIncompleteTransactions ? ROLLBACK : STOP)
                .rollbackRegistry(chunkedTransactionTracker)
                .recoveryMode(mode));
        monitor.recoveryCompleted();
    }
}
