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
import org.neo4j.kernel.recovery.IncompleteTransactionAction;
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
    private final boolean failOnCorruptedLogs;

    public DatabaseRecoveryFacade(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            MemoryTracker memoryTracker,
            InternalLogProvider logProvider,
            KernelVersionProvider emptyLogsFallbackKernelVersion) {
        this(fs, pageCache, tracers, config, memoryTracker, logProvider, emptyLogsFallbackKernelVersion, false);
    }

    public DatabaseRecoveryFacade(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            MemoryTracker memoryTracker,
            InternalLogProvider logProvider,
            KernelVersionProvider emptyLogsFallbackKernelVersion,
            boolean failOnCorruptedLogs) {
        this.fs = fs;
        this.pageCache = pageCache;
        this.tracers = tracers;
        this.config = config;
        this.memoryTracker = memoryTracker;
        this.logProvider = logProvider;
        this.emptyLogsFallbackKernelVersion = emptyLogsFallbackKernelVersion;
        this.failOnCorruptedLogs = failOnCorruptedLogs;
    }

    @Override
    public void performRecovery(
            DatabaseLayout layout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor recoveryFacadeMonitor,
            RecoveryMode recoveryMode,
            ChunkedTransactionTracker chunkedTransactionTracker,
            IncompleteTransactionAction incompleteTransactionAction)
            throws IOException {
        recovery(
                layout,
                recoveryCriteria,
                recoveryFacadeMonitor,
                recoveryMode,
                false,
                incompleteTransactionAction,
                chunkedTransactionTracker);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout,
            IncompleteTransactionAction incompleteTransactionAction,
            ChunkedTransactionTracker chunkedTransactionTracker)
            throws IOException {
        performRecovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                EMPTY_MONITOR,
                RecoveryMode.FULL,
                chunkedTransactionTracker,
                incompleteTransactionAction);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout,
            RecoveryFacadeMonitor monitor,
            RecoveryMode mode,
            ChunkedTransactionTracker chunkedTransactionTracker)
            throws IOException {
        performRecovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                monitor,
                mode,
                chunkedTransactionTracker,
                IncompleteTransactionAction.APPLY);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            IncompleteTransactionAction rollbackIncompleteTransactions)
            throws IOException {
        performRecovery(
                databaseLayout,
                recoveryCriteria,
                monitor,
                RecoveryMode.FULL,
                new ChunkedTransactionTracker(),
                rollbackIncompleteTransactions);
    }

    @Override
    public void forceRecovery(
            DatabaseLayout databaseLayout,
            RecoveryFacadeMonitor monitor,
            RecoveryMode recoveryMode,
            IncompleteTransactionAction rollbackIncompleteTransactions)
            throws IOException {
        recovery(
                databaseLayout,
                RecoveryCriteria.ALL,
                monitor,
                RecoveryMode.FULL,
                true,
                rollbackIncompleteTransactions,
                new ChunkedTransactionTracker());
    }

    private void recovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            RecoveryMode mode,
            boolean force,
            IncompleteTransactionAction incompleteTransactionAction,
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
        if (failOnCorruptedLogs) {
            // online recovery may never truncate a corrupt tail, regardless of config.
            recoveryContext.forceFailOnCorruptedLogs();
        }
        Recovery.performRecovery(recoveryContext
                .recoveryPredicate(recoveryCriteria.toPredicate())
                .incompleteTransactionAction(incompleteTransactionAction)
                .rollbackRegistry(chunkedTransactionTracker)
                .recoveryMode(mode));
        monitor.recoveryCompleted();
    }
}
