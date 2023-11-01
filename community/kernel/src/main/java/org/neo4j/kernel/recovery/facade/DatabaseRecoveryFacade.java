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
            RecoveryMode recoveryMode)
            throws IOException {
        recovery(layout, recoveryCriteria, recoveryFacadeMonitor, recoveryMode);
    }

    @Override
    public void performRecovery(DatabaseLayout databaseLayout) throws IOException {
        performRecovery(databaseLayout, EMPTY_MONITOR, RecoveryMode.FULL);
    }

    @Override
    public void performRecovery(DatabaseLayout databaseLayout, RecoveryFacadeMonitor monitor, RecoveryMode mode)
            throws IOException {
        performRecovery(databaseLayout, RecoveryCriteria.ALL, monitor, mode);
    }

    @Override
    public void performRecovery(
            DatabaseLayout databaseLayout, RecoveryCriteria recoveryCriteria, RecoveryFacadeMonitor monitor)
            throws IOException {
        performRecovery(databaseLayout, recoveryCriteria, monitor, RecoveryMode.FULL);
    }

    private void recovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            RecoveryMode mode)
            throws IOException {
        monitor.recoveryStarted();
        Recovery.performRecovery(Recovery.context(
                        fs,
                        pageCache,
                        tracers,
                        config,
                        databaseLayout,
                        memoryTracker,
                        IOController.DISABLED,
                        logProvider,
                        emptyLogsFallbackKernelVersion)
                .recoveryPredicate(recoveryCriteria.toPredicate())
                .recoveryMode(mode));
        monitor.recoveryCompleted();
    }
}
