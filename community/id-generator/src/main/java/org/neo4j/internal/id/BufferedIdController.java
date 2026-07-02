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
package org.neo4j.internal.id;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

/**
 * Storage id controller that provide buffering possibilities to be able so safely free and reuse ids.
 * Allows perform clear and maintenance operations over currently buffered set of ids.
 * @see BufferingIdGeneratorFactory
 */
public class BufferedIdController extends LifecycleAdapter implements IdController {
    private static final String BUFFERED_ID_CONTROLLER = "idController";
    private final AbstractBufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private final JobScheduler scheduler;
    private final CursorContextFactory contextFactory;
    private final DatabaseConfig databaseConfig;
    private final String databaseName;
    private final InternalLog log;
    private IdMaintenanceJob freeIdsJob;
    private IdMaintenanceJob loadIdsJob;
    private volatile boolean running;
    private volatile DatabaseReadOnlyChecker databaseReadOnlyChecker;

    public BufferedIdController(
            AbstractBufferingIdGeneratorFactory bufferingIdGeneratorFactory,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            DatabaseConfig databaseConfig,
            String databaseName,
            LogService logService) {
        this.bufferingIdGeneratorFactory = bufferingIdGeneratorFactory;
        this.scheduler = scheduler;
        this.contextFactory = contextFactory;
        this.databaseConfig = databaseConfig;
        this.databaseName = databaseName;
        this.log = logService.getInternalLog(BufferedIdController.class);
    }

    @Override
    public void init() throws Exception {
        bufferingIdGeneratorFactory.init();
    }

    @Override
    public void start() throws Exception {
        bufferingIdGeneratorFactory.start();
        running = true;
        var monitoringParams = JobMonitoringParams.systemJob(databaseName, "ID generator maintenance");
        long intervalMillis = databaseConfig
                .get(GraphDatabaseInternalSettings.id_controller_maintenance_interval)
                .toMillis();
        freeIdsJob = new IdMaintenanceJob(scheduler, monitoringParams, intervalMillis, MAINTENANCE_FREE_IDS);
        loadIdsJob = new IdMaintenanceJob(scheduler, monitoringParams, intervalMillis, MAINTENANCE_LOAD_IDS);
    }

    @Override
    public void stop() throws Exception {
        running = false;
        IdMaintenanceJob freeIdsJob = this.freeIdsJob;
        IdMaintenanceJob loadIdsJob = this.loadIdsJob;
        try (freeIdsJob;
                loadIdsJob) {
            if (freeIdsJob != null) {
                freeIdsJob.cancelJob();
            }
            if (loadIdsJob != null) {
                loadIdsJob.cancelJob();
            }
        }
        bufferingIdGeneratorFactory.stop();
    }

    @Override
    public void shutdown() throws Exception {
        bufferingIdGeneratorFactory.shutdown();
    }

    @Override
    public void maintenance(int flags) {
        if (databaseReadOnlyChecker.isReadOnly()) {
            // Avoid doing this when in read-only mode since it may incur I/O and added space on disk
            return;
        }

        if ((flags & MAINTENANCE_FREE_IDS) != 0) {
            freeIdsJob.maintenance();
        }
        if ((flags & MAINTENANCE_LOAD_IDS) != 0) {
            loadIdsJob.maintenance();
        }
    }

    @Override
    public void initialize(
            FileSystemAbstraction fs,
            StoreFile storeFile,
            Config config,
            Supplier<TransactionSnapshot> snapshotSupplier,
            VisibilityHorizonVisibilityBoundary visibilityBoundary,
            IdFreeCondition condition,
            MemoryTracker memoryTracker,
            DatabaseReadOnlyChecker databaseReadOnlyChecker)
            throws IOException {
        bufferingIdGeneratorFactory.initialize(
                fs, storeFile, config, snapshotSupplier, visibilityBoundary, condition, memoryTracker);
        this.databaseReadOnlyChecker = databaseReadOnlyChecker;
    }

    private class IdMaintenanceJob implements AutoCloseable {
        private final Lock lock = new ReentrantLock();
        private final int flags;
        private volatile JobHandle<?> jobHandle;

        IdMaintenanceJob(JobScheduler scheduler, JobMonitoringParams monitoringParams, long intervalMillis, int flags) {
            this.flags = flags;
            this.jobHandle = scheduler.scheduleRecurring(
                    Group.STORAGE_MAINTENANCE,
                    monitoringParams,
                    this::maintenance,
                    intervalMillis,
                    intervalMillis,
                    MILLISECONDS);
        }

        private void maintenance() {
            lock.lock();
            try {
                if (running) {
                    try (var cursorContext = contextFactory.create(BUFFERED_ID_CONTROLLER)) {
                        bufferingIdGeneratorFactory.maintenance(flags, cursorContext);
                    } catch (Throwable t) {
                        log.error("Exception when performing id maintenance", t);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        void cancelJob() {
            JobHandle<?> jobHandle = this.jobHandle;
            try {
                if (jobHandle != null) {
                    jobHandle.cancel();
                }
            } finally {
                this.jobHandle = null;
            }
        }

        @Override
        public void close() {
            // this lock/unlock is to coordinate with cancelJob(), so that this will block until the job is completed,
            // if it's currently being run.
            lock.lock();
            lock.unlock();
        }
    }
}
