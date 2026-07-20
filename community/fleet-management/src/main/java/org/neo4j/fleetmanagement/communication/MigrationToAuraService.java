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
package org.neo4j.fleetmanagement.communication;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.MigrationToAura;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.migration.aura.ApiConnector;
import org.neo4j.fleetmanagement.migration.aura.CapturingExecutionContext;
import org.neo4j.fleetmanagement.migration.aura.MigrationExecution;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

public class MigrationToAuraService extends AbstractReportingService implements PropertyChangeListener {

    private final Config config;
    private final DatabaseContextProvider<DatabaseContext> databaseContextProvider;
    private final ThreadPoolExecutor executor;
    private final AtomicReference<MigrationExecution> migrationExecution = new AtomicReference<>();
    private final ApiConnector apiConnector;
    private final CapturingContextFactory capturingContextFactory;

    private record CapturingContextFactory(
            Config config, ITransactor transactor, ServerIdentity serverIdentity, Upstream upstream) {

        CapturingExecutionContext create(String migrationId) {
            return CapturingExecutionContext.create(
                    homeDir(),
                    confDir(),
                    new DefaultFileSystemAbstraction(),
                    dbmsId(),
                    serverId(),
                    projectId(),
                    migrationId);
        }

        private Path homeDir() {
            return config.get(GraphDatabaseSettings.neo4j_home);
        }

        private Path confDir() {
            return config.get(GraphDatabaseSettings.configuration_directory);
        }

        private String dbmsId() {
            return TopologyMapper.getDbmsId(transactor::getDatabases);
        }

        private String serverId() {
            return serverIdentity.serverId().uuid().toString();
        }

        private String projectId() {
            return upstream.getApiKey().projectId();
        }
    }

    public MigrationToAuraService(
            ServerIdentity serverIdentity,
            ITransactor transactor,
            Upstream upstream,
            State state,
            Config config,
            Configuration configuration,
            DatabaseContextProvider<DatabaseContext> databaseContextProvider) {
        super(transactor, upstream, state, configuration);
        this.config = config;
        this.databaseContextProvider = databaseContextProvider;
        this.executor = createRejectingExecutor();
        this.apiConnector = createApiConnector();
        this.capturingContextFactory = new CapturingContextFactory(config, transactor, serverIdentity, upstream);
        configuration.addPropertyChangeListener(this);
    }

    /**
     * @return a ThreadPoolExecutor with a single thread and a SynchronousQueue as work queue,
     * so that if a task is already running, the next one will be rejected with RejectedExecutionException.
     * This is to avoid multiple concurrent migrations, which could cause performance issues and conflicts.
     */
    private static ThreadPoolExecutor createRejectingExecutor() {
        return new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    var thread = Executors.defaultThreadFactory().newThread(runnable);
                    thread.setName("neo4j.FleetManagement.MigrationToAura");
                    thread.setUncaughtExceptionHandler((t, e) -> Logger.getFleetManagerLogger()
                            .debug(
                                    "Uncaught exception in FleetManagement migration to Aura thread: %s",
                                    ExceptionUtils.getStackTrace(e)));
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy() // Throws RejectedExecutionException,
                );
    }

    @Override
    public void report() {
        var execution = migrationExecution.get();
        if (execution != null) {
            execution.reportStatus();
        }
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        if (Configuration.MIGRATIONS_TO_AURA_CHANGE.equals(evt.getPropertyName())) {
            @SuppressWarnings("unchecked")
            var migrationsToAura = (List<MigrationToAura>) evt.getNewValue();
            if (migrationsToAura != null && !migrationsToAura.isEmpty()) {
                // Take the first migration in the list. The list is for future implementations where several databases
                // can be migrated simultaneously.
                execute(migrationsToAura.getFirst());
            }
        }
    }

    private void execute(MigrationToAura migrationToAura) {
        var databaseContext = databaseContextProvider.getDatabaseContext(migrationToAura.sourceDatabaseName);
        if (databaseContext.isEmpty()) {
            userLog.info(
                    "No database with name %s, ignoring the migration %s on this server",
                    migrationToAura.sourceDatabaseName, migrationToAura.migrationId);
            return;
        }
        userLog.info("Processing migration to Aura with id %s", migrationToAura.migrationId);
        MigrationExecution execution = new MigrationExecution(
                userLog,
                fleetManagerLog,
                config,
                capturingContextFactory.create(migrationToAura.migrationId),
                apiConnector,
                databaseContext.get());
        if (!migrationExecution.compareAndSet(null, execution)) {
            fleetManagerLog.debug(
                    "a migration task is already being processed, ignore message for migration %s",
                    migrationToAura.migrationId);
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    execution.processMigration(migrationToAura);
                } finally {
                    migrationExecution.compareAndSet(execution, null);
                }
            });
        } catch (RejectedExecutionException e) {
            migrationExecution.compareAndSet(execution, null);
            fleetManagerLog.debug(
                    "a migration task is already being processed, ignore message for migration %s",
                    migrationToAura.migrationId);
        }
    }

    private ApiConnector createApiConnector() {
        return new ApiConnector() {
            @Override
            public void call(Object msg, Upstream.Endpoint endpoint) {
                callApi(msg, endpoint);
            }

            @Override
            public <T> T call(Object msg, Upstream.Endpoint endpoint, Class<T> responseType) {
                return callApi(msg, endpoint, responseType);
            }
        };
    }

    public static class MigrationsToAuraReportingTask extends FleetManagerTask {
        private final MigrationToAuraService migrationToAuraService;

        public MigrationsToAuraReportingTask(
                State state, ClusterSync clusterSync, MigrationToAuraService migrationToAuraService) {
            super(state, clusterSync);
            this.migrationToAuraService = migrationToAuraService;
        }

        @Override
        protected void execute() {
            if (this.state.isConnected()) {
                this.migrationToAuraService.report();
            }
        }
    }

    public void stop() {
        try {
            executor.shutdown();
            fleetManagerLog.debug("Shutting down migration service");
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                userLog.warn("Executor did not terminate in 5 minutes, forcefully shutting down");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            userLog.error("Migration service shutdown interrupted, forcefully shutting down");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
