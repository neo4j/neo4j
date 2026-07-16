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
package org.neo4j.fleetmanagement.bootstrap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.FleetManagementSettings;
import org.neo4j.fleetmanagement.communication.ConfigService;
import org.neo4j.fleetmanagement.communication.ConnectService;
import org.neo4j.fleetmanagement.communication.MetricsService;
import org.neo4j.fleetmanagement.communication.MigrationToAuraService;
import org.neo4j.fleetmanagement.communication.PingService;
import org.neo4j.fleetmanagement.communication.QueryService;
import org.neo4j.fleetmanagement.communication.SecurityLogsService;
import org.neo4j.fleetmanagement.communication.TopologyService;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.diagnostics.DiagnosticsService;
import org.neo4j.fleetmanagement.procedures.MetricNamesSupplier;
import org.neo4j.fleetmanagement.procedures.Neo4jConfigNamesSupplier;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

public class FleetManagement extends LifecycleAdapter {
    private MainService mainService;
    private ScheduledExecutorService scheduler;
    private State state;
    private ConfigService configService;
    private DiagnosticsService diagnosticsService;

    public FleetManagement(
            LogService logService,
            DatabaseManagementService databaseManagementService,
            Config config,
            DbmsInfo dbmsInfo,
            FileSystemAbstraction fs,
            ServerIdentity serverIdentity,
            Monitors monitoring,
            State state,
            DatabaseContextProvider<DatabaseContext> databaseContextProvider) {

        if (!config.get(FleetManagementSettings.fleet_manager_enabled)) {
            // Stop immediately if disabled
            return;
        }

        ITransactor transactor;
        Log log;
        Configuration configuration;
        try {
            var neo4jLog = logService.getUserLog(FleetManagement.class);
            Logger.initLogger(neo4jLog);
            log = Logger.getNeo4jLogger();
            this.state = state;
            configuration = new Configuration();
            MetricNamesSupplier.setConfiguration(configuration);
            Neo4jConfigNamesSupplier.setConfiguration(configuration);
            // Need to explicitly specify constructor arg types because reflection will not pick the interface type
            transactor = Reflection.getConstructorOfImplementationOf(
                            ITransactor.class, DbmsInfo.class, ServerIdentity.class, State.class)
                    .newInstance(dbmsInfo, serverIdentity, this.state);
            transactor.init(databaseManagementService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        scheduler = Executors.newScheduledThreadPool(1, runnable -> {
            var thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("neo4j.FleetManagement");
            thread.setUncaughtExceptionHandler((t, e) -> Logger.getFleetManagerLogger()
                    .debug("Uncaught exception in FleetManagement thread: %s", ExceptionUtils.getStackTrace(e)));
            return thread;
        });

        var upstream = new Upstream(transactor, log, config, this.state);
        var connectService =
                new ConnectService(config, fs, transactor, serverIdentity, upstream, this.state, configuration);
        var reportingService =
                new TopologyService(config, fs, transactor, serverIdentity, upstream, this.state, configuration);
        var metricsService =
                new MetricsService(transactor, serverIdentity, upstream, config, this.state, configuration, dbmsInfo);
        this.configService = new ConfigService(transactor, serverIdentity, upstream, config, this.state, configuration);
        var pingService = new PingService(config, fs, transactor, upstream, serverIdentity, this.state, configuration);
        var queryService = new QueryService(transactor, serverIdentity, upstream, this.state, configuration);
        var securityLogsService =
                new SecurityLogsService(transactor, upstream, this.state, serverIdentity, configuration);
        var migrationService = new MigrationToAuraService(
                serverIdentity, transactor, upstream, this.state, config, configuration, databaseContextProvider);

        var clusterSync = new ClusterSync(transactor, upstream, this.state);

        this.diagnosticsService =
                new DiagnosticsService(logService, config, fs, databaseManagementService, databaseContextProvider);

        this.mainService = new MainService(
                reportingService,
                metricsService,
                queryService,
                migrationService,
                clusterSync,
                scheduler,
                connectService,
                pingService,
                securityLogsService,
                this.state,
                configuration,
                monitoring,
                config);
    }

    @Override
    public void start() {
        if (mainService == null) {
            return;
        }
        configService.start();
        mainService.start();
    }

    @Override
    public void stop() {
        if (mainService == null) {
            return;
        }
        state.removePropertyChangeListeners();
        mainService.stop();
    }

    @Override
    public void shutdown() {
        if (mainService == null) {
            return;
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
