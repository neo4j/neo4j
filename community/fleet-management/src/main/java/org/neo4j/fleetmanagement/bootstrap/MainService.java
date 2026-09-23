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

import static org.neo4j.fleetmanagement.configuration.Configuration.TASK_REPORTING_INTERVAL_CHANGE;
import static org.neo4j.fleetmanagement.configuration.State.ACTIVE_CHANGE;
import static org.neo4j.fleetmanagement.configuration.State.CONNECTED_CHANGE;
import static org.neo4j.fleetmanagement.configuration.State.ROTATING_TOKEN_CHANGE;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.neo4j.configuration.Config;
import org.neo4j.fleetmanagement.communication.ConnectService;
import org.neo4j.fleetmanagement.communication.DiagnosticsService;
import org.neo4j.fleetmanagement.communication.MetricsService;
import org.neo4j.fleetmanagement.communication.MigrationToAuraService;
import org.neo4j.fleetmanagement.communication.PingService;
import org.neo4j.fleetmanagement.communication.QueryService;
import org.neo4j.fleetmanagement.communication.SecurityLogsService;
import org.neo4j.fleetmanagement.communication.TopologyService;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.logs.SecurityLogInterceptor;
import org.neo4j.fleetmanagement.queries.QueryInterceptor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;

public class MainService implements PropertyChangeListener {
    private final Log userLog;
    private final Logger fleetManagerLog;
    private final TopologyService topologyService;
    private final MetricsService metricsService;
    private final QueryService queryService;
    private final PingService pingService;
    private final ClusterSync clusterSync;
    private final ScheduledExecutorService scheduler;
    private final ConnectService connectService;
    private final SecurityLogsService securityLogsService;
    private final State state;
    private final Configuration configuration;
    private final Monitors monitoring;
    private final MigrationToAuraService migrationService;

    private ScheduledFuture<?> connectServiceTaskHandle;
    private ConnectService.ConnectServiceTask connectServiceTask;

    private final DiagnosticsService diagnosticsService;

    private Boolean lastActiveState = null;
    private Boolean lastConnectedState = null;
    private Boolean lastTokenRotatingState = null;
    private boolean queryMonitorAdded = false;
    private boolean securityLogMonitorAdded = false;
    private final QueryInterceptor queryInterceptor;
    private final SecurityLogInterceptor securityLogInterceptor;

    private final CopyOnWriteArrayList<ScheduledFuture<?>> jobHandles;

    public MainService(
            TopologyService topologyService,
            MetricsService metricsService,
            QueryService queryService,
            MigrationToAuraService migrationService,
            DiagnosticsService diagnosticsService,
            ClusterSync clusterSync,
            ScheduledExecutorService scheduler,
            ConnectService connectService,
            PingService pingService,
            SecurityLogsService securityLogsService,
            State state,
            Configuration configuration,
            Monitors monitoring,
            Config config) {
        this.userLog = Logger.getNeo4jLogger();
        this.fleetManagerLog = Logger.getFleetManagerLogger();
        this.topologyService = topologyService;
        this.metricsService = metricsService;
        this.queryService = queryService;
        this.securityLogsService = securityLogsService;
        this.migrationService = migrationService;
        this.pingService = pingService;
        this.clusterSync = clusterSync;
        this.scheduler = scheduler;
        this.connectService = connectService;
        this.configuration = configuration;
        this.monitoring = monitoring;

        this.state = state;
        this.jobHandles = new CopyOnWriteArrayList<>();
        this.queryInterceptor = new QueryInterceptor(queryService, config);
        this.securityLogInterceptor = new SecurityLogInterceptor(securityLogsService);
        this.diagnosticsService = diagnosticsService;
    }

    public void start() {
        if (!this.jobHandles.isEmpty()) {
            throw new IllegalStateException("Service already started");
        }
        this.state.addPropertyChangeListener(this);
        this.configuration.addPropertyChangeListener(this);
        this.metricsService.start();

        this.connectServiceTask = new ConnectService.ConnectServiceTask(state, clusterSync, connectService);
        this.connectServiceTaskHandle =
                this.scheduler.scheduleAtFixedRate(this.connectServiceTask, 1, 30, TimeUnit.SECONDS);
    }

    public void stop() {
        if (this.connectServiceTaskHandle != null) {
            this.connectServiceTaskHandle.cancel(false);
            this.connectServiceTaskHandle = null;
        }

        try {
            if (queryMonitorAdded) {
                this.queryService.report(); // Flush any remaining logs
            }
        } catch (Exception e) {
            // Ignore errors while flushing logs
        }
        stopReportingTasks();
        migrationService.stop();
        this.configuration.removePropertyChangeListeners();
        if (state.isConnected()) {
            this.connectService.disconnect("Service is stopped");
        }
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        switch (evt.getPropertyName()) {
            case ACTIVE_CHANGE:
                activeStateChangeHandler(evt);
                return;
            case CONNECTED_CHANGE:
                connectedStateChangeHandler(evt);
                return;
            case ROTATING_TOKEN_CHANGE:
                tokenRotatingStateChangeHandler(evt);
                return;
            case TASK_REPORTING_INTERVAL_CHANGE:
                handleConfigurationUpdate();
        }
    }

    private void tokenRotatingStateChangeHandler(PropertyChangeEvent evt) {
        var value = (Boolean) evt.getNewValue();
        if (lastTokenRotatingState != null && lastTokenRotatingState.equals(value)) {
            return;
        }
        lastTokenRotatingState = value;
        if (value) {
            userLog.info("Fleet Manager token is being rotated");
            this.connectService.reconnect("Token is being rotated");
        } else {
            userLog.info("Fleet Manager token is now rotated");
        }
    }

    private void connectedStateChangeHandler(PropertyChangeEvent evt) {
        var value = (Boolean) evt.getNewValue();
        if (lastConnectedState != null && lastConnectedState.equals(value)) {
            return;
        }
        lastConnectedState = value;
        if (value) {
            startReportingTasks();
            userLog.info("Fleet Manager connection successfully established");
        } else {
            stopReportingTasks();
            userLog.info("Fleet Manager connection was closed");
        }
    }

    private void activeStateChangeHandler(PropertyChangeEvent evt) {
        var value = (Boolean) evt.getNewValue();
        if (lastActiveState != null && lastActiveState.equals(value)) {
            return;
        }
        lastActiveState = value;
        if (value) {
            this.state.setConnectionMessage("Connecting");
            connectServiceTask.run();
            userLog.info("Fleet Manager is enabled");
        } else {
            if (state.isConnected()) {
                this.connectService.disconnect(null);
            }
            userLog.info("This server has built-in support for online monitoring through Fleet Manager. "
                    + "Visit https://console.neo4j.io to get started!");
        }
    }

    private void startReportingTasks() {
        var taskReportingInterval = configuration.getTaskReportingInterval();
        if (taskReportingInterval == null || taskReportingInterval.isEmpty()) {
            if (state.isConnected()) {
                this.connectService.disconnect("Reporting intervals are not configured.");
            }
            return;
        }
        taskReportingInterval.forEach((taskType, interval) -> {
            switch (taskType) {
                case TOPOLOGY:
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new TopologyService.TopologyReportingTask(state, clusterSync, topologyService),
                            1,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case METRICS:
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new MetricsService.MetricsReportingTask(state, clusterSync, metricsService),
                            1,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case QUERIES:
                    if (!queryMonitorAdded) {
                        // Start intercepting queries only when query task is activated
                        monitoring.addMonitorListener(this.queryInterceptor);
                        queryMonitorAdded = true;
                    }
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new QueryService.QueryReportingTask(state, clusterSync, queryService),
                            1,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case SECURITY_LOGS:
                    if (!securityLogMonitorAdded) {
                        monitoring.addMonitorListener(securityLogInterceptor);
                        securityLogMonitorAdded = true;
                    }
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new SecurityLogsService.SecurityLogsReportingTask(state, clusterSync, securityLogsService),
                            1,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case PING:
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new PingService.PingTask(state, clusterSync, pingService),
                            interval,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case MIGRATIONS_TO_AURA:
                    jobHandles.add(scheduler.scheduleAtFixedRate(
                            new MigrationToAuraService.MigrationsToAuraReportingTask(
                                    state, clusterSync, migrationService),
                            interval,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                case DIAGNOSTIC_REPORT:
                    jobHandles.add(this.scheduler.scheduleAtFixedRate(
                            new DiagnosticsService.DiagnosticsServiceTask(state, clusterSync, diagnosticsService),
                            interval,
                            interval,
                            TimeUnit.SECONDS));
                    break;
                default:
                    break;
            }
        });
    }

    private void stopReportingTasks() {
        fleetManagerLog.debug("Stopping reporting tasks");
        if (!jobHandles.isEmpty()) {
            jobHandles.forEach(jobHandle -> jobHandle.cancel(false));
            jobHandles.clear();
        }
        if (queryMonitorAdded) {
            monitoring.removeMonitorListener(this.queryInterceptor);
            queryMonitorAdded = false;
        }

        if (securityLogMonitorAdded) {
            monitoring.removeMonitorListener(securityLogInterceptor);
            securityLogMonitorAdded = false;
        }
    }

    private void handleConfigurationUpdate() {
        if (state.isConnected()) {
            userLog.info("Configuration updated, restarting reporting tasks with new intervals");
            stopReportingTasks();
            startReportingTasks();
        }
    }
}
