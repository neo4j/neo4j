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

import static org.neo4j.fleetmanagement.configuration.Configuration.updateConfigurationIfPresent;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.ConfigurationResponse;
import org.neo4j.fleetmanagement.communication.model.PingMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.io.fs.FileSystemAbstraction;

public class PingService extends AbstractReportingService {
    private final TopologyMapper topologyMapper;

    public PingService(
            Config config,
            FileSystemAbstraction fs,
            ITransactor transactor,
            Upstream upstream,
            ServerIdentity serverIdentity,
            State state,
            Configuration configuration) {
        super(transactor, upstream, state, configuration);
        this.topologyMapper = new TopologyMapper(config, fs, transactor, serverIdentity);
    }

    @Override
    public void report() {
        if (!this.state.isConnected()) {
            // Fleet manager is not connected - skip report
            return;
        }
        String serverVersion;
        String serverId;
        try {
            serverVersion = topologyMapper.getServerVersion();
            serverId = topologyMapper.getServerId();
        } catch (Exception e) {
            this.userLog.error("Fleet manager failed to connect - exception in mapTopology ", e);
            return;
        }
        var projectId = upstream.getApiKey().projectId();
        PingMessage msg = new PingMessage(serverId, serverVersion, projectId);
        ConfigurationResponse configurationResponse = callApi(msg, Upstream.Endpoint.PING, ConfigurationResponse.class);
        updateConfigurationIfPresent(configuration, configurationResponse);
    }

    public static class PingTask extends FleetManagerTask {
        private final PingService pingService;

        public PingTask(State state, ClusterSync clusterSync, PingService pingService) {
            super(state, clusterSync);
            this.pingService = pingService;
        }

        @Override
        protected void execute() {
            if (this.state.isConnected()) {
                this.pingService.report();
            }
        }
    }
}
