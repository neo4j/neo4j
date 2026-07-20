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

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.ReportingMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.metrics.ServerMetadata;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.topology.model.Dbms;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.io.fs.FileSystemAbstraction;

public class TopologyService extends AbstractReportingService {

    private final TopologyMapper topologyMapper;

    public TopologyService(
            Config config,
            FileSystemAbstraction fs,
            ITransactor transactor,
            ServerIdentity serverIdentity,
            Upstream upstream,
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

        Dbms dbmsData;
        try {
            dbmsData = topologyMapper.mapTopology();
        } catch (Exception e) {
            this.userLog.error("Fleet manager failed to report - exception in mapTopology ", e);
            return;
        }

        ReportingMessage msg = new ReportingMessage();
        try {
            ServerMetadata.getInstance().populateStaticInfo(msg);
        } catch (Exception e) {
            this.userLog.error("Fleet manager failed to collect static information: " + e.getMessage());
            throw new RuntimeException(e);
        }
        msg.dbms = dbmsData;
        msg.projectId = upstream.getApiKey().projectId();

        transmitReport(msg, Upstream.Endpoint.REPORTING);

        if (!this.state.isTopologyInitialized()) {
            this.state.setTopologyInitialized(true);
        }
    }

    public static class TopologyReportingTask extends FleetManagerTask {
        private final TopologyService topologyService;

        public TopologyReportingTask(State state, ClusterSync clusterSync, TopologyService topologyService) {
            super(state, clusterSync);
            this.topologyService = topologyService;
        }

        @Override
        protected void execute() {
            if (this.state.isConnected()) {
                this.topologyService.report();
            }
        }
    }
}
