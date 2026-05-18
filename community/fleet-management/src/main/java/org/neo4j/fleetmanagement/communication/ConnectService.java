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

import static org.neo4j.fleetmanagement.communication.Helpers.responseOk;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.nio.charset.StandardCharsets;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.ConfigurationResponse;
import org.neo4j.fleetmanagement.communication.model.ConnectMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.FleetManagerVersion;
import org.neo4j.io.fs.FileSystemAbstraction;

public class ConnectService extends BaseService {
    private final TopologyMapper topologyMapper;
    private final Upstream upstream;

    public ConnectService(
            Config config,
            FileSystemAbstraction fs,
            ITransactor transactor,
            ServerIdentity serverIdentity,
            Upstream upstream,
            State state,
            Configuration configuration) {
        super(transactor, state, configuration);
        this.topologyMapper = new TopologyMapper(config, fs, transactor, serverIdentity);
        this.upstream = upstream;
    }

    private synchronized void ensureConnected() {
        if (!state.isConnected()) {
            connect();
        }
    }

    public synchronized void disconnect(String errorMessage) {
        this.state.setDisconnected(errorMessage);
    }

    public synchronized void reconnect(String errorMessage) {
        disconnect(errorMessage);
        ensureConnected();
    }

    private void connect() {
        String serverVersion;
        String serverId;
        String dbmsId;
        String fleetManagerVersion;
        boolean isSystemDbWriter;
        try {
            serverVersion = topologyMapper.getServerVersion();
            serverId = topologyMapper.getServerId();
            dbmsId = TopologyMapper.getDbmsId(transactor::getDatabases);
            fleetManagerVersion = FleetManagerVersion.getFleetManagerVersion();
            isSystemDbWriter = topologyMapper.isSystemDbWriter(serverId);
        } catch (Exception e) {
            this.userLog.error("Fleet manager failed to connect - exception in mapTopology: ", e);
            return;
        }

        var apiKey = upstream.getApiKey();
        var projectId = apiKey.projectId();
        ConnectMessage msg = new ConnectMessage(
                serverId, "plugin", dbmsId, serverVersion, projectId, fleetManagerVersion, isSystemDbWriter);
        try {
            String payload = objectMapper.writeValueAsString(msg);
            this.fleetManagerLog.debug("Fleet manager connecting.");
            this.fleetManagerLog.payload("Fleet manager connecting with payload: %s", payload);

            upstream.generateToken();
            Upstream.UpstreamPostRequest upstreamPostRequest = upstream.postTo(Upstream.Endpoint.CONNECT);
            var responseCode = upstreamPostRequest.transmit(payload.getBytes(StandardCharsets.UTF_8));
            byte[] responseBody = upstreamPostRequest.getResponseBody();

            if (responseOk(responseCode) && responseBody != null) {
                this.fleetManagerLog.payload(
                        "Fleet manager connect received response: %s",
                        new String(responseBody, StandardCharsets.UTF_8));
                ConfigurationResponse configurationResponse;
                try {
                    configurationResponse = objectMapper.readValue(responseBody, ConfigurationResponse.class);
                } catch (JsonProcessingException e) {
                    var errorMsg =
                            "Fleet manager encountered an error after connecting to the API - Failed to deserialize configuration message: "
                                    + e.getMessage();
                    this.userLog.warn(errorMsg);
                    this.disconnect(errorMsg);
                    return;
                }

                if (configurationResponse != null) {
                    handleConfigurationResponse(configurationResponse);
                }
                this.state.setConnected();
            } else {
                handleErrorResponse("Fleet manager failed to connect", responseCode, responseBody);
            }
        } catch (JsonProcessingException e) {
            var errorMsg = "Fleet manager failed to connect to the API - Failed to serialize connect message: "
                    + e.getMessage();
            this.userLog.error(errorMsg);
            this.state.setDisconnected(errorMsg);
            throw new RuntimeException(e);
        } catch (ProtocolException e) {
            var errorMsg = "Fleet manager failed to connect to the API - ProtocolException: " + e.getMessage();
            this.userLog.error(errorMsg);
            this.state.setDisconnected(errorMsg);
            throw new RuntimeException(e);
        } catch (MalformedURLException e) {
            var errorMsg = "Fleet manager failed to connect to the API - MalformedURLException: " + e.getMessage();
            this.userLog.error(errorMsg);
            this.state.setDisconnected(errorMsg);
            throw new RuntimeException(e);
        } catch (IOException e) {
            var errorMsg = "Fleet manager failed to connect to the API - IOException: " + e.getMessage();

            if (upstream.isReachable()) {
                errorMsg = "Fleet manager failed to connect to the API but API is confirmed reachable - IOException: "
                        + e.getMessage();
            }

            this.userLog.error(errorMsg);
            this.state.setDisconnected(errorMsg);
            throw new RuntimeException(e);
        }
    }

    private void handleConfigurationResponse(ConfigurationResponse configurationResponse)
            throws IOException, IllegalArgumentException {
        updateRotatedTokenIfPresent(configurationResponse);
        Configuration.updateConfigurationIfPresent(configuration, configurationResponse);
    }

    private void updateRotatedTokenIfPresent(ConfigurationResponse configurationResponse) throws IOException {
        if (configurationResponse.newToken() != null
                && !configurationResponse.newToken().isEmpty()) {
            this.transactor.setToken(configurationResponse.newToken());
            this.upstream.setToken(configurationResponse.newToken());
            this.state.setRotatingToken(false);
        }
    }

    public static class ConnectServiceTask extends FleetManagerTask {
        private final ConnectService connectService;

        public ConnectServiceTask(State state, ClusterSync clusterSync, ConnectService connectService) {
            super(state, clusterSync);
            this.connectService = connectService;
        }

        protected void execute() {
            if (this.state.isActive() && !this.state.isRotatingToken()) {
                connectService.ensureConnected();
            }
        }
    }
}
