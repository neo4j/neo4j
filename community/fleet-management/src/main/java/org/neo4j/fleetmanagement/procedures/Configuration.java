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
package org.neo4j.fleetmanagement.procedures;

import static org.neo4j.fleetmanagement.common.TransactionUtil.withTransactionAndErrorHandling;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.utils.TokenUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class Configuration {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseAPI db;

    @Context
    public State state;

    @Context
    public Log log;

    public static class Result {
        @Description("A message indicating the result of the operation.")
        public String result;

        public Result(String result) {
            this.result = result;
        }
    }

    public static class StatusResult {
        @Description("Is Fleet Manager active?")
        public boolean active;

        @Description("Is Fleet Manager connected?")
        public boolean connected;

        @Description("Error message if Fleet Manager is not connected")
        public String statusMessage;

        public StatusResult(boolean active, boolean connected, String message) {
            this.active = active;
            this.connected = connected;
            this.statusMessage = message;
        }
    }

    public static class TokenInspectResult {
        @Description("ID of the Aura project the Deployment token belongs to.")
        public String projectId;

        @Description("Token expiry")
        public String tokenExpires;

        @Description("Error message if token inspection fails.")
        public String errorMessage;

        public TokenInspectResult(String projectId, String expiry, String errorMessage) {
            this.projectId = projectId;
            this.tokenExpires = expiry;
            this.errorMessage = errorMessage;
        }
    }

    public static class TokenRegistrationResult extends TokenInspectResult {
        @Description("Token registration status")
        public String status;

        public TokenRegistrationResult(String projectId, String expiry, String errorMessage, String status) {
            super(projectId, expiry, errorMessage);
            this.status = status;
        }
    }

    private void ensureSystemDb() {
        if (!Objects.equals(db.databaseName(), "system")) {
            throw new RuntimeException("This procedure can only be run on the system database");
        }
    }

    @Procedure(name = "fleetManagement.registerToken", mode = Mode.DBMS)
    @SystemProcedure
    @Admin
    @Description("Add a token for authenticating to Fleet Manager")
    public Stream<Result> registerToken(
            @Name(value = "token", description = "A token obtained from Neo4j Aura.") String token) {
        var active = getTokenStatus();
        var connected = state.isConnected();

        ensureSystemDb();
        return withTransactionAndErrorHandling(
                db,
                tx -> {
                    Optional<Node> maybeNode = tx.findNodes(Label.label("FleetManagementConfiguration")).stream()
                            .findFirst();
                    if (maybeNode.isPresent() && maybeNode.get().hasProperty("token")) {
                        if (active) {
                            state.setActive(false);
                        }

                        if (connected) {
                            state.setDisconnected("Disconnecting before registering a new token");
                        }
                    }

                    Node node = maybeNode.orElseGet(() -> tx.createNode(Label.label("FleetManagementConfiguration")));

                    node.setProperty("token", token);
                    tx.commit();

                    state.setActive(true);
                    var apiKey = TokenUtils.parseToken(token);
                    return Stream.of(
                            new Result("Token registered successfully to monitor this deployment for Project with ID: `"
                                    + apiKey.projectId() + "`"));
                },
                e -> {
                    String message = "An error occurred while registering the token: " + e.getMessage();
                    log.error(message, e);
                    return Stream.of(new Result(message));
                });
    }

    @Procedure(name = "fleetManagement.inspectToken", mode = Mode.DBMS)
    @SystemProcedure
    @Admin
    @Description("Inspect a Fleet Manager access token.")
    public Stream<TokenInspectResult> inspectToken(
            @Name(
                            value = "token",
                            defaultValue = "",
                            description =
                                    "(optional) A token obtained from Neo4j Aura. Will use the currently registered token, if not provided.")
                    String token) {
        if (token == null || token.isEmpty()) {
            ensureSystemDb();
            return withTransactionAndErrorHandling(
                    db,
                    tx -> {
                        Optional<Node> maybeNode = tx.findNodes(Label.label("FleetManagementConfiguration")).stream()
                                .findFirst();
                        if (maybeNode.isEmpty()) {
                            return Stream.of(new TokenInspectResult(null, null, "Provide a token input to inspect"));
                        }

                        Node node = maybeNode.get();

                        var registeredToken = (String) node.getProperty("token");
                        var apiKey = TokenUtils.parseToken(registeredToken);
                        return Stream.of(new TokenInspectResult(
                                apiKey.projectId(), apiKey.expiryTime().toString(), null));
                    },
                    e -> {
                        String message = "An error occurred while inspecting the token: " + e.getMessage();
                        log.error(message, e);
                        return Stream.of(new TokenInspectResult(null, null, message));
                    });
        } else {
            try {
                var apiKey = TokenUtils.parseToken(token);
                return Stream.of(new TokenInspectResult(
                        apiKey.projectId(), apiKey.expiryTime().toString(), null));
            } catch (IOException e) {
                String message = "An error occurred while inspecting the token: " + e.getMessage();
                log.error(message, e);
                return Stream.of(new TokenInspectResult(null, null, message));
            }
        }
    }

    @Procedure(name = "fleetManagement.disable", mode = Mode.DBMS)
    @SystemProcedure
    @Admin
    @Description("Disable Fleet Manager")
    public Stream<Result> disable() {
        ensureSystemDb();
        return withTransactionAndErrorHandling(
                db,
                tx -> {
                    Optional<Node> maybeNode = tx.findNodes(Label.label("FleetManagementConfiguration")).stream()
                            .findFirst();
                    maybeNode.ifPresent(Node::delete);
                    tx.commit();

                    this.state.setActive(false);
                    return Stream.of(new Result("Fleet Manager has been disabled"));
                },
                e -> {
                    String message = "An error occurred while disabling Fleet Manager: " + e.getMessage();
                    log.error(message, e);
                    return Stream.of(new Result(message));
                });
    }

    @Procedure(name = "fleetManagement.status", mode = Mode.READ)
    @SystemProcedure
    @Admin
    @Description("Check the status of Fleet Manager.")
    public Stream<StatusResult> status() {
        var active = getTokenStatus();
        var connected = state.isConnected();
        String errorMessage;
        if (!active && !connected && state.getConnectionMessage() == null) {
            errorMessage = "Fleet Manager is disabled. Use the procedure fleetManagement.registerToken to get started.";
        } else {
            errorMessage = state.getConnectionMessage();
        }
        return Stream.of(new StatusResult(active, connected, errorMessage));
    }

    @Procedure(name = "fleetManagement.restart", mode = Mode.DBMS)
    @SystemProcedure
    @Admin
    @Description("Restart Fleet Manager.")
    public Stream<Result> restart() {
        var hasToken = getTokenStatus();
        var connected = state.isConnected();

        state.setActive(false);
        if (hasToken) {
            if (connected) {
                state.setDisconnected("Disconnecting before restarting Fleet Manager");
            }

            state.setActive(true);
            return Stream.of(new Result("Fleet Manager is restarting"));
        } else {
            return Stream.of(new Result("Register a token to enable Fleet Manager"));
        }
    }

    private boolean getTokenStatus() {
        return withTransactionAndErrorHandling(
                db,
                tx -> {
                    Optional<Node> maybeNode = tx.findNodes(Label.label("FleetManagementConfiguration")).stream()
                            .findFirst();
                    Boolean status =
                            maybeNode.map(node -> node.hasProperty("token")).orElse(false);
                    tx.commit();
                    return status;
                },
                e -> {
                    String message = "An error occurred while fetching Fleet Manager token status: " + e.getMessage();
                    log.error(message, e);
                    return false;
                });
    }
}
