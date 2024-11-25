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
package org.neo4j.shell.completions;

import static org.neo4j.shell.util.Versions.version;

import java.util.Optional;
import org.neo4j.driver.Value;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.util.Versions;

public class DbInfoImpl extends DbInfo {
    private static final Logger log = Logger.create();

    final BoltStateHandler boltStateHandler;
    final boolean completionsEnabledByConfig;
    QueryPoller queryPoller;

    private void initializeQueryPoller() {
        this.queryPoller = new QueryPoller(boltStateHandler);
        var fetchDataSummary = new QueryPoller.PollingQuery(QueryPoller.fetchDataSummary, records -> {
            this.labels = records.get(0).get("result").asList(Value::asString);
            this.relationshipTypes = records.get(1).get("result").asList(Value::asString);
            this.propertyKeys = records.get(2).get("result").asList(Value::asString);
        });
        var fetchProcedures = new QueryPoller.PollingQuery(QueryPoller.fetchProcedures, records -> {
            this.procedures =
                    records.stream().map(r -> r.get("name").asString()).toList();
        });
        var fetchFunctions = new QueryPoller.PollingQuery(QueryPoller.fetchFunctions, records -> {
            this.functions = records.stream().map(r -> r.get("name").asString()).toList();
        });
        var fetchDatabases = new QueryPoller.PollingQuery(QueryPoller.fetchDatabases, records -> {
            this.databaseNames =
                    records.stream().map(r -> r.get("name").asString()).toList();
            this.aliasNames = records.stream()
                    .flatMap(r -> r.get("aliases").asList(Value::toString).stream()
                            .map(alias -> {
                                if (alias.startsWith("\"") && alias.endsWith("\"")) {
                                    return alias.substring(1, alias.length() - 1);
                                } else {
                                    return alias;
                                }
                            }))
                    .toList();
        });
        var fetchRoles = new QueryPoller.PollingQuery(QueryPoller.fetchRoles, records -> {
            this.roleNames = records.stream().map(r -> r.get("role").asString()).toList();
        });
        var fetchUsers = new QueryPoller.PollingQuery(QueryPoller.fetchUsers, records -> {
            this.userNames = records.stream().map(r -> r.get("user").asString()).toList();
        });

        queryPoller.startPolling(
                fetchDataSummary, fetchProcedures, fetchFunctions, fetchDatabases, fetchRoles, fetchUsers);
    }

    public DbInfoImpl(
            ParameterService parameterService, BoltStateHandler boltStateHandler, boolean completionsEnabledByConfig) {
        super(parameterService);
        this.completionsEnabledByConfig = completionsEnabledByConfig;
        this.boltStateHandler = boltStateHandler;
        if (completionsEnabled()) {
            initializeQueryPoller();
        }
    }

    @Override
    public boolean completionsEnabled() {
        if (completionsEnabledByConfig
                && versionCompatibleWithCompletions.isEmpty()
                && boltStateHandler.isConnected()) {
            try {
                var serverVersion = version(boltStateHandler.getServerVersion());
                var enableCompletions = serverVersion.compareTo(version("5.0.0")) >= 0;
                versionCompatibleWithCompletions = Optional.of(enableCompletions);
            } catch (Versions.FailedToParseException e) {
                log.warn("Failed to parse server version", e);
            }
        }

        return versionCompatibleWithCompletions.orElse(false);
    }

    @Override
    public void resumePolling() {
        if (queryPoller != null) {
            queryPoller.resumePolling();
        } else if (completionsEnabled()) {
            initializeQueryPoller();
        }
    }

    @Override
    public void stopPolling() {
        if (queryPoller != null) {
            queryPoller.stopPolling();
        }
    }

    @Override
    public void close() throws Exception {
        cleanDbInfo();
        if (queryPoller != null) {
            queryPoller.close();
            queryPoller = null;
        }
    }
}
