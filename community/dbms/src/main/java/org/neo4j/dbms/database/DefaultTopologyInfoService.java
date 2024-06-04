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
package org.neo4j.dbms.database;

import static org.neo4j.dbms.database.DatabaseDetails.ROLE_PRIMARY;
import static org.neo4j.dbms.database.DatabaseDetails.TYPE_STANDARD;
import static org.neo4j.dbms.database.DatabaseDetails.TYPE_SYSTEM;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess.READ_ONLY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.systemgraph.InstanceModeConstraint;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.Version;

public class DefaultTopologyInfoService implements TopologyInfoService {
    private final ReadOnlyDatabases readOnlyDatabases;
    private final ServerId serverId;
    private final Config config;
    private final DatabaseStateService stateService;
    private final DefaultDatabaseDetailsExtrasProvider databaseDetailsExtrasProvider;
    private final SocketAddress fixBoltAddress;

    public DefaultTopologyInfoService(
            ServerId serverId,
            Config config,
            DatabaseStateService stateService,
            ReadOnlyDatabases readOnlyDatabases,
            DefaultDatabaseDetailsExtrasProvider databaseDetailsExtrasProvider) {
        this.serverId = serverId;
        this.config = config;
        this.stateService = stateService;
        this.readOnlyDatabases = readOnlyDatabases;
        this.databaseDetailsExtrasProvider = databaseDetailsExtrasProvider;
        this.fixBoltAddress = config.get(BoltConnector.advertised_address);
    }

    @Override
    public Set<ServerDetails> servers(Transaction transaction) {
        var desiredDatabases = Set.of(
                NamedDatabaseId.SYSTEM_DATABASE_NAME, config.get(GraphDatabaseSettings.initial_default_database));
        var hostedDatabases = stateService.stateOfAllDatabases().keySet().stream()
                .map(NamedDatabaseId::name)
                .collect(Collectors.toSet());
        return Set.of(new ServerDetails(
                serverId,
                serverId.uuid().toString(),
                address(BoltConnector.enabled, BoltConnector.advertised_address),
                address(HttpConnector.enabled, HttpConnector.advertised_address),
                address(HttpsConnector.enabled, HttpsConnector.advertised_address),
                Set.of(),
                ServerDetails.State.ENABLED,
                ServerDetails.RunningState.AVAILABLE,
                hostedDatabases,
                desiredDatabases,
                Set.of(),
                Set.of(),
                InstanceModeConstraint.NONE,
                Optional.of(Version.getNeo4jVersion())));
    }

    @Override
    public Set<DatabaseDetails> databases(
            Transaction transaction, Set<NamedDatabaseId> databaseIds, RequestedExtras requestedExtras) {
        return databaseIds.stream().map(id -> database(id, requestedExtras)).collect(Collectors.toSet());
    }

    private DatabaseDetails database(NamedDatabaseId id, RequestedExtras detailsLevel) {
        var extraDetails = databaseDetailsExtrasProvider.extraDetails(
                id.databaseId(), new RequestedExtras(false, detailsLevel.storeInfo()));
        return new DatabaseDetails(
                Optional.of(serverId),
                readOnlyDatabases.isReadOnly(id.databaseId()) ? READ_ONLY : READ_WRITE,
                Optional.of(fixBoltAddress),
                Optional.of(ROLE_PRIMARY),
                true,
                stateService.stateOfDatabase(id).operatorState().description(),
                stateService.causeOfFailure(id).map(Throwable::getMessage).orElse(""),
                Optional.empty(),
                Optional.of(0L),
                id,
                id.isSystemDatabase() ? TYPE_SYSTEM : TYPE_STANDARD,
                Collections.emptyMap(),
                extraDetails.storeId(),
                extraDetails.externalStoreId(),
                1,
                0);
    }

    private Optional<SocketAddress> address(Setting<Boolean> enabled, Setting<SocketAddress> advertisedAddress) {
        return Optional.ofNullable(config.get(enabled) ? config.get(advertisedAddress) : null);
    }
}
