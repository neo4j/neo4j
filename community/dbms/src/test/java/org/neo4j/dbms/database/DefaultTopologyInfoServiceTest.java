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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.DatabaseDetails.ROLE_PRIMARY;
import static org.neo4j.dbms.database.DatabaseDetails.TYPE_STANDARD;
import static org.neo4j.dbms.database.DatabaseDetails.TYPE_SYSTEM;
import static org.neo4j.dbms.database.TopologyInfoService.RequestedExtras.ALL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE;
import static org.neo4j.kernel.database.DatabaseId.SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.systemgraph.InstanceModeConstraint;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.Version;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;

class DefaultTopologyInfoServiceTest {
    private final ServerId serverId = new ServerId(UUID.randomUUID());
    private final Config config = Config.defaults(BoltConnector.enabled, Boolean.TRUE);
    private final NamedDatabaseId databaseId = DatabaseIdFactory.from("foo", UUID.randomUUID());

    @Test
    void shouldReturnDatabases() {
        // given
        var stateService = mock(DatabaseStateService.class);
        var databaseState = mock(DatabaseState.class);
        var operatorState = mock(OperatorState.class);
        var status = "Barbie";
        when(operatorState.description()).thenReturn(status);
        when(databaseState.operatorState()).thenReturn(operatorState);
        when(stateService.stateOfDatabase(any())).thenReturn(databaseState);
        when(stateService.causeOfFailure(any())).thenReturn(Optional.empty());

        var extrasProvider = mock(DefaultDatabaseDetailsExtrasProvider.class);
        var systemStoreId = new StoreId(11, 21, "engine", "format", 31, 41);
        var systemExternalStoreId = new ExternalStoreId(UUID.randomUUID());
        var userStoreId = new StoreId(61, 51, "engine", "format", 1, 0);
        var userExternalStoreId = new ExternalStoreId(UUID.randomUUID());
        when(extrasProvider.extraDetails(eq(databaseId.databaseId()), any()))
                .thenReturn(new DatabaseDetailsExtras(
                        Optional.empty(), Optional.of(userStoreId), Optional.of(userExternalStoreId)));
        when(extrasProvider.extraDetails(eq(SYSTEM_DATABASE_ID), any()))
                .thenReturn(new DatabaseDetailsExtras(
                        Optional.empty(), Optional.of(systemStoreId), Optional.of(systemExternalStoreId)));

        var boltAddress = config.get(BoltConnector.advertised_address);
        var service = new DefaultTopologyInfoService(
                serverId, config, stateService, new DefaultReadOnlyDatabases(), extrasProvider);

        // when
        var result = service.databases(null, Set.of(NAMED_SYSTEM_DATABASE_ID, databaseId), ALL);

        // then
        assertThat(result).hasSize(2);
        var systemDetails = result.stream()
                .filter(d -> d.namedDatabaseId().isSystemDatabase())
                .findFirst()
                .orElseThrow();
        assertThat(systemDetails.databaseAccess()).isEqualTo(READ_WRITE);
        assertThat(systemDetails.status()).isEqualTo(status);
        assertThat(systemDetails.statusMessage()).isEmpty();
        assertThat(systemDetails.role()).hasValue(ROLE_PRIMARY);
        assertThat(systemDetails.writer()).isTrue();
        assertThat(systemDetails.actualPrimariesCount()).isOne();
        assertThat(systemDetails.actualSecondariesCount()).isZero();
        assertThat(systemDetails.type()).isEqualTo(TYPE_SYSTEM);
        assertThat(systemDetails.namedDatabaseId()).isEqualTo(NAMED_SYSTEM_DATABASE_ID);
        assertThat(systemDetails.txCommitLag()).hasValue(0L);
        assertThat(systemDetails.lastCommittedTxId()).isEmpty();
        assertThat(systemDetails.storeId()).hasValue(systemStoreId);
        assertThat(systemDetails.externalStoreId()).hasValue(systemExternalStoreId);
        assertThat(systemDetails.serverId()).hasValue(serverId);
        assertThat(systemDetails.boltAddress()).hasValue(boltAddress);

        var userDetails = result.stream()
                .filter(d -> !d.namedDatabaseId().isSystemDatabase())
                .findFirst()
                .orElseThrow();
        assertThat(userDetails.databaseAccess()).isEqualTo(READ_WRITE);
        assertThat(userDetails.status()).isEqualTo(status);
        assertThat(userDetails.statusMessage()).isEmpty();
        assertThat(userDetails.role()).hasValue(ROLE_PRIMARY);
        assertThat(userDetails.writer()).isTrue();
        assertThat(userDetails.actualPrimariesCount()).isOne();
        assertThat(userDetails.actualSecondariesCount()).isZero();
        assertThat(userDetails.type()).isEqualTo(TYPE_STANDARD);
        assertThat(userDetails.namedDatabaseId()).isEqualTo(databaseId);
        assertThat(userDetails.txCommitLag()).hasValue(0L);
        assertThat(userDetails.lastCommittedTxId()).isEmpty();
        assertThat(userDetails.storeId()).hasValue(userStoreId);
        assertThat(userDetails.externalStoreId()).hasValue(userExternalStoreId);
        assertThat(userDetails.serverId()).hasValue(serverId);
        assertThat(userDetails.boltAddress()).hasValue(boltAddress);
    }

    @Test
    void shouldReturnOneServer() {
        // given
        var stateService = mock(DatabaseStateService.class);
        var thirdDatabaseName = "bar";
        when(stateService.stateOfAllDatabases())
                .thenReturn(Map.of(
                        NAMED_SYSTEM_DATABASE_ID,
                        mock(DatabaseState.class),
                        databaseId,
                        mock(DatabaseState.class),
                        DatabaseIdFactory.from(thirdDatabaseName, UUID.randomUUID()),
                        mock(DatabaseState.class)));

        var extrasProvider = mock(DefaultDatabaseDetailsExtrasProvider.class);

        var hostedDatabases = Set.of(SYSTEM_DATABASE_NAME, databaseId.name(), thirdDatabaseName);
        var desiredDatabases = Set.of(SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME);
        var boltAddress = config.get(BoltConnector.advertised_address);
        var service = new DefaultTopologyInfoService(
                serverId, config, stateService, new DefaultReadOnlyDatabases(), extrasProvider);

        // when
        var result = service.servers(null);

        // then
        assertThat(result).hasSize(1);
        var serverDetails = result.iterator().next();
        assertThat(serverDetails.name()).isEqualTo(serverId.uuid().toString());
        assertThat(serverDetails.serverId()).isEqualTo(serverId);
        assertThat(serverDetails.boltAddress()).hasValue(boltAddress);
        assertThat(serverDetails.httpAddress()).isEmpty();
        assertThat(serverDetails.httpsAddress()).isEmpty();
        assertThat(serverDetails.runningState()).isEqualTo(ServerDetails.RunningState.AVAILABLE);
        assertThat(serverDetails.state()).isEqualTo(ServerDetails.State.ENABLED);
        assertThat(serverDetails.modeConstraint()).isEqualTo(InstanceModeConstraint.NONE);
        assertThat(serverDetails.tags()).isEmpty();
        assertThat(serverDetails.deniedDatabases()).isEmpty();
        assertThat(serverDetails.allowedDatabases()).isEmpty();
        assertThat(serverDetails.hostedDatabases()).containsExactlyInAnyOrderElementsOf(hostedDatabases);
        assertThat(serverDetails.desiredDatabases()).containsExactlyInAnyOrderElementsOf(desiredDatabases);
        assertThat(serverDetails.neo4jVersion()).hasValue(Version.getNeo4jVersion());
    }
}
