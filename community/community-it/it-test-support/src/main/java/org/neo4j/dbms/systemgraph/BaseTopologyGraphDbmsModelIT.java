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
package org.neo4j.dbms.systemgraph;

import static java.time.Duration.ofSeconds;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_MAX_LIFETIME;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_ACQUISITION_TIMEOUT;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_IDLE_TEST;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_MAX_SIZE;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_TIMEOUT;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.LOGGING_LEVEL;
import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.SSL_ENFORCED;
import static org.neo4j.dbms.systemgraph.InstanceModeConstraint.PRIMARY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.ALIAS_PROPERTIES_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.CONNECTS_WITH_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_CREATED_AT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DESIGNATED_SEEDER_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEED_CONFIG_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEED_CREDENTIALS_ENCRYPTED_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEED_CREDENTIALS_IV_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SHARD_COUNT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STORE_FORMAT_NEW_DB_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STORE_RANDOM_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UPDATE_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_VIRTUAL_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DELETED_DATABASE_DUMP_DATA_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DELETED_DATABASE_KEEP_DATA_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DELETED_DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DRIVER_SETTINGS_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseStatus.OFFLINE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HOSTED_ON_BOOTSTRAPPER_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HOSTED_ON_MODE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HOSTED_ON_RAFT_MEMBER_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HOSTED_ON_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_DISCOVERED_AT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_MODE_CONSTRAINT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_STATUS_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus.ENABLED;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.LATEST_SUPPORTED_COMPONENT_VERSIONS_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PROPERTIES_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.REMOVED_INSTANCE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.SUPPORTED_COMPONENT_VERSIONS_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGET_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_ALLOCATOR_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_AUTO_ENABLE_FREE_SERVERS_FLAG;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PRIMARIES_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_SECONDARIES_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.URL_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.VERSION_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.WAS_HOSTED_ON_RELATIONSHIP;
import static org.neo4j.values.storable.DurationValue.duration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Level;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public abstract class BaseTopologyGraphDbmsModelIT {
    @Inject
    protected DatabaseManagementService managementService;

    @Inject
    protected GraphDatabaseService db;

    protected Transaction tx;

    @BeforeEach
    void before() {
        tx = db.beginTx();
        createModel(tx);
    }

    @AfterEach
    void after() {
        tx.commit();
        tx.close();
    }

    protected abstract void createModel(Transaction tx);

    protected ServerId newInstance(Consumer<InstanceNodeBuilder> setup) {
        return newInstance(setup, false);
    }

    protected ServerId newRemovedInstance(Consumer<InstanceNodeBuilder> setup) {
        return newInstance(setup, true);
    }

    protected NamedDatabaseId newDatabase(Consumer<DatabaseNodeBuilder> setup) {
        return newDatabase(setup, false);
    }

    protected NamedDatabaseId newDeletedDatabase(Consumer<DatabaseNodeBuilder> setup) {
        return newDatabase(setup, true);
    }

    public static ServerId serverId(int seed) {
        var rng = new Random(seed);
        return new ServerId(new UUID(rng.nextLong(), rng.nextLong()));
    }

    public static Set<ServerId> serverIds(int from, int until) {
        return IntStream.range(from, until)
                .mapToObj(BaseTopologyGraphDbmsModelIT::serverId)
                .collect(Collectors.toSet());
    }

    protected void connect(
            NamedDatabaseId databaseId,
            ServerId serverId,
            TopologyGraphDbmsModel.HostedOnMode mode,
            boolean bootstrapper,
            boolean wasHostedOn) {
        connect(
                databaseId,
                serverId,
                mode,
                bootstrapper,
                wasHostedOn,
                mode == HostedOnMode.RAFT ? UUID.randomUUID() : null);
    }

    protected void connect(
            NamedDatabaseId databaseId,
            ServerId serverId,
            TopologyGraphDbmsModel.HostedOnMode mode,
            boolean bootstrapper,
            boolean wasHostedOn,
            UUID raftMemberId) {
        try (var tx = db.beginTx()) {
            var database = findDatabase(databaseId, tx);
            var instance = findInstance(serverId, tx);
            var relationship = mergeHostOn(wasHostedOn, database, instance);
            relationship.setProperty(HOSTED_ON_MODE_PROPERTY, mode.modeName());
            if (bootstrapper) {
                relationship.setProperty(HOSTED_ON_BOOTSTRAPPER_PROPERTY, true);
            }
            if (mode == HostedOnMode.RAFT && raftMemberId != null) {
                relationship.setProperty(HOSTED_ON_RAFT_MEMBER_ID_PROPERTY, raftMemberId.toString());
            }
            tx.commit();
        }
    }

    private Relationship mergeHostOn(boolean wasHostedOn, Node database, Node instance) {
        try (Stream<Relationship> stream =
                database
                        .getRelationships(Direction.OUTGOING, HOSTED_ON_RELATIONSHIP, WAS_HOSTED_ON_RELATIONSHIP)
                        .stream()) {
            stream.filter(rel -> Objects.equals(rel.getEndNode(), instance)).forEach(Relationship::delete);
        }
        var nextRelLabel = wasHostedOn ? WAS_HOSTED_ON_RELATIONSHIP : HOSTED_ON_RELATIONSHIP;
        return database.createRelationshipTo(instance, nextRelLabel);
    }

    protected void disconnect(NamedDatabaseId databaseId, ServerId serverId, boolean replaceWithWas) {
        try (var tx = db.beginTx()) {
            var database = findDatabase(databaseId, tx);
            var instance = findInstance(serverId, tx);

            try (Stream<Relationship> relationships = database.getRelationships(HOSTED_ON_RELATIONSHIP).stream()) {
                relationships.filter(rel -> rel.getEndNode().equals(instance)).forEach(rel -> {
                    if (replaceWithWas) {
                        var was = database.createRelationshipTo(instance, WAS_HOSTED_ON_RELATIONSHIP);
                        was.setProperty(HOSTED_ON_MODE_PROPERTY, rel.getProperty(HOSTED_ON_MODE_PROPERTY));
                    }
                    rel.delete();
                });
            }
            tx.commit();
        }
    }

    protected void databaseDelete(NamedDatabaseId id) {
        try (var tx = db.beginTx()) {
            var database = findDatabase(id, tx);
            database.setProperty(
                    DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty(DATABASE_UPDATE_ID_PROPERTY)) + 1L);
            database.addLabel(DELETED_DATABASE_LABEL);
            database.removeLabel(DATABASE_LABEL);
            tx.commit();
        }
    }

    protected void databaseSetState(NamedDatabaseId id, TopologyGraphDbmsModel.DatabaseStatus state) {
        try (var tx = db.beginTx()) {
            var database = findDatabase(id, tx);
            database.setProperty(
                    DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty(DATABASE_UPDATE_ID_PROPERTY)) + 1L);
            database.setProperty(DATABASE_STATUS_PROPERTY, state.statusName());
            tx.commit();
        }
    }

    protected void databaseIncreaseUpdateId(NamedDatabaseId... ids) {
        try (var tx = db.beginTx()) {
            for (NamedDatabaseId id : ids) {
                var database = findDatabase(id, tx);
                database.setProperty(
                        DATABASE_UPDATE_ID_PROPERTY, ((long) database.getProperty(DATABASE_UPDATE_ID_PROPERTY)) + 1L);
            }
            tx.commit();
        }
    }

    private NamedDatabaseId newDatabase(Consumer<DatabaseNodeBuilder> setup, boolean deleted) {
        try (var tx = db.beginTx()) {
            var builder = new DatabaseNodeBuilder(tx, deleted);
            setup.accept(builder);
            return builder.commit();
        }
    }

    private ServerId newInstance(Consumer<InstanceNodeBuilder> setup, boolean removed) {
        try (var tx = db.beginTx()) {
            var builder = new InstanceNodeBuilder(tx, removed);
            setup.accept(builder);
            return builder.commit();
        }
    }

    private Node findInstance(ServerId serverId, Transaction tx) {
        return Optional.ofNullable(tx.findNode(
                        INSTANCE_LABEL, INSTANCE_UUID_PROPERTY, serverId.uuid().toString()))
                .orElseGet(() -> tx.findNode(
                        REMOVED_INSTANCE_LABEL,
                        DATABASE_UUID_PROPERTY,
                        serverId.uuid().toString()));
    }

    private Node findDatabase(NamedDatabaseId databaseId, Transaction tx) {
        return Optional.ofNullable(tx.findNode(
                        DATABASE_LABEL,
                        DATABASE_UUID_PROPERTY,
                        databaseId.databaseId().uuid().toString()))
                .orElseGet(() -> tx.findNode(
                        DELETED_DATABASE_LABEL,
                        DATABASE_UUID_PROPERTY,
                        databaseId.databaseId().uuid().toString()));
    }

    protected static class DatabaseNodeBuilder {
        Transaction tx;
        Node node;

        public DatabaseNodeBuilder(Transaction tx, boolean deleted) {
            this.tx = tx;
            node = tx.createNode(deleted ? DELETED_DATABASE_LABEL : DATABASE_LABEL);
        }

        public DatabaseNodeBuilder withDatabase(String databaseName) {
            return withDatabase(DatabaseIdFactory.from(databaseName, UUID.randomUUID()));
        }

        public DatabaseNodeBuilder withDatabase(NamedDatabaseId namedDatabaseId) {
            node.setProperty(DATABASE_NAME_PROPERTY, namedDatabaseId.name());
            node.setProperty(
                    DATABASE_UUID_PROPERTY, namedDatabaseId.databaseId().uuid().toString());
            node.setProperty(DATABASE_STATUS_PROPERTY, TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName());
            node.setProperty(DATABASE_UPDATE_ID_PROPERTY, 0L);
            return this;
        }

        public DatabaseNodeBuilder withStoreFormat(String storeFormat) {
            node.setProperty(DATABASE_STORE_FORMAT_NEW_DB_PROPERTY, storeFormat);
            return this;
        }

        public DatabaseNodeBuilder asStopped() {
            node.setProperty(DATABASE_STATUS_PROPERTY, OFFLINE.statusName());
            return this;
        }

        public DatabaseNodeBuilder asDefault() {
            node.setProperty(DATABASE_DEFAULT_PROPERTY, true);
            return this;
        }

        public DatabaseNodeBuilder asVirtual() {
            node.addLabel(COMPOSITE_DATABASE_LABEL);
            node.setProperty(DATABASE_VIRTUAL_PROPERTY, true);
            return this;
        }

        public DatabaseNodeBuilder withStoreIdParts(long creationTime, long random) {
            node.setProperty(
                    DATABASE_CREATED_AT_PROPERTY,
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationTime), ZoneId.systemDefault()));
            node.setProperty(DATABASE_STORE_RANDOM_ID_PROPERTY, random);
            return this;
        }

        public DatabaseNodeBuilder withAllocationNumbers(int primaries, int secondaries) {
            node.setProperty(DATABASE_PRIMARIES_PROPERTY, primaries);
            node.setProperty(DATABASE_SECONDARIES_PROPERTY, secondaries);
            return this;
        }

        public DatabaseNodeBuilder withDesignatedSeeder(ServerId designatedSeeder) {
            node.setProperty(
                    DATABASE_DESIGNATED_SEEDER_PROPERTY, designatedSeeder.uuid().toString());
            return this;
        }

        public DatabaseNodeBuilder withSeedingServers(Set<ServerId> seedingServers) {
            String[] servers = seedingServers.stream()
                    .map(serverId -> serverId.uuid().toString())
                    .toArray(String[]::new);
            node.setProperty(DATABASE_SEEDING_SERVERS_PROPERTY, servers);
            return this;
        }

        public DatabaseNodeBuilder withDeletedDatabaseKeepProperty(Set<ServerId> serversKeepingData) {
            node.setProperty(
                    DELETED_DATABASE_KEEP_DATA_PROPERTY,
                    serversKeepingData.stream().map(id -> id.uuid().toString()).toArray(String[]::new));
            return this;
        }

        public DatabaseNodeBuilder withDumpDatabaseProperty() {
            node.setProperty(DELETED_DATABASE_DUMP_DATA_PROPERTY, true);
            return this;
        }

        public DatabaseNodeBuilder withSeedingParameters(String uri, byte[] password, byte[] iv, String config) {
            node.setProperty(DATABASE_SEED_URI_PROPERTY, uri);
            node.setProperty(DATABASE_SEED_CREDENTIALS_ENCRYPTED_PROPERTY, password);
            node.setProperty(DATABASE_SEED_CREDENTIALS_IV_PROPERTY, iv);
            node.setProperty(DATABASE_SEED_CONFIG_PROPERTY, config);
            return this;
        }

        public DatabaseNodeBuilder withShardCount(int shardCount) {
            node.setProperty(DATABASE_SHARD_COUNT_PROPERTY, shardCount);
            return this;
        }

        public NamedDatabaseId commit() {
            var id = DatabaseIdFactory.from((String) node.getProperty(DATABASE_NAME_PROPERTY), UUID.fromString((String)
                    node.getProperty(DATABASE_UUID_PROPERTY)));
            tx.commit();
            return id;
        }
    }

    protected static class InstanceNodeBuilder {
        Transaction tx;
        Node node;

        public InstanceNodeBuilder(Transaction tx, boolean removed) {
            this.tx = tx;
            node = tx.createNode(removed ? REMOVED_INSTANCE_LABEL : INSTANCE_LABEL);
        }

        public InstanceNodeBuilder withInstance() {
            return withInstance(new ServerId(UUID.randomUUID()));
        }

        public InstanceNodeBuilder withInstance(ServerId serverId) {
            node.setProperty(INSTANCE_UUID_PROPERTY, serverId.uuid().toString());
            node.setProperty(INSTANCE_MODE_CONSTRAINT_PROPERTY, PRIMARY.name());
            node.setProperty(INSTANCE_STATUS_PROPERTY, ENABLED.name());
            return this;
        }

        public InstanceNodeBuilder withInstance(ServerId serverId, String name) {
            node.setProperty(INSTANCE_UUID_PROPERTY, serverId.uuid().toString());
            node.setProperty(INSTANCE_MODE_CONSTRAINT_PROPERTY, PRIMARY.name());
            node.setProperty(INSTANCE_STATUS_PROPERTY, ENABLED.name());
            node.setProperty(INSTANCE_NAME_PROPERTY, name);
            return this;
        }

        public InstanceNodeBuilder withModeConstraint(InstanceModeConstraint modeConstraint) {
            node.setProperty(INSTANCE_MODE_CONSTRAINT_PROPERTY, modeConstraint.name());
            return this;
        }

        public InstanceNodeBuilder withComponentVersions(Map<SystemGraphComponent.Name, Integer> versions) {
            var versionsNode = tx.createNode(SUPPORTED_COMPONENT_VERSIONS_LABEL);
            node.createRelationshipTo(versionsNode, LATEST_SUPPORTED_COMPONENT_VERSIONS_RELATIONSHIP);
            versions.forEach((key, value) -> {
                versionsNode.setProperty(key.name(), value);
            });
            return this;
        }

        public InstanceNodeBuilder asDeallocating() {
            node.setProperty(INSTANCE_STATUS_PROPERTY, TopologyGraphDbmsModel.InstanceStatus.DEALLOCATING.name());
            return this;
        }

        public ServerId commit() {
            node.setProperty(INSTANCE_DISCOVERED_AT_PROPERTY, ZonedDateTime.now());
            var id = new ServerId(UUID.fromString((String) node.getProperty(INSTANCE_UUID_PROPERTY)));
            tx.commit();
            return id;
        }
    }

    protected Node createInternalReferenceForDatabase(
            Transaction tx, String name, boolean primary, NamedDatabaseId databaseId) {
        return createInternalReferenceForDatabase(tx, DEFAULT_NAMESPACE, name, primary, databaseId);
    }

    protected Node createInternalReferenceForDatabase(
            Transaction tx, String namespace, String name, boolean primary, NamedDatabaseId databaseId) {
        var databaseNode = findDatabase(databaseId, tx);
        var referenceNode = tx.createNode(DATABASE_NAME_LABEL);
        referenceNode.setProperty(PRIMARY_PROPERTY, primary);
        referenceNode.setProperty(DATABASE_NAME_PROPERTY, name);
        referenceNode.setProperty(NAMESPACE_PROPERTY, namespace);
        referenceNode.createRelationshipTo(databaseNode, TARGETS_RELATIONSHIP);
        return referenceNode;
    }

    protected Node createExternalReferenceForDatabase(
            Transaction tx, String name, String targetName, RemoteUri uri, UUID uuid) {
        var referenceNode = tx.createNode(REMOTE_DATABASE_LABEL, DATABASE_NAME_LABEL);
        referenceNode.setProperty(PRIMARY_PROPERTY, false);
        referenceNode.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
        referenceNode.setProperty(DATABASE_NAME_PROPERTY, name);
        referenceNode.setProperty(TARGET_NAME_PROPERTY, targetName);
        var uriString =
                String.format("%s://%s", uri.getScheme(), uri.getAddresses().get(0));
        referenceNode.setProperty(URL_PROPERTY, uriString);
        referenceNode.setProperty(VERSION_PROPERTY, uuid.toString());
        return referenceNode;
    }

    protected Node createExternalReferenceForDatabase(
            Transaction tx, String namespace, String name, String targetName, RemoteUri uri, UUID uuid) {
        var referenceNode = createExternalReferenceForDatabase(tx, name, targetName, uri, uuid);
        referenceNode.setProperty(NAMESPACE_PROPERTY, namespace);
        return referenceNode;
    }

    protected Node createDriverSettingsForExternalAlias(
            Transaction tx, Node externalRefNode, DriverSettings driverSettings) {
        var settingsNode = tx.createNode(DRIVER_SETTINGS_LABEL);
        driverSettings.isSslEnforced().ifPresent(enabled -> settingsNode.setProperty(SSL_ENFORCED.toString(), enabled));
        driverSettings
                .connectionTimeout()
                .ifPresent(timeout -> settingsNode.setProperty(CONNECTION_TIMEOUT.toString(), timeout));
        driverSettings
                .connectionMaxLifetime()
                .ifPresent(lifetime -> settingsNode.setProperty(CONNECTION_MAX_LIFETIME.toString(), lifetime));
        driverSettings
                .connectionPoolAcquisitionTimeout()
                .ifPresent(
                        timeout -> settingsNode.setProperty(CONNECTION_POOL_ACQUISITION_TIMEOUT.toString(), timeout));
        driverSettings
                .connectionPoolIdleTest()
                .ifPresent(test -> settingsNode.setProperty(CONNECTION_POOL_IDLE_TEST.toString(), test));
        driverSettings
                .connectionPoolMaxSize()
                .ifPresent(size -> settingsNode.setProperty(CONNECTION_POOL_MAX_SIZE.toString(), size));
        driverSettings
                .loggingLevel()
                .ifPresent(level -> settingsNode.setProperty(LOGGING_LEVEL.toString(), level.toString()));
        externalRefNode.createRelationshipTo(settingsNode, CONNECTS_WITH_RELATIONSHIP);
        return settingsNode;
    }

    protected Node createPropertiesForAlias(Transaction tx, Node aliasNode, Map<String, Object> properties) {
        var propertiesNode = tx.createNode(ALIAS_PROPERTIES_LABEL);
        properties.forEach(propertiesNode::setProperty);
        aliasNode.createRelationshipTo(propertiesNode, PROPERTIES_RELATIONSHIP);
        return propertiesNode;
    }

    protected static Stream<Arguments> aliasProperties() {
        return Stream.of(Arguments.of(Map.of()), Arguments.of(Map.of("key1", "string", "key2", 123L)));
    }

    protected static Stream<Arguments> driverSettings() {
        var completeSettings = DriverSettings.builder()
                .withSslEnforced(true)
                .withConnectionTimeout(duration(ofSeconds(10)))
                .withConnectionPoolAcquisitionTimeout(duration(ofSeconds(1)))
                .withConnectionMaxLifeTime(duration(ofSeconds(300)))
                .withConnectionPoolIdleTest(duration(ofSeconds(1)))
                .withConnectionPoolMaxSize(0)
                .withLoggingLevel(Level.INFO)
                .build();

        var missingSettings = DriverSettings.builder()
                .withSslEnforced(false)
                .withLoggingLevel(Level.DEBUG)
                .build();

        var missingOtherSettings = DriverSettings.builder()
                .withConnectionTimeout(duration(ofSeconds(10)))
                .withConnectionPoolAcquisitionTimeout(duration(ofSeconds(1)))
                .withConnectionMaxLifeTime(duration(ofSeconds(300)))
                .withConnectionPoolIdleTest(duration(ofSeconds(1)))
                .build();

        return Stream.of(
                Arguments.of(completeSettings), Arguments.of(missingSettings), Arguments.of(missingOtherSettings));
    }

    protected void createVersionNode() {
        try (var tx = this.db.beginTx()) {
            var versionNode = tx.createNode(VERSION_LABEL);
            versionNode.setProperty(DBMS_RUNTIME_COMPONENT.name(), DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion());
            var configNode = tx.createNode(TOPOLOGY_GRAPH_CONFIG_LABEL);
            configNode.setProperty(TOPOLOGY_GRAPH_CONFIG_ALLOCATOR_PROPERTY, "foo");
            configNode.setProperty(TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PRIMARIES_PROPERTY, 9L);
            configNode.setProperty(TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_SECONDARIES_PROPERTY, 10L);
            configNode.setProperty(TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_PROPERTY, "bar");
            configNode.setProperty(TOPOLOGY_GRAPH_CONFIG_AUTO_ENABLE_FREE_SERVERS_FLAG, false);
            tx.commit();
        }
    }
}
