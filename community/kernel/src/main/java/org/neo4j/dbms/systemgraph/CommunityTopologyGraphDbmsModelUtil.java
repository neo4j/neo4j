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

import static org.neo4j.dbms.systemgraph.DriverSettings.Keys.CONNECTION_POOL_ACQUISITION_TIMEOUT;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceImpl.Internal;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.Level;
import org.neo4j.values.storable.DurationValue;

public final class CommunityTopologyGraphDbmsModelUtil {
    private CommunityTopologyGraphDbmsModelUtil() {}

    static Stream<Internal> getAllPrimaryStandardDatabaseReferencesInRoot(Transaction tx) {
        return tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).stream()
                .filter(node -> !node.hasProperty(TopologyGraphDbmsModel.DATABASE_VIRTUAL_PROPERTY))
                .map(node -> {
                    var databaseId = getDatabaseId(node);
                    var alias = new NormalizedDatabaseName(databaseId.name());
                    var ref = new Internal(alias, databaseId, true);
                    return ref.maybeAsShard(node.getDegree(TopologyGraphDbmsModel.HAS_SHARD, Direction.INCOMING) > 0);
                });
    }

    static NormalizedDatabaseName getNameProperty(String labelName, Node node) {
        return new NormalizedDatabaseName(
                getPropertyOnNode(labelName, node, TopologyGraphDbmsModel.NAME_PROPERTY, String.class));
    }

    static TopologyGraphDbmsModel.DatabaseAccess getDatabaseAccess(Node databaseNode) {
        var accessString = (String) databaseNode.getProperty(
                TopologyGraphDbmsModel.DATABASE_ACCESS_PROPERTY,
                TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE.toString());
        return Enum.valueOf(TopologyGraphDbmsModel.DatabaseAccess.class, accessString);
    }

    public static Optional<Internal> createInternalReference(Node alias, NamedDatabaseId targetedDatabase) {
        return ignoreConcurrentDeletes(() -> {
            var aliasName = new NormalizedDatabaseName(getPropertyOnNode(
                    TopologyGraphDbmsModel.DATABASE_NAME, alias, TopologyGraphDbmsModel.NAME_PROPERTY, String.class));
            var namespace = new NormalizedDatabaseName(getPropertyOnNode(
                    TopologyGraphDbmsModel.DATABASE_NAME,
                    alias,
                    TopologyGraphDbmsModel.NAMESPACE_PROPERTY,
                    String.class));
            var primary = getPropertyOnNode(
                    TopologyGraphDbmsModel.DATABASE_NAME,
                    alias,
                    TopologyGraphDbmsModel.PRIMARY_PROPERTY,
                    Boolean.class);
            return Optional.of(new Internal(aliasName, namespace, targetedDatabase, primary));
        });
    }

    public static Optional<DatabaseReferenceImpl.External> createExternalReference(Node ref) {
        return ignoreConcurrentDeletes(() -> {
            var uriString = getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL_DESCRIPTION,
                    ref,
                    TopologyGraphDbmsModel.URL_PROPERTY,
                    String.class);
            var targetName = new NormalizedDatabaseName(getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL_DESCRIPTION,
                    ref,
                    TopologyGraphDbmsModel.TARGET_NAME_PROPERTY,
                    String.class));
            var aliasName = new NormalizedDatabaseName(getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL_DESCRIPTION,
                    ref,
                    TopologyGraphDbmsModel.NAME_PROPERTY,
                    String.class));
            var namespace = new NormalizedDatabaseName(getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL_DESCRIPTION,
                    ref,
                    TopologyGraphDbmsModel.NAMESPACE_PROPERTY,
                    String.class));

            var uri = URI.create(uriString);
            var host = SocketAddressParser.socketAddress(uri, BoltConnector.DEFAULT_PORT, SocketAddress::new);
            var remoteUri = new RemoteUri(uri.getScheme(), List.of(host), uri.getQuery());
            var uuid = getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL_DESCRIPTION,
                    ref,
                    TopologyGraphDbmsModel.VERSION_PROPERTY,
                    String.class);
            return Optional.of(new DatabaseReferenceImpl.External(
                    targetName, aliasName, namespace, remoteUri, UUID.fromString(uuid)));
        });
    }

    public static Optional<DriverSettings> getDriverSettings(Node aliasNode) {
        return ignoreConcurrentDeletes(() -> {
            var connectsWith = StreamSupport.stream(
                            aliasNode
                                    .getRelationships(
                                            Direction.OUTGOING, TopologyGraphDbmsModel.CONNECTS_WITH_RELATIONSHIP)
                                    .spliterator(),
                            false)
                    .toList(); // Must be collected to exhaust the underlying iterator

            return connectsWith.stream()
                    .findFirst()
                    .map(Relationship::getEndNode)
                    .map(CommunityTopologyGraphDbmsModelUtil::createDriverSettings);
        });
    }

    public static Optional<Map<String, Object>> getAliasProperties(Node aliasNode) {
        return ignoreConcurrentDeletes(() -> {
            var propertiesRels = StreamSupport.stream(
                            aliasNode
                                    .getRelationships(
                                            Direction.OUTGOING, TopologyGraphDbmsModel.PROPERTIES_RELATIONSHIP)
                                    .spliterator(),
                            false)
                    .toList(); // Must be collected to exhaust the underlying iterator

            return propertiesRels.stream()
                    .findFirst()
                    .map(Relationship::getEndNode)
                    .map(Entity::getAllProperties);
        });
    }

    public static Optional<ExternalDatabaseCredentials> getDatabaseCredentials(Node aliasNode) {
        return ignoreConcurrentDeletes(() -> {
            var username = getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE,
                    aliasNode,
                    TopologyGraphDbmsModel.USERNAME_PROPERTY,
                    String.class);
            var password = getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE,
                    aliasNode,
                    TopologyGraphDbmsModel.PASSWORD_PROPERTY,
                    byte[].class);
            var iv = getPropertyOnNode(
                    TopologyGraphDbmsModel.REMOTE_DATABASE,
                    aliasNode,
                    TopologyGraphDbmsModel.IV_PROPERTY,
                    byte[].class);
            return Optional.of(new ExternalDatabaseCredentials(username, password, iv));
        });
    }

    private static DriverSettings createDriverSettings(Node driverSettingsNode) {
        var builder = DriverSettings.builder();
        // TODO: Remove sslEnabled and use sslPolicy? Needs Cypher support
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.SSL_ENFORCED,
                        Boolean.class)
                .ifPresent(builder::withSslEnforced);
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.CONNECTION_TIMEOUT,
                        DurationValue.class)
                .ifPresent(builder::withConnectionTimeout);
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.CONNECTION_MAX_LIFETIME,
                        DurationValue.class)
                .ifPresent(builder::withConnectionMaxLifeTime);
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        CONNECTION_POOL_ACQUISITION_TIMEOUT.toString(),
                        DurationValue.class)
                .ifPresent(builder::withConnectionPoolAcquisitionTimeout);
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.CONNECTION_POOL_IDLE_TEST,
                        DurationValue.class)
                .ifPresent(builder::withConnectionPoolIdleTest);
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.CONNECTION_POOL_MAX_SIZE,
                        Number.class)
                .ifPresent(value -> builder.withConnectionPoolMaxSize(value.intValue()));
        getOptionalPropertyOnNode(
                        TopologyGraphDbmsModel.DRIVER_SETTINGS,
                        driverSettingsNode,
                        TopologyGraphDbmsModel.LOGGING_LEVEL,
                        String.class)
                .ifPresent(level -> builder.withLoggingLevel(Level.valueOf(level)));

        return builder.build();
    }

    static Optional<DatabaseReference> getInternalDatabaseReference(Transaction tx, String databaseName) {
        var aliasNode = findAliasNodeInDefaultNamespace(tx, databaseName);
        return aliasNode.flatMap(alias -> getTargetedDatabase(alias).flatMap(db -> createInternalReference(alias, db)));
    }

    static Optional<DatabaseReference> getExternalDatabaseReference(Transaction tx, String databaseName) {
        var aliasNode = findAliasNodeInDefaultNamespace(tx, databaseName);
        return aliasNode
                .filter(node -> node.hasLabel(TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL))
                .flatMap(CommunityTopologyGraphDbmsModelUtil::createExternalReference);
    }

    static Optional<NamedDatabaseId> getDatabaseIdByAlias(Transaction tx, String databaseName) {
        return findAliasNodeInDefaultNamespace(tx, databaseName)
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getTargetedDatabase);
    }

    static Optional<NamedDatabaseId> getDatabaseIdBy(Transaction tx, String propertyKey, String propertyValue) {
        try {
            var node = tx.findNode(TopologyGraphDbmsModel.DATABASE_LABEL, propertyKey, propertyValue);

            if (node == null) {
                return Optional.empty();
            }

            var databaseName = getPropertyOnNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL.name(),
                    node,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    String.class);
            var databaseUuid = getPropertyOnNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL.name(),
                    node,
                    TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY,
                    String.class);

            return Optional.of(DatabaseIdFactory.from(databaseName, UUID.fromString(databaseUuid)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * *Note* may return `Optional.empty`.
     * <p>
     * It s semantically invalid for an alias to *not* have target, but we ignore it because of the possibility of concurrent deletes.
     */
    private static Optional<NamedDatabaseId> getTargetedDatabase(Node aliasNode) {
        return ignoreConcurrentDeletes(
                () -> {
                    try (Stream<Relationship> stream = aliasNode
                            .getRelationships(Direction.OUTGOING, TopologyGraphDbmsModel.TARGETS_RELATIONSHIP)
                            .stream()) {
                        return stream.findFirst()
                                .map(Relationship::getEndNode)
                                .map(CommunityTopologyGraphDbmsModelUtil::getDatabaseId);
                    }
                });
    }

    public static Optional<Node> getTargetedDatabaseNode(Node aliasNode) {
        return ignoreConcurrentDeletes(
                () -> {
                    try (Stream<Relationship> stream = aliasNode
                            .getRelationships(Direction.OUTGOING, TopologyGraphDbmsModel.TARGETS_RELATIONSHIP)
                            .stream()) {
                        return stream.findFirst().map(Relationship::getEndNode);
                    }
                });
    }

    static NamedDatabaseId getDatabaseId(Node databaseNode) {
        var name = (String) databaseNode.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY);
        var uuid = UUID.fromString((String) databaseNode.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY));
        return DatabaseIdFactory.from(name, uuid);
    }

    private static <T> Optional<T> getOptionalPropertyOnNode(String labelName, Node node, String key, Class<T> type) {
        Object value;
        try {
            value = node.getProperty(key);
        } catch (NotFoundException e) {
            return Optional.empty();
        }

        if (value == null) {
            return Optional.empty();
        }

        if (!type.isInstance(value)) {
            throw new IllegalStateException(
                    String.format("%s has non %s property %s.", labelName, type.getSimpleName(), key));
        }

        return Optional.of(type.cast(value));
    }

    private static <T> T getPropertyOnNode(String labelName, Node node, String key, Class<T> type) {
        var value = node.getProperty(key);
        if (value == null) {
            throw new IllegalStateException(String.format("%s has no property %s.", labelName, key));
        }
        if (!type.isInstance(value)) {
            throw new IllegalStateException(
                    String.format("%s has non %s property %s.", labelName, type.getSimpleName(), key));
        }
        return type.cast(value);
    }

    private static Optional<Node> findAliasNodeInDefaultNamespace(Transaction tx, String databaseName) {
        try (var nodes = tx.findNodes(
                TopologyGraphDbmsModel.DATABASE_NAME_LABEL, TopologyGraphDbmsModel.NAME_PROPERTY, databaseName)) {
            return nodes.stream()
                    .filter(n -> getOptionalPropertyOnNode(
                                    TopologyGraphDbmsModel.DATABASE_NAME,
                                    n,
                                    TopologyGraphDbmsModel.NAMESPACE_PROPERTY,
                                    String.class)
                            .orElse(TopologyGraphDbmsModel.DEFAULT_NAMESPACE)
                            .equals(TopologyGraphDbmsModel.DEFAULT_NAMESPACE))
                    .findFirst();
        }
    }

    static <T> Optional<T> ignoreConcurrentDeletes(Supplier<Optional<T>> operation) {
        try {
            return operation.get();
        } catch (NotFoundException e) {
            return Optional.empty();
        }
    }
}
