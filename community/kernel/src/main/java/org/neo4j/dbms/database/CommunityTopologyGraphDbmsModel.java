/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

public class CommunityTopologyGraphDbmsModel implements TopologyGraphDbmsModel {
    protected final Transaction tx;

    public CommunityTopologyGraphDbmsModel(Transaction tx) {
        this.tx = tx;
    }

    public Map<NamedDatabaseId, TopologyGraphDbmsModel.DatabaseAccess> getAllDatabaseAccess() {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
            return nodes.stream()
                    .collect(Collectors.toMap(
                            CommunityTopologyGraphDbmsModel::getDatabaseId,
                            CommunityTopologyGraphDbmsModel::getDatabaseAccess));
        }
    }

    private static TopologyGraphDbmsModel.DatabaseAccess getDatabaseAccess(Node databaseNode) {
        var accessString = (String) databaseNode.getProperty(
                DATABASE_ACCESS_PROPERTY, TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE.toString());
        return Enum.valueOf(TopologyGraphDbmsModel.DatabaseAccess.class, accessString);
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByAlias(String databaseName) {
        return getDatabaseIdByAlias0(databaseName).or(() -> getDatabaseIdBy(DATABASE_NAME_PROPERTY, databaseName));
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid) {
        return getDatabaseIdBy(DATABASE_UUID_PROPERTY, uuid.toString());
    }

    @Override
    public Map<String, NamedDatabaseId> getAllDatabaseAliases() {
        var allDbNames =
                getAllDatabaseIds().stream().collect(Collectors.toMap(NamedDatabaseId::name, Function.identity()));
        var allDbAliases = getAllDatabaseAliases0();

        var all = new HashMap<String, NamedDatabaseId>();
        all.putAll(allDbNames);
        all.putAll(allDbAliases);
        return Map.copyOf(all);
    }

    @Override
    public Set<NamedDatabaseId> getAllDatabaseIds() {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
            return nodes.stream()
                    .map(CommunityTopologyGraphDbmsModel::getDatabaseId)
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    private Map<String, NamedDatabaseId> getAllDatabaseAliases0() {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
            return nodes.stream()
                    .flatMap(alias -> getTargetedDatabase(alias).flatMap(db -> aliasDatabaseIdPair(alias, db)).stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private Optional<Map.Entry<String, NamedDatabaseId>> aliasDatabaseIdPair(
            Node alias, NamedDatabaseId targetedDatabase) {
        return ignoreConcurrentDeletes(() -> {
            var aliasName = getPropertyOnNode(DATABASE_NAME, alias, NAME_PROPERTY);
            return Optional.of(Map.entry(aliasName, targetedDatabase));
        });
    }

    private Optional<NamedDatabaseId> getDatabaseIdByAlias0(String databaseName) {
        var node = Optional.ofNullable(tx.findNode(DATABASE_NAME_LABEL, NAME_PROPERTY, databaseName));
        return node.flatMap(CommunityTopologyGraphDbmsModel::getTargetedDatabase);
    }

    private Optional<NamedDatabaseId> getDatabaseIdBy(String propertyKey, String propertyValue) {
        try {
            var node = tx.findNode(DATABASE_LABEL, propertyKey, propertyValue);

            if (node == null) {
                return Optional.empty();
            }

            var databaseName = getPropertyOnNode(DATABASE_LABEL.name(), node, DATABASE_NAME_PROPERTY);
            var databaseUuid = getPropertyOnNode(DATABASE_LABEL.name(), node, DATABASE_UUID_PROPERTY);

            return Optional.of(DatabaseIdFactory.from(databaseName, UUID.fromString(databaseUuid)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * *Note* may return `Optional.empty`.
     *
     * It s semantically invalid for an alias to *not* have target, but we ignore it because of the possibility of concurrent deletes.
     */
    private static Optional<NamedDatabaseId> getTargetedDatabase(Node aliasNode) {
        return ignoreConcurrentDeletes(() -> {
            try (Stream<Relationship> stream =
                    aliasNode.getRelationships(Direction.OUTGOING, TARGETS_RELATIONSHIP).stream()) {
                return stream.findFirst()
                        .map(Relationship::getEndNode)
                        .map(CommunityTopologyGraphDbmsModel::getDatabaseId);
            }
        });
    }

    private static NamedDatabaseId getDatabaseId(Node databaseNode) {
        var name = (String) databaseNode.getProperty(DATABASE_NAME_PROPERTY);
        var uuid = UUID.fromString((String) databaseNode.getProperty(DATABASE_UUID_PROPERTY));
        return DatabaseIdFactory.from(name, uuid);
    }

    private static String getPropertyOnNode(String labelName, Node node, String key) {
        var value = node.getProperty(key);
        if (value == null) {
            throw new IllegalStateException(String.format("%s has no property %s.", labelName, key));
        }
        if (!(value instanceof String)) {
            throw new IllegalStateException(String.format("%s has non String property %s.", labelName, key));
        }
        return (String) value;
    }

    private static <T> Optional<T> ignoreConcurrentDeletes(Supplier<Optional<T>> operation) {
        try {
            return operation.get();
        } catch (NotFoundException e) {
            return Optional.empty();
        }
    }
}
