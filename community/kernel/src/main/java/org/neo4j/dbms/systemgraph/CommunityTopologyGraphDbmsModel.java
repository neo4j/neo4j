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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedCatalogEntry;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public class CommunityTopologyGraphDbmsModel implements TopologyGraphDbmsModel {
    protected final Transaction tx;

    public CommunityTopologyGraphDbmsModel(Transaction tx) {
        this.tx = tx;
    }

    public Map<NamedDatabaseId, TopologyGraphDbmsModel.DatabaseAccess> getAllDatabaseAccess() {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
            return nodes.stream()
                    .collect(Collectors.toMap(
                            CommunityTopologyGraphDbmsModelUtil::getDatabaseId,
                            CommunityTopologyGraphDbmsModelUtil::getDatabaseAccess));
        }
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByAlias(String databaseName) {
        return CommunityTopologyGraphDbmsModelUtil.getDatabaseIdByAliasInRoot(tx, databaseName)
                .or(() ->
                        CommunityTopologyGraphDbmsModelUtil.getDatabaseIdBy(tx, DATABASE_NAME_PROPERTY, databaseName));
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid) {
        return CommunityTopologyGraphDbmsModelUtil.getDatabaseIdBy(tx, DATABASE_UUID_PROPERTY, uuid.toString());
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid, boolean resolveToShardedDb) {
        // resolveToShardedDb not needed, since we do not support sharded databases in community
        return CommunityTopologyGraphDbmsModelUtil.getDatabaseIdBy(tx, DATABASE_UUID_PROPERTY, uuid.toString());
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences() {
        var primaryRefs = CommunityTopologyGraphDbmsModelUtil.getAllPrimaryStandardDatabaseReferencesInRoot(tx);
        var internalAliasRefs = getAllInternalDatabaseReferencesInRoot();
        var externalRefs = getAllExternalDatabaseReferencesInRoot();
        var compositeRefs = getAllCompositeDatabaseReferencesInRoot();
        var virtualSPDRefs = getAllVirtualSPDReferencesInRoot();
        var spdGraphShardRefs = getAllSPDGraphShardReferencesInRoot();
        var spdPropertyShardRefs = getAllSPDPropertyShardReferencesInRoot();
        var mirrorRefs = getAllMirrorReferencesInRoot();
        return Stream.of(
                        primaryRefs,
                        internalAliasRefs,
                        externalRefs,
                        compositeRefs,
                        virtualSPDRefs,
                        spdGraphShardRefs,
                        spdPropertyShardRefs,
                        mirrorRefs)
                .flatMap(s -> s)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferences() {
        return getAllCompositeDatabaseReferencesInRoot().collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Optional<DatabaseReference> getDatabaseRefByAlias(NormalizedCatalogEntry catalogEntry) {
        if (catalogEntry.compositeDb().isPresent()) {
            return resolveConstituent(catalogEntry.compositeDb().get(), catalogEntry.databaseAlias());
        } else {
            return resolveRootReference(catalogEntry.databaseAlias());
        }
    }

    @Override
    public Optional<DatabaseReference> getDatabaseRefByDisplayName(NormalizedDatabaseName displayName) {
        return Optional.<DatabaseReference>empty()
                .or(() -> getCompositeDatabaseReference(displayName.name()))
                .or(() -> getVirtualSPDReferences(displayName.name()))
                .or(() -> getSPDGraphShardReference(displayName.name()))
                .or(() -> getSPDPropertyShardReference(displayName.name()))
                .or(() -> getMirrorReferences(displayName.name()))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReference(tx, displayName.name()))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReference(tx, displayName.name()));
    }

    private Optional<DatabaseReference> resolveConstituent(String composite, String constituent) {
        return Optional.<DatabaseReference>empty()
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReference(tx, composite, constituent))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReferenceInRoot(
                        tx, composite, constituent));
    }

    private Optional<DatabaseReference> resolveRootReference(String normalizedDatabaseAlias) {
        return Optional.<DatabaseReference>empty()
                .or(() -> getCompositeDatabaseReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> getVirtualSPDReferencesInRoot(normalizedDatabaseAlias))
                .or(() -> getSPDGraphShardReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> getSPDPropertyShardReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> getMirrorReferencesInRoot(normalizedDatabaseAlias))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReferenceInRoot(
                        tx, normalizedDatabaseAlias))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReferenceInRoot(
                        tx, normalizedDatabaseAlias));
    }

    private Stream<Node> getAllAliasNodesInRoot() {
        return getAllAliasNodesInNamespace(DEFAULT_NAMESPACE);
    }

    private Stream<Node> getAllAliasNodesInNamespace(String namespace) {
        return tx.findNodes(DATABASE_NAME_LABEL, NAMESPACE_PROPERTY, namespace).stream().toList().stream();
    }

    private Stream<Node> getRemoteAliasNodesInNamespace(String namespace) {
        return tx.findNodes(REMOTE_DATABASE_LABEL, NAMESPACE_PROPERTY, namespace).stream().toList().stream();
    }

    private Stream<Node> getAliasNodeInNamespace(String namespace, String databaseName) {
        return tx.findNodes(DATABASE_NAME_LABEL, NAMESPACE_PROPERTY, namespace, NAME_PROPERTY, databaseName).stream()
                .toList()
                .stream();
    }

    private Stream<Node> getAliasNode(String displayName) {
        return tx.findNodes(DATABASE_NAME_LABEL, DISPLAY_NAME_PROPERTY, displayName).stream().toList().stream();
    }

    private Stream<Node> getAliasNodeInRoot(String databaseName) {
        return getAliasNodeInNamespace(DEFAULT_NAMESPACE, databaseName);
    }

    @Override
    public Optional<DriverSettings> getDriverSettings(String databaseName, String namespace) {
        databaseName = NormalizedDatabaseName.normalize(databaseName);
        namespace = NormalizedDatabaseName.normalize(namespace);
        return tx.findNodes(REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName, NAMESPACE_PROPERTY, namespace).stream()
                .toList()
                .stream()
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getDriverSettings);
    }

    @Override
    public Optional<Map<String, Object>> getAliasProperties(String databaseName, String namespace) {
        databaseName = NormalizedDatabaseName.normalize(databaseName);
        namespace = NormalizedDatabaseName.normalize(namespace);
        return getAliasNodeInNamespace(namespace, databaseName)
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getAliasProperties);
    }

    @Override
    public Optional<CypherVersion> getRemoteAliasLanguageVersion(String remoteAliasName) {
        try (var nodes = tx.findNodes(REMOTE_DATABASE_LABEL, NAME_PROPERTY, remoteAliasName)) {
            var filtered = nodes.stream()
                    .filter(node -> node.getProperty(NAMESPACE_PROPERTY).equals(DEFAULT_NAMESPACE))
                    .toList();
            if (filtered.isEmpty()) {
                return Optional.empty();
            }
            String defaultLanguage = (String) filtered.getFirst().getProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY);
            if (defaultLanguage != null) {
                return CypherVersion.fromStoredValueOptional(defaultLanguage);
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Optional<ExternalDatabaseCredentials> getExternalDatabaseCredentials(
            DatabaseReferenceImpl.External databaseReference) {
        String databaseName = databaseReference.alias().name();
        String namespace =
                databaseReference.namespace().map(NormalizedDatabaseName::name).orElse(DEFAULT_NAMESPACE);
        return tx.findNodes(REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName, NAMESPACE_PROPERTY, namespace).stream()
                .toList()
                .stream()
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getDatabaseCredentials);
    }

    private Stream<DatabaseReferenceImpl.Mirror> getAllMirrorReferencesInRoot() {
        return getAllAliasNodesInRoot()
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(MIRROR_LABEL))
                        .flatMap(db -> createMirrorReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.Mirror> getMirrorReferences(String displayName) {
        return getMirrorReferences(getAliasNode(displayName));
    }

    private Optional<DatabaseReferenceImpl.Mirror> getMirrorReferencesInRoot(String databaseName) {
        return getMirrorReferences(getAliasNodeInRoot(databaseName));
    }

    private Optional<DatabaseReferenceImpl.Mirror> getMirrorReferences(Stream<Node> aliases) {
        return aliases.flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(MIRROR_LABEL))
                        .flatMap(db -> createMirrorReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Stream<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferencesInRoot() {
        return getAllAliasNodesInRoot()
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .flatMap(db -> createCompositeReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.Composite> getCompositeDatabaseReferenceInRoot(String databaseName) {
        return getCompositeDatabaseReference(getAliasNodeInRoot(databaseName));
    }

    private Optional<DatabaseReferenceImpl.Composite> getCompositeDatabaseReference(String displayName) {
        return getCompositeDatabaseReference(getAliasNode(displayName));
    }

    private Optional<DatabaseReferenceImpl.Composite> getCompositeDatabaseReference(Stream<Node> aliases) {
        return aliases.flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .flatMap(db -> createCompositeReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Optional<DatabaseReferenceImpl.Composite> createCompositeReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var aliasName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE_NAME, alias);
            var databaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);
            var compositeName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE, db);
            var components = getAllDatabaseReferencesInComposite(compositeName);
            return Optional.of(new DatabaseReferenceImpl.Composite(aliasName, databaseId, components));
        });
    }

    private Stream<DatabaseReferenceImpl.VirtualSPD> getAllVirtualSPDReferencesInRoot() {
        return getAllAliasNodesInRoot()
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(SPD_LABEL))
                        .flatMap(db -> createVirtualSPDReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.VirtualSPD> getVirtualSPDReferences(String displayName) {
        return getVirtualSPDReferences(getAliasNode(displayName));
    }

    private Optional<DatabaseReferenceImpl.VirtualSPD> getVirtualSPDReferencesInRoot(String databaseName) {
        return getVirtualSPDReferences(getAliasNodeInRoot(databaseName));
    }

    private Optional<DatabaseReferenceImpl.VirtualSPD> getVirtualSPDReferences(Stream<Node> nodes) {
        return nodes.flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(SPD_LABEL))
                        .flatMap(db -> createVirtualSPDReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Stream<DatabaseReferenceImpl.GraphShard> getAllSPDGraphShardReferencesInRoot() {
        return getAllAliasNodesInRoot()
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(GRAPH_SHARD_LABEL))
                        .flatMap(db -> createSPDGraphShardReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.GraphShard> getSPDGraphShardReference(String displayName) {
        return getSPDGraphShardReferenceInRoot(getAliasNode(displayName));
    }

    private Optional<DatabaseReferenceImpl.GraphShard> getSPDGraphShardReferenceInRoot(String databaseName) {
        return getSPDGraphShardReferenceInRoot(getAliasNodeInRoot(databaseName));
    }

    private Optional<DatabaseReferenceImpl.GraphShard> getSPDGraphShardReferenceInRoot(Stream<Node> nodes) {
        return nodes.flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(GRAPH_SHARD_LABEL))
                        .flatMap(db -> createSPDGraphShardReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Stream<DatabaseReferenceImpl.PropertyShard> getAllSPDPropertyShardReferencesInRoot() {
        return getAllAliasNodesInRoot()
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(PROPERTY_SHARD_LABEL))
                        .flatMap(db -> createSPDPropertyShardReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.PropertyShard> getSPDPropertyShardReference(String displayName) {
        return getSPDPropertyShardReference(getAliasNode(displayName));
    }

    private Optional<DatabaseReferenceImpl.PropertyShard> getSPDPropertyShardReferenceInRoot(String databaseName) {
        return getSPDPropertyShardReference(getAliasNodeInRoot(databaseName));
    }

    private Optional<DatabaseReferenceImpl.PropertyShard> getSPDPropertyShardReference(Stream<Node> nodes) {
        return nodes.flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(PROPERTY_SHARD_LABEL))
                        .flatMap(db -> createSPDPropertyShardReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Optional<DatabaseReferenceImpl.VirtualSPD> createVirtualSPDReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var spdAliasName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE_NAME, alias);
            NamedDatabaseId spdNamedDatabaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);

            var graphShard =
                    db
                            .getRelationships(Direction.OUTGOING, TopologyGraphDbmsModel.HAS_GRAPH_SHARD_RELATIONSHIP)
                            .stream()
                            .flatMap(rel -> createSPDGraphShardReference(rel.getEndNode()).stream())
                            .toList();
            if (graphShard.isEmpty()) {
                return Optional.empty();
            }

            boolean isPrimary = (boolean) alias.getProperty(PRIMARY_PROPERTY);
            return Optional.of(new DatabaseReferenceImpl.VirtualSPD(
                    spdAliasName,
                    new NormalizedDatabaseName((String) alias.getProperty(NAMESPACE_PROPERTY)),
                    spdNamedDatabaseId,
                    graphShard.getFirst(),
                    isPrimary));
        });
    }

    private Optional<DatabaseReferenceImpl.GraphShard> createSPDGraphShardReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var aliasName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE_NAME, alias);
            var databaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);
            var shards = StreamSupport.stream(
                            db.getRelationships(
                                            Direction.OUTGOING, TopologyGraphDbmsModel.HAS_PROPERTY_SHARD_RELATIONSHIP)
                                    .spliterator(),
                            false)
                    .flatMap(rel -> getDatabaseRefByAlias(
                            new NormalizedCatalogEntry((String) rel.getEndNode().getProperty(DATABASE_NAME_PROPERTY)))
                            .map(ref -> Pair.of(
                                    (int) rel.getProperty(HAS_PROPERTY_SHARD_INDEX_PROPERTY),
                                    (DatabaseReferenceImpl.PropertyShard) ref))
                            .stream())
                    .collect(Collectors.toMap(Pair::first, Pair::other));
            String owningDatabase = CommunityTopologyGraphDbmsModelUtil.readGraphShardOwningDatabase(db)
                    .orElseThrow();
            return Optional.of(new DatabaseReferenceImpl.GraphShard(aliasName, databaseId, owningDatabase, shards));
        });
    }

    private Optional<DatabaseReferenceImpl.GraphShard> createSPDGraphShardReference(Node db) {
        var aliasName = new NormalizedDatabaseName((String) db.getProperty(NAME_PROPERTY));
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var databaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);
            var shards = StreamSupport.stream(
                            db.getRelationships(
                                            Direction.OUTGOING, TopologyGraphDbmsModel.HAS_PROPERTY_SHARD_RELATIONSHIP)
                                    .spliterator(),
                            false)
                    .flatMap(rel -> getDatabaseRefByAlias(
                            new NormalizedCatalogEntry((String) rel.getEndNode().getProperty(DATABASE_NAME_PROPERTY)))
                            .map(ref -> Pair.of(
                                    (int) rel.getProperty(HAS_PROPERTY_SHARD_INDEX_PROPERTY),
                                    (DatabaseReferenceImpl.PropertyShard) ref))
                            .stream())
                    .collect(Collectors.toMap(Pair::first, Pair::other));
            String owningDatabase = CommunityTopologyGraphDbmsModelUtil.readGraphShardOwningDatabase(db)
                    .orElseThrow();
            return Optional.of(new DatabaseReferenceImpl.GraphShard(aliasName, databaseId, owningDatabase, shards));
        });
    }

    private static Optional<DatabaseReferenceImpl.PropertyShard> createSPDPropertyShardReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.createInternalReference(
                        alias, CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db))
                .flatMap(internal -> CommunityTopologyGraphDbmsModelUtil.readPropertyShardOwningDatabaseAndIndex(db)
                        .map(p -> internal.asShard(p.first(), p.other())));
    }

    private static Optional<DatabaseReferenceImpl.Mirror> createMirrorReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.createInternalReference(
                        alias, CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db))
                .map(DatabaseReferenceImpl.Internal::asMirror);
    }

    private Set<DatabaseReference> getAllDatabaseReferencesInComposite(NormalizedDatabaseName compositeName) {
        var internalRefs = getAllInternalDatabaseReferencesInNamespace(compositeName.name());
        var spdInternalRefs = getAllSpdDatabaseReferencesInNamespace(compositeName.name());
        var externalRefs = getAllExternalDatabaseReferencesInNamespace(compositeName.name());
        return Stream.concat(Stream.concat(internalRefs, externalRefs), spdInternalRefs)
                .collect(Collectors.toUnmodifiableSet());
    }

    private Stream<DatabaseReferenceImpl.External> getAllExternalDatabaseReferencesInRoot() {
        return getAllExternalDatabaseReferencesInNamespace(DEFAULT_NAMESPACE);
    }

    private Stream<DatabaseReferenceImpl.External> getAllExternalDatabaseReferencesInNamespace(String namespace) {
        return getRemoteAliasNodesInNamespace(namespace)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.createExternalReference(alias).stream());
    }

    private Stream<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferencesInRoot() {
        return getAllInternalDatabaseReferencesInNamespace(DEFAULT_NAMESPACE);
    }

    private Stream<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferencesInNamespace(String namespace) {
        return getAllAliasNodesInNamespace(namespace)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(node -> !node.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .filter(node -> !node.hasLabel(SPD_LABEL))
                        .filter(node -> !node.hasLabel(GRAPH_SHARD_LABEL))
                        .filter(node -> !node.hasLabel(PROPERTY_SHARD_LABEL))
                        .filter(node -> !node.hasLabel(MIRROR_LABEL))
                        .map(CommunityTopologyGraphDbmsModelUtil::getDatabaseId)
                        .flatMap(db -> CommunityTopologyGraphDbmsModelUtil.createInternalReference(alias, db))
                        .stream());
    }

    private Stream<DatabaseReferenceImpl.Internal> getAllSpdDatabaseReferencesInNamespace(String namespace) {
        return getAllAliasNodesInNamespace(namespace)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(node -> !node.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .filter(node -> node.hasLabel(SPD_LABEL))
                        .flatMap(db -> createVirtualSPDReference(alias, db))
                        .stream());
    }
}
