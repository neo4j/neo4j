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
package org.neo4j.storageengine.api;

/**
 * Cursor over nodes and its data.
 */
public interface StorageNodeCursor extends StorageEntityScanCursor<AllNodeScan> {
    /**
     * @return label ids of the node this cursor currently is placed at.
     */
    long[] labels();

    /**
     * @return {@code true} if the node this cursor is placed at has the given {@code label}, otherwise {@code false}.
     */
    boolean hasLabel(int label);

    /**
     * Atomically read labels and properties.
     */
    default long[] labelsAndProperties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
        var labels = labels();
        properties(propertyCursor, selection);
        return labels;
    }

    /**
     * @return {@code true} if the node this cursor is placed at has at least one label, otherwise {@code false}.
     */
    boolean hasLabel();

    /**
     * @return reference for reading all relationships of the node this cursor currently is placed at.
     */
    long relationshipsReference();

    /**
     * Initializes the provided {@code traversalCursor} with selected relationships connected to the node this cursor is currently at.
     * After this call the relationships can be accessed using {@link StorageRelationshipTraversalCursor#next()}.
     *
     * @param traversalCursor the {@link StorageRelationshipTraversalCursor} to initialize with relationships for this current node.
     * @param selection {@link RelationshipSelection} of relationships to select.
     */
    void relationships(StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection);

    /**
     * @return {@code true} if a call to {@link #relationshipsTo(StorageRelationshipTraversalCursor, RelationshipSelection, long)} is allowed,
     * i.e. that this cursor implementation supports fast relationships to query, otherwise {@code false}.
     */
    boolean supportsFastRelationshipsTo();

    /**
     * Initializes the provided {@code traversalCursor} with selected relationships connecting the node this cursor is currently at
     * with the provided {@code neighbourNodeReference}.
     * After this call the relationships can be accessed using {@link StorageRelationshipTraversalCursor#next()}.
     *
     * @param traversalCursor the {@link StorageRelationshipTraversalCursor} to initialize with relationships for this current node.
     * @param selection {@link RelationshipSelection} of relationships to select.
     * @param neighbourNodeReference the neighbour {@link StorageNodeCursor#entityReference() node reference} to look for.
     * @throws UnsupportedOperationException if not supported, i.e. if {@link #supportsFastRelationshipsTo()} returns {@code false}.
     */
    void relationshipsTo(
            StorageRelationshipTraversalCursor traversalCursor,
            RelationshipSelection selection,
            long neighbourNodeReference);

    /**
     * @return all relationship types that this node has, i.e. all relationship types in the returned array there are at one such
     * relationship of on this node.
     */
    int[] relationshipTypes();

    /**
     * Visits degrees, i.e. number of relationships, for relationships of the given {@code selection} and gives those degrees to the {@code mutator}.
     * @param selection {@link RelationshipSelection} to get degrees for.
     * @param mutator to given the degrees to.
     */
    void degrees(RelationshipSelection selection, Degrees.Mutator mutator);

    /**
     * NOTE the fact that this method is here means physical details about underlying storage leaks into this API.
     * However this method has to exist as long as the kernel API also exposes this. This needs to change at some point.
     *
     * @return whether or not this node is dense.
     */
    boolean supportsFastDegreeLookup();
}
