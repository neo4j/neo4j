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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.DirectedTypes;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * Utilities for dealing with RelationshipSelectionCursor and corresponding iterators.
 */
public final class RelationshipSelections {

    private RelationshipSelections() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * Returns an outgoing selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorContext underlying page cursor context
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor outgoingCursor(
            CursorFactory cursors, NodeCursor node, int[] types, CursorContext cursorContext) {
        return relationshipsCursor(
                cursors.allocateRelationshipTraversalCursor(cursorContext), node, types, Direction.OUTGOING);
    }

    /**
     * Returns an outgoing selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor outgoingCursor(
            RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types) {
        return relationshipsCursor(traversalCursor, node, types, Direction.OUTGOING);
    }

    /**
     * Returns an incoming selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor incomingCursor(
            RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types) {
        return relationshipsCursor(traversalCursor, node, types, Direction.INCOMING);
    }

    /**
     * Returns an incoming selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorContext underlying page cursor context
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor incomingCursor(
            CursorFactory cursors, NodeCursor node, int[] types, CursorContext cursorContext) {
        return relationshipsCursor(
                cursors.allocateRelationshipTraversalCursor(cursorContext), node, types, Direction.INCOMING);
    }

    /**
     * Returns a multi-directed selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorContext underlying page cursor context
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor allCursor(
            CursorFactory cursors, NodeCursor node, int[] types, CursorContext cursorContext) {
        return relationshipsCursor(
                cursors.allocateRelationshipTraversalCursor(cursorContext), node, types, Direction.BOTH);
    }

    /**
     * Returns a multi-directed selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor allCursor(
            RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types) {
        node.relationships(traversalCursor, RelationshipSelection.selection(types, Direction.BOTH));
        return traversalCursor;
    }

    /**
     * Allows specification of types for the three different directions. Assumes that the
     * three arrays are all pairwise disjoint. A null array signifies that we allow all types in the corresponding
     * direction.
     * <p>
     * If one of the directed arrays are null, then the other directed array must be empty per the disjoint
     * assumption. If the bothTypes array is null, then both other arrays need to be empty.
     *
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node
     * @param directedTypes The types and corresponding directions to traverse
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor multiTypeMultiDirectionCursor(
            RelationshipTraversalCursor traversalCursor, NodeCursor node, DirectedTypes directedTypes) {
        node.relationships(traversalCursor, RelationshipSelection.selection(directedTypes));
        return traversalCursor;
    }

    public static RelationshipTraversalCursor relationshipsCursor(
            RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types, Direction outgoing) {
        node.relationships(traversalCursor, RelationshipSelection.selection(types, outgoing));
        return traversalCursor;
    }

    /**
     * Returns an outgoing resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorContext underlying page cursor context
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> outgoingIterator(
            CursorFactory cursors,
            NodeCursor node,
            int[] types,
            RelationshipFactory<T> factory,
            CursorContext cursorContext) {
        return new RelationshipEntityIterator<>(outgoingCursor(cursors, node, types, cursorContext), factory);
    }

    /**
     * Returns an incoming resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorContext underlying page cursor context
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> incomingIterator(
            CursorFactory cursors,
            NodeCursor node,
            int[] types,
            RelationshipFactory<T> factory,
            CursorContext cursorContext) {
        return new RelationshipEntityIterator<>(incomingCursor(cursors, node, types, cursorContext), factory);
    }

    /**
     * Returns a multi-directed resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorContext underlying page cursor context
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> allIterator(
            CursorFactory cursors,
            NodeCursor node,
            int[] types,
            RelationshipFactory<T> factory,
            CursorContext cursorContext) {
        return new RelationshipEntityIterator<>(allCursor(cursors, node, types, cursorContext), factory);
    }

    private static class RelationshipEntityIterator<T> extends PrefetchingResourceIterator<T> {
        private final RelationshipTraversalCursor relationshipTraversalCursor;
        private final RelationshipFactory<T> factory;

        RelationshipEntityIterator(
                RelationshipTraversalCursor relationshipTraversalCursor, RelationshipFactory<T> factory) {
            this.relationshipTraversalCursor = relationshipTraversalCursor;
            this.factory = factory;
        }

        @Override
        public void close() {
            relationshipTraversalCursor.close();
        }

        @Override
        protected T fetchNextOrNull() {
            if (relationshipTraversalCursor.next()) {
                return factory.relationship(relationshipTraversalCursor);
            }
            close();
            return null;
        }
    }
}
