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
package org.neo4j.storageengine.api.txstate;

import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties;
import org.neo4j.util.Preconditions;

/**
 * A {@link RelationshipModifications} is a set representing relationship changes with utility functions for iterating over different subsets/groupings
 * The changes are first grouped by node, then by relationship type and last relationship direction
 *
 * <pre>
 * // For example
 * ids.forEachSplit( byNode ->
 * {
 *     // To access ids for each chain (by node, relationship type and direction) individually
 *     byNode.forEachSplit( byNodeAndType ->
 *     {
 *         byNodeAndType.out().forEach( id -> {} ); // all relationship ids for the OUTGOING relationship chain for this node and type
 *         byNodeAndType.in().forEach( id -> {} );  // all relationship ids for the INCOMING relationship chain for this node and type
 *         byNodeAndType.loop().forEach( id -> {} );// all relationship ids for the LOOP relationship chain for this node and type
 *     } );
 *
 *     // To access all ids for this node
 *     byNode.forEach( id -> {} ); // all relationship ids for this node
 * } );
 * </pre>
 */
public interface RelationshipModifications {
    RelationshipBatch EMPTY_BATCH = new RelationshipBatch() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forEach(RelationshipVisitorWithProperties relationship) {}
    };

    static IdDataDecorator noAdditionalDataDecorator() {
        return new IdDataDecorator() {
            @Override
            public <E extends Exception> void accept(long id, RelationshipVisitorWithProperties<E> visitor) throws E {
                visitor.visit(id, -1, -1, -1, Collections.emptyList());
            }
        };
    }

    static RelationshipBatch idsAsBatch(LongSet ids) {
        return idsAsBatch(ids, noAdditionalDataDecorator());
    }

    static RelationshipBatch idsAsBatch(LongSet ids, IdDataDecorator idDataDecorator) {
        return new RelationshipBatch() {
            @Override
            public int size() {
                return ids.size();
            }

            @Override
            public boolean isEmpty() {
                return ids.isEmpty();
            }

            @Override
            public boolean contains(long id) {
                return ids.contains(id);
            }

            @Override
            public long first() {
                return ids.longIterator().next();
            }

            @Override
            public <E extends Exception> void forEach(RelationshipVisitorWithProperties<E> relationship) throws E {
                LongIterator iterator = ids.longIterator();
                while (iterator.hasNext()) {
                    long id = iterator.next();
                    idDataDecorator.accept(id, relationship);
                }
            }
        };
    }

    /**
     * Visits all relationships, split by node.
     * @param visitor to receive the relationship data.
     */
    void forEachSplit(IdsVisitor visitor);

    /**
     * @return all created relationships.
     */
    RelationshipBatch creations();

    /**
     * @return all deleted relationships.
     */
    RelationshipBatch deletions();

    interface IdsVisitor extends Consumer<NodeRelationshipIds> {}

    interface TypeIdsVisitor extends Consumer<NodeRelationshipTypeIds> {}

    interface InterruptibleTypeIdsVisitor extends Predicate<NodeRelationshipTypeIds> {}

    /**
     * Relationship IDs and data for a node.
     */
    interface NodeRelationshipIds {
        /**
         * @return ID of the node these relationships are for.
         */
        long nodeId();

        /**
         * @return whether or not there are created relationships for this node.
         */
        boolean hasCreations();

        /**
         * @return whether or not there are created relationships of the given type for this node.
         */
        boolean hasCreations(int type);

        /**
         * @return whether or not there are deleted relationships for this node.
         */
        boolean hasDeletions();

        /**
         * @return created relationships for this node.
         */
        RelationshipBatch creations();

        /**
         * @return deleted relationships for this node.
         */
        RelationshipBatch deletions();

        /**
         * Visits all created relationships for this node, split by type.
         * @param visitor to receive the relationship data.
         */
        default void forEachCreationSplit(TypeIdsVisitor visitor) {
            forEachCreationSplitInterruptible(byType -> {
                visitor.accept(byType);
                return false;
            });
        }

        /**
         * Can be interrupted earlier if the provided visitor decides to, instead of forcing it to be exhausted.
         * @see #forEachCreationSplit(TypeIdsVisitor)
         */
        void forEachCreationSplitInterruptible(InterruptibleTypeIdsVisitor visitor);

        /**
         * Visits all deleted relationships for this node, split by type.
         * @param visitor to receive the relationship data.
         */
        default void forEachDeletionSplit(TypeIdsVisitor visitor) {
            forEachDeletionSplitInterruptible(byType -> {
                visitor.accept(byType);
                return false;
            });
        }

        /**
         * Can be interrupted earlier if the provided visitor decides to, instead of forcing it to be exhausted.
         * @see #forEachCreationSplit(TypeIdsVisitor)
         */
        void forEachDeletionSplitInterruptible(InterruptibleTypeIdsVisitor visitor);
    }

    interface NodeRelationshipTypeIds {
        /**
         * @return the relationship type for the relationships provided by this instance.
         */
        int type();

        /**
         * @return the relationships of the given direction.
         */
        default RelationshipBatch ids(RelationshipDirection direction) {
            return switch (direction) {
                case OUTGOING -> out();
                case INCOMING -> in();
                case LOOP -> loop();
            };
        }

        /**
         * @return whether or not there are any {@link RelationshipDirection#OUTGOING outgoing} relationships for this node and type.
         */
        boolean hasOut();

        /**
         * @return whether or not there are any {@link RelationshipDirection#INCOMING incoming} relationships for this node and type.
         */
        boolean hasIn();

        /**
         * @return whether or not there are any {@link RelationshipDirection#LOOP loop} relationships for this node and type.
         */
        boolean hasLoop();

        /**
         * @return all {@link RelationshipDirection#OUTGOING outgoing} relationships for this node and type.
         */
        RelationshipBatch out();

        /**
         * @return all {@link RelationshipDirection#INCOMING incoming} relationships for this node and type.
         */
        RelationshipBatch in();

        /**
         * @return all {@link RelationshipDirection#LOOP loop} relationships for this node and type.
         */
        RelationshipBatch loop();
    }

    interface RelationshipBatch {
        /**
         * @return number of relationships in this batch.
         */
        int size();

        /**
         * Allows {@link RelationshipVisitor} to visit all relationships in this batch.
         * @param relationship the visitor.
         * @param <E> type of exception visitor can throw.
         * @throws E on visitor error.
         */
        <E extends Exception> void forEach(RelationshipVisitorWithProperties<E> relationship) throws E;

        // The default implementations below are inefficient, but are implemented like this for simplicity of test
        // versions of this interface,
        // any implementor that is in production code will implement properly

        /**
         * @return whether or not this batch is empty.
         */
        default boolean isEmpty() {
            return size() == 0;
        }

        /**
         * @param id relationship id the check.
         * @return whether or not this batch contains the given relationship id.
         */
        default boolean contains(long id) {
            if (isEmpty()) {
                return false;
            }
            MutableBoolean contains = new MutableBoolean();
            forEach((relationshipId, typeId, startNodeId, endNodeId, addedProperties) -> {
                if (relationshipId == id) {
                    contains.setTrue();
                }
            });
            return contains.booleanValue();
        }

        /**
         * @return the first relationship ID in this batch.
         * @throws IllegalStateException if this batch is empty.
         */
        default long first() {
            Preconditions.checkState(!isEmpty(), "No ids");
            MutableLong first = new MutableLong(-1);
            forEach((relationshipId, typeId, startNodeId, endNodeId, addedProperties) -> {
                if (first.longValue() == -1) {
                    first.setValue(relationshipId);
                }
            });
            return first.longValue();
        }
    }

    /**
     * A way to carry tx state information lookup about relationships into places that needs it. In a way it's a very specific sub-interface of a bigger
     * transaction state interface which isn't really available in this component.
     */
    interface IdDataDecorator {
        /**
         * Allows visitor to get more data about the relationship of the given id.
         * @param id the relationship id.
         * @param visitor to receive the relationship data.
         * @param <E> type of exception visitor can throw.
         * @throws E on visitor error.
         */
        <E extends Exception> void accept(long id, RelationshipVisitorWithProperties<E> visitor) throws E;
    }
}
