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
package org.neo4j.kernel.impl.api.state;

import static org.neo4j.collection.PrimitiveLongCollections.concat;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.storageengine.api.txstate.RelationshipModifications.EMPTY_BATCH;
import static org.neo4j.storageengine.api.txstate.RelationshipModifications.idsAsBatch;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.function.ThrowingLongConsumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

/**
 * Maintains relationships that have been added for a specific node.
 * <p/>
 * This class is not a trustworthy source of information unless you are careful - it does not, for instance, remove
 * rels if they are added and then removed in the same tx. It trusts wrapping data structures for that filtering.
 */
public class RelationshipChangesForNode {
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(RelationshipChangesForNode.class);

    /**
     * Allows this data structure to work both for tracking removals and additions.
     */
    public enum DiffStrategy {
        REMOVE {
            @Override
            int augmentDegree(int degree, int diff) {
                return degree - diff;
            }
        },
        ADD {
            @Override
            int augmentDegree(int degree, int diff) {
                return degree + diff;
            }
        };

        abstract int augmentDegree(int degree, int diff);
    }

    private final DiffStrategy diffStrategy;
    private final MemoryTracker memoryTracker;
    private final MutableIntObjectMap<RelationshipSetsByDirection> byType;

    static RelationshipChangesForNode createRelationshipChangesForNode(
            DiffStrategy diffStrategy, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new RelationshipChangesForNode(diffStrategy, memoryTracker);
    }

    private RelationshipChangesForNode(DiffStrategy diffStrategy, MemoryTracker memoryTracker) {
        this.diffStrategy = diffStrategy;
        this.memoryTracker = memoryTracker;
        this.byType = HeapTrackingCollections.newIntObjectHashMap(memoryTracker);
    }

    public void addRelationship(long relId, int typeId, RelationshipDirection direction) {
        byType.getIfAbsentPutWithKey(typeId, RelationshipSetsByDirection::new)
                .getOrCreateIds(direction)
                .add(relId);
    }

    public boolean removeRelationship(long relId, int typeId, RelationshipDirection direction) {
        RelationshipSetsByDirection byDirection = byType.get(typeId);
        if (byDirection != null) {
            MutableLongSet ids = byDirection.getIds(direction);
            if (ids != null) {
                if (ids.remove(relId)) {
                    if (ids.isEmpty()) {
                        byDirection.deleteIds(direction);
                        if (byDirection.isEmpty()) {
                            byType.remove(typeId);
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public int augmentDegree(RelationshipDirection direction, int degree, int typeId) {
        return diffStrategy.augmentDegree(degree, degreeDiff(typeId, direction));
    }

    private int degreeDiff(int type, RelationshipDirection direction) {
        RelationshipSetsByDirection byDirection = byType.get(type);
        if (byDirection != null) {
            MutableLongSet ids = byDirection.getIds(direction);
            if (ids != null) {
                return ids.size();
            }
        }
        return 0;
    }

    public void clear() {
        byType.clear();
    }

    boolean isEmpty() {
        return byType.isEmpty();
    }

    public LongIterator getRelationships() {
        return aggregatedIds(
                RelationshipDirection.INCOMING, RelationshipDirection.OUTGOING, RelationshipDirection.LOOP);
    }

    private static LongIterator nonEmptyConcat(LongIterator... primitiveIds) {
        return concat(filter(ids -> ids != ImmutableEmptyLongIterator.INSTANCE, iterator(primitiveIds)));
    }

    public LongIterator getRelationships(Direction direction) {
        return switch (direction) {
            case INCOMING -> aggregatedIds(RelationshipDirection.INCOMING, RelationshipDirection.LOOP);
            case OUTGOING -> aggregatedIds(RelationshipDirection.OUTGOING, RelationshipDirection.LOOP);
            case BOTH -> aggregatedIds(
                    RelationshipDirection.INCOMING, RelationshipDirection.OUTGOING, RelationshipDirection.LOOP);
        };
    }

    private LongIterator aggregatedIds(RelationshipDirection... directions) {
        List<LongIterator> iterators = new ArrayList<>();
        for (RelationshipSetsByDirection byDirection : byType) {
            for (RelationshipDirection direction : directions) {
                addIdIterator(iterators, byDirection, direction);
            }
        }
        return iterators.isEmpty()
                ? ImmutableEmptyLongIterator.INSTANCE
                : nonEmptyConcat(iterators.toArray(LongIterator[]::new));
    }

    private void addIdIterator(
            List<LongIterator> iterators, RelationshipSetsByDirection byDirection, RelationshipDirection direction) {
        MutableLongSet ids = byDirection.getIds(direction);
        if (ids != null) {
            iterators.add(primitiveIds(ids));
        }
    }

    public LongIterator getRelationships(Direction direction, int type) {
        RelationshipSetsByDirection typeSets = byType.get(type);
        if (typeSets == null) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        MutableLongSet loops = typeSets.getIds(RelationshipDirection.LOOP);
        switch (direction) {
            case INCOMING -> {
                MutableLongSet incoming = typeSets.getIds(RelationshipDirection.INCOMING);
                return incoming == null && loops == null
                        ? ImmutableEmptyLongIterator.INSTANCE
                        : nonEmptyConcat(primitiveIds(incoming), primitiveIds(loops));
            }
            case OUTGOING -> {
                MutableLongSet outgoing = typeSets.getIds(RelationshipDirection.OUTGOING);
                return outgoing == null && loops == null
                        ? ImmutableEmptyLongIterator.INSTANCE
                        : nonEmptyConcat(primitiveIds(outgoing), primitiveIds(loops));
            }
            case BOTH -> {
                MutableLongSet incoming = typeSets.getIds(RelationshipDirection.INCOMING);
                MutableLongSet outgoing = typeSets.getIds(RelationshipDirection.OUTGOING);
                return nonEmptyConcat(primitiveIds(outgoing), primitiveIds(incoming), primitiveIds(loops));
            }
            default -> throw new IllegalArgumentException("Unknown direction: " + direction);
        }
    }

    public LongIterator getRelationships(RelationshipDirection direction, int type) {
        RelationshipSetsByDirection typeSets = byType.get(type);
        if (typeSets == null) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        MutableLongSet set = typeSets.getIds(direction);
        return set == null ? ImmutableEmptyLongIterator.INSTANCE : primitiveIds(set);
    }

    public boolean hasRelationships(int type) {
        RelationshipSetsByDirection byDirection = byType.get(type);
        if (byDirection != null) {
            return !byDirection.isEmpty();
        }
        return false;
    }

    public IntSet relationshipTypes() {
        return byType.keySet();
    }

    private static LongIterator primitiveIds(LongSet relationships) {
        return relationships == null
                ? ImmutableEmptyLongIterator.INSTANCE
                : relationships.freeze().longIterator();
    }

    <E extends Exception> void visitIds(ThrowingLongConsumer<E> visitor) throws E {
        for (RelationshipSetsByDirection typeSets : byType) {
            if (typeSets.ids != null) {
                for (MutableLongSet ids : typeSets.ids) {
                    if (ids != null) {
                        for (MutableLongIterator idIterator = ids.longIterator(); idIterator.hasNext(); ) {
                            visitor.accept(idIterator.next());
                        }
                    }
                }
            }
        }
    }

    void visitIdsSplit(
            RelationshipModifications.InterruptibleTypeIdsVisitor idsByType,
            RelationshipModifications.IdDataDecorator idDataDecorator) {
        for (RelationshipSetsByDirection typeSets : byType) {
            if (idsByType.test(new IdsByType(typeSets, idDataDecorator))) {
                break;
            }
        }
    }

    int totalCount() {
        int count = 0;
        for (RelationshipSetsByDirection byDirection : byType) {
            count += count(byDirection, RelationshipDirection.OUTGOING);
            count += count(byDirection, RelationshipDirection.INCOMING);
            count += count(byDirection, RelationshipDirection.LOOP);
        }
        return count;
    }

    private int count(RelationshipSetsByDirection byDirection, RelationshipDirection direction) {
        MutableLongSet ids = byDirection.getIds(direction);
        return ids != null ? ids.size() : 0;
    }

    private static final class IdsByType implements RelationshipModifications.NodeRelationshipTypeIds {
        private final RelationshipSetsByDirection byDirection;
        private final RelationshipModifications.IdDataDecorator idDataDecorator;

        IdsByType(RelationshipSetsByDirection byDirection, RelationshipModifications.IdDataDecorator idDataDecorator) {
            this.byDirection = byDirection;
            this.idDataDecorator = idDataDecorator;
        }

        @Override
        public int type() {
            return byDirection.type;
        }

        @Override
        public boolean hasOut() {
            return has(RelationshipDirection.OUTGOING);
        }

        @Override
        public boolean hasIn() {
            return has(RelationshipDirection.INCOMING);
        }

        @Override
        public boolean hasLoop() {
            return has(RelationshipDirection.LOOP);
        }

        @Override
        public RelationshipBatch out() {
            return idBatch(RelationshipDirection.OUTGOING);
        }

        @Override
        public RelationshipBatch in() {
            return idBatch(RelationshipDirection.INCOMING);
        }

        @Override
        public RelationshipBatch loop() {
            return idBatch(RelationshipDirection.LOOP);
        }

        private RelationshipBatch idBatch(RelationshipDirection direction) {
            MutableLongSet ids = byDirection.getIds(direction);
            return ids != null ? idsAsBatch(ids, idDataDecorator) : EMPTY_BATCH;
        }

        private boolean has(RelationshipDirection direction) {
            return byDirection.getIds(direction) != null;
        }
    }

    private class RelationshipSetsByDirection {
        private final int type;
        private MutableLongSet[] ids;

        RelationshipSetsByDirection(int type) {
            this.type = type;
        }

        MutableLongSet getIds(RelationshipDirection direction) {
            return ids == null ? null : ids[direction.ordinal()];
        }

        MutableLongSet getOrCreateIds(RelationshipDirection direction) {
            int index = direction.ordinal();
            if (ids == null) {
                ids = new MutableLongSet[3];
            }
            if (ids[index] == null) {
                ids[index] = HeapTrackingCollections.newLongSet(memoryTracker);
            }
            return ids[index];
        }

        void deleteIds(RelationshipDirection direction) {
            assert ids[direction.ordinal()].isEmpty();
            ids[direction.ordinal()] = null;
        }

        boolean isEmpty() {
            if (ids != null) {
                for (MutableLongSet set : ids) {
                    if (set != null) {
                        if (!set.isEmpty()) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
