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

import static java.util.Collections.emptyList;
import static org.neo4j.collection.diffset.TrackableDiffSets.newMutableLongDiffSets;
import static org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.createRelationshipChangesForNode;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.collection.diffset.MutableLongDiffSets;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;
import org.neo4j.values.storable.Value;

class NodeStateImpl extends EntityStateImpl implements NodeState {
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(NodeStateImpl.class);

    static final NodeState EMPTY = new NodeState() {
        @Override
        public Iterable<StorageProperty> addedProperties() {
            return emptyList();
        }

        @Override
        public Iterable<StorageProperty> changedProperties() {
            return emptyList();
        }

        @Override
        public IntIterable removedProperties() {
            return IntSets.immutable.empty();
        }

        @Override
        public Iterable<StorageProperty> addedAndChangedProperties() {
            return emptyList();
        }

        @Override
        public boolean hasPropertyChanges() {
            return false;
        }

        @Override
        public LongDiffSets labelDiffSets() {
            return LongDiffSets.EMPTY;
        }

        @Override
        public boolean fillDegrees(RelationshipSelection selection, Degrees.Mutator degree) {
            // do nothing
            return true;
        }

        @Override
        public long getId() {
            throw new UnsupportedOperationException("id not defined");
        }

        @Override
        public boolean isPropertyChangedOrRemoved(int propertyKey) {
            return false;
        }

        @Override
        public boolean isPropertyAdded(int propertyKey) {
            return false;
        }

        @Override
        public Value propertyValue(int propertyKey) {
            return null;
        }

        @Override
        public LongIterator getAddedRelationships() {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public LongIterator getAddedRelationships(Direction direction) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public LongIterator getAddedRelationships(Direction direction, int relType) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public LongIterator getAddedRelationships(RelationshipDirection direction, int relType) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public IntIterable getAddedRelationshipTypes() {
            return IntSets.immutable.empty();
        }

        @Override
        public IntIterable getAddedAndRemovedRelationshipTypes() {
            return IntSets.immutable.empty();
        }
    };
    private final boolean addedInThisBatch;

    private MutableLongDiffSets labelDiffSets;
    private RelationshipChangesForNode relationshipsAdded;
    private RelationshipChangesForNode relationshipsRemoved;
    private boolean deleted;

    static NodeStateImpl createNodeState(
            long id, boolean addedInThisBatch, CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new NodeStateImpl(id, addedInThisBatch, collectionsFactory, memoryTracker);
    }

    private NodeStateImpl(
            long id, boolean addedInThisBatch, CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        super(id, collectionsFactory, memoryTracker);
        this.addedInThisBatch = addedInThisBatch;
    }

    @Override
    public LongDiffSets labelDiffSets() {
        return labelDiffSets == null ? LongDiffSets.EMPTY : labelDiffSets;
    }

    MutableLongDiffSets getOrCreateLabelDiffSets() {
        if (labelDiffSets == null) {
            labelDiffSets = newMutableLongDiffSets(collectionsFactory, memoryTracker);
        }
        return labelDiffSets;
    }

    public void addRelationship(long relId, int typeId, RelationshipDirection direction) {
        if (!hasAddedRelationships()) {
            relationshipsAdded = createRelationshipChangesForNode(DiffStrategy.ADD, memoryTracker);
        }
        relationshipsAdded.addRelationship(relId, typeId, direction);
    }

    public void removeRelationship(long relId, int typeId, RelationshipDirection direction) {
        if (hasAddedRelationships()) {
            if (relationshipsAdded.removeRelationship(relId, typeId, direction)) {
                // This was a rel that was added in this tx, no need to add it to the remove list, instead we just
                // remove it from added relationships.
                if (relationshipsAdded.isEmpty()) {
                    relationshipsAdded = null;
                }
                return;
            }
        }
        if (!hasRemovedRelationships()) {
            relationshipsRemoved = createRelationshipChangesForNode(DiffStrategy.REMOVE, memoryTracker);
        }
        relationshipsRemoved.addRelationship(relId, typeId, direction);
    }

    @Override
    public void clear() {
        super.clear();
        // Intentionally don't clear the relationships because we need those grouped per node in command creation
        // Even the added relationships we need to know when to add to the removed set in some cases
        if (labelDiffSets != null) {
            labelDiffSets = null;
        }
    }

    private int augmentDegree(RelationshipDirection direction, int typeId) {
        int degree = 0;
        if (hasAddedRelationships()) {
            degree = relationshipsAdded.augmentDegree(direction, degree, typeId);
        }
        if (hasRemovedRelationships()) {
            degree = relationshipsRemoved.augmentDegree(direction, degree, typeId);
        }
        return degree;
    }

    @Override
    public boolean fillDegrees(RelationshipSelection selection, Degrees.Mutator degrees) {
        IntIterator txTypes = getAddedAndRemovedRelationshipTypes().intIterator();
        while (txTypes.hasNext()) {
            int type = txTypes.next();
            if (selection.test(type)) {
                int outgoing = selection.test(RelationshipDirection.OUTGOING)
                        ? augmentDegree(RelationshipDirection.OUTGOING, type)
                        : 0;
                int incoming = selection.test(RelationshipDirection.INCOMING)
                        ? augmentDegree(RelationshipDirection.INCOMING, type)
                        : 0;
                int loop = selection.test(RelationshipDirection.LOOP)
                        ? augmentDegree(RelationshipDirection.LOOP, type)
                        : 0;
                if (!degrees.add(type, outgoing, incoming, loop)) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean hasAddedRelationships() {
        return relationshipsAdded != null;
    }

    public boolean hasAddedRelationships(int type) {
        return relationshipsAdded != null && relationshipsAdded.hasRelationships(type);
    }

    boolean hasRemovedRelationships() {
        return relationshipsRemoved != null && !relationshipsRemoved.isEmpty();
    }

    @Override
    public LongIterator getAddedRelationships() {
        return relationshipsAdded != null ? relationshipsAdded.getRelationships() : ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public LongIterator getAddedRelationships(Direction direction) {
        return relationshipsAdded != null
                ? relationshipsAdded.getRelationships(direction)
                : ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public LongIterator getAddedRelationships(Direction direction, int relType) {
        return relationshipsAdded != null
                ? relationshipsAdded.getRelationships(direction, relType)
                : ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public LongIterator getAddedRelationships(RelationshipDirection direction, int relType) {
        return relationshipsAdded != null
                ? relationshipsAdded.getRelationships(direction, relType)
                : ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public IntIterable getAddedRelationshipTypes() {
        return relationshipsAdded != null ? relationshipsAdded.relationshipTypes() : IntSets.immutable.empty();
    }

    @Override
    public IntIterable getAddedAndRemovedRelationshipTypes() {
        if (relationshipsAdded == null && relationshipsRemoved == null) {
            return IntSets.immutable.empty();
        }
        if (relationshipsAdded != null && relationshipsRemoved != null) {
            MutableIntSet types = IntSets.mutable.withAll(relationshipsAdded.relationshipTypes());
            types.addAll(relationshipsRemoved.relationshipTypes());
            return types;
        }
        return relationshipsAdded != null
                ? relationshipsAdded.relationshipTypes()
                : relationshipsRemoved.relationshipTypes();
    }

    RelationshipBatch additionsAsRelationshipBatch(RelationshipModifications.IdDataDecorator decorator) {
        return new RelationshipBatchImpl(relationshipsAdded, decorator);
    }

    RelationshipBatch removalsAsRelationshipBatch(RelationshipModifications.IdDataDecorator decorator) {
        return new RelationshipBatchImpl(relationshipsRemoved, decorator);
    }

    void visitAddedIdsSplit(
            RelationshipModifications.InterruptibleTypeIdsVisitor visitor,
            RelationshipModifications.IdDataDecorator idDataDecorator) {
        if (hasAddedRelationships()) {
            relationshipsAdded.visitIdsSplit(visitor, idDataDecorator);
        }
    }

    void visitRemovedIdsSplit(RelationshipModifications.InterruptibleTypeIdsVisitor visitor) {
        if (hasRemovedRelationships()) {
            relationshipsRemoved.visitIdsSplit(visitor, RelationshipModifications.noAdditionalDataDecorator());
        }
    }

    boolean isDeleted() {
        return deleted;
    }

    boolean isAddedInThisBatch() {
        return addedInThisBatch;
    }

    void markAsDeleted() {
        this.deleted = true;
        clear();
    }

    private static class RelationshipBatchImpl implements RelationshipBatch {
        private final RelationshipChangesForNode relationships;
        private final RelationshipModifications.IdDataDecorator decorator;

        RelationshipBatchImpl(
                RelationshipChangesForNode relationships, RelationshipModifications.IdDataDecorator decorator) {
            this.relationships = relationships;
            this.decorator = decorator;
        }

        @Override
        public int size() {
            return relationships != null ? relationships.totalCount() : 0;
        }

        @Override
        public boolean isEmpty() {
            return relationships == null;
        }

        @Override
        public <E extends Exception> void forEach(RelationshipVisitorWithProperties<E> relationship) throws E {
            relationships.visitIds(id -> decorator.accept(id, relationship));
        }
    }
}
