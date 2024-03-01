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

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.values.storable.Value;

class RelationshipStateImpl extends EntityStateImpl implements RelationshipState {
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(RelationshipStateImpl.class);

    static final RelationshipState EMPTY = new RelationshipState() {
        @Override
        public long getId() {
            throw new UnsupportedOperationException("id not defined");
        }

        @Override
        public int getType() {
            throw new UnsupportedOperationException("type not defined");
        }

        @Override
        public <EX extends Exception> boolean accept(RelationshipVisitor<EX> visitor) {
            return false;
        }

        @Override
        public <EX extends Exception> boolean accept(RelationshipVisitorWithProperties<EX> visitor) throws EX {
            return false;
        }

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
        public boolean isPropertyChangedOrRemoved(int propertyKey) {
            return false;
        }

        @Override
        public Value propertyValue(int propertyKey) {
            return null;
        }
    };

    private final long startNode;
    private final long endNode;
    private final int type;
    private boolean deleted;

    static RelationshipStateImpl createRelationshipStateImpl(
            long id,
            int type,
            long startNode,
            long endNode,
            CollectionsFactory collectionsFactory,
            MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new RelationshipStateImpl(id, type, startNode, endNode, collectionsFactory, memoryTracker);
    }

    private RelationshipStateImpl(
            long id,
            int type,
            long startNode,
            long endNode,
            CollectionsFactory collectionsFactory,
            MemoryTracker memoryTracker) {
        super(id, collectionsFactory, memoryTracker);
        this.type = type;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    void setDeleted() {
        this.deleted = true;
    }

    boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public <EX extends Exception> boolean accept(RelationshipVisitor<EX> visitor) throws EX {
        if (type != -1) {
            visitor.visit(getId(), type, startNode, endNode);
            return true;
        }
        return false;
    }

    @Override
    public <EX extends Exception> boolean accept(RelationshipVisitorWithProperties<EX> visitor) throws EX {
        if (type != -1) {
            visitor.visit(getId(), type, startNode, endNode, addedProperties());
            return true;
        }
        return false;
    }
}
