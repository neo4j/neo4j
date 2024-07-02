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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * {@link RelationshipTypeIndexCursor} which is node-based, i.e. the IDs driving the cursor are node IDs that contain
 * relationships of types we're interested in. For each node ID that we get from the underlying lookup index use the node cursor
 * to go there and read the relationships of the given type and iterate over those, then go to the next node ID from the lookup index, a.s.o.
 * We do not support ordered result as it is impossible to do so when traversing the nodes for relationship ids.
 * @see StorageEngineIndexingBehaviour
 */
public class DefaultNodeBasedRelationshipTypeIndexCursor
        extends IndexCursor<IndexProgressor, DefaultNodeBasedRelationshipTypeIndexCursor>
        implements InternalRelationshipTypeIndexCursor {
    private final DefaultNodeCursor nodeCursor;
    private final DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private Read read;
    private TxStateHolder txStateHolder;
    private AccessModeProvider accessModeProvider;
    private LongIterator addedRelationships;
    private LongSet removedNodes;
    private int type;
    private long relId = LongReference.NULL;
    private RelationshipSelection selection;
    private long nodeFromIndex;
    private ReadState readState;

    DefaultNodeBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultNodeBasedRelationshipTypeIndexCursor> pool,
            DefaultNodeCursor nodeCursor,
            DefaultRelationshipTraversalCursor relationshipTraversalCursor) {
        super(pool);
        this.nodeCursor = nodeCursor;
        this.relationshipTraversalCursor = relationshipTraversalCursor;
    }

    @Override
    public void initializeQuery(IndexProgressor progressor, int type, IndexOrder order) {
        LongIterator addedRelationships = null;
        LongSet removedNodes = null;
        if (txStateHolder.hasTxStateWithChanges()) {
            addedRelationships = txStateHolder
                    .txState()
                    .relationshipsWithTypeChanged(type)
                    .getAdded()
                    .freeze()
                    .longIterator();
            removedNodes =
                    txStateHolder.txState().addedAndRemovedNodes().getRemoved().freeze();
        }
        initializeQuery(progressor, type, addedRelationships, removedNodes);
    }

    @Override
    public void initializeQuery(
            IndexProgressor progressor, int type, LongIterator addedRelationships, LongSet removedNodes) {
        super.initialize(progressor);
        this.type = type;
        this.selection = RelationshipSelection.selection(type, Direction.OUTGOING);
        this.addedRelationships = addedRelationships; // To return from TX state
        this.removedNodes = removedNodes; // To check from index hits
        this.readState = addedRelationships != null ? ReadState.TXSTATE_READ : ReadState.INDEX_READ;

        if (tracer != null) {
            tracer.onRelationshipTypeScan(type);
        }
    }

    @Override
    public boolean acceptEntity(long nodeId, int type) {
        if (type != this.type) {
            return false;
        }
        if (removedNodes != null && removedNodes.contains(nodeId)) {
            return false;
        }
        nodeFromIndex = nodeId;
        return true;
    }

    @Override
    public boolean isClosed() {
        return isProgressorClosed();
    }

    @Override
    public boolean next() {
        boolean hasNext = innerNext();
        if (hasNext && tracer != null) {
            tracer.onRelationship(relId);
        }
        return hasNext;
    }

    private boolean innerNext() {
        while (readState != ReadState.UNAVAILABLE) {
            switch (readState) {
                case TXSTATE_READ -> {
                    while (addedRelationships.hasNext()) {
                        long id = addedRelationships.next();
                        // Position cursor on the rel from tx state
                        relationshipTraversalCursor.init(id, read, txStateHolder, accessModeProvider);
                        if (relationshipTraversalCursor.next()) {
                            relId = id;
                            return true;
                        }
                    }
                    readState = ReadState.INDEX_READ;
                }
                case INDEX_READ ->
                // indexNext() calls acceptEntity() with data from index
                readState = indexNext() ? ReadState.NODE_READ : ReadState.UNAVAILABLE;
                case NODE_READ -> {
                    nodeCursor.single(nodeFromIndex, read, txStateHolder, accessModeProvider);
                    if (nodeCursor.next()) {
                        nodeCursor.relationships(relationshipTraversalCursor, selection);
                        readState = ReadState.RELATIONSHIP_READ;
                    } else {
                        readState = ReadState.INDEX_READ;
                    }
                }
                case RELATIONSHIP_READ -> {
                    while (relationshipTraversalCursor.next()) {
                        // Since we check tx state separately, lets not return them here!
                        if (relationshipTraversalCursor.currentAddedInTx == LongReference.NULL) {
                            relId = relationshipTraversalCursor.relationshipReference();
                            return true;
                        }
                    }
                    readState = ReadState.INDEX_READ;
                }
            }
        }
        relId = LongReference.NULL;
        return false;
    }

    @Override
    public float score() {
        return Float.NaN;
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        checkReadFromStore();
        relationshipTraversalCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        checkReadFromStore();
        return relationshipTraversalCursor.propertiesReference();
    }

    @Override
    public long relationshipReference() {
        return relId;
    }

    @Override
    public int type() {
        return type;
    }

    @Override
    public void source(NodeCursor cursor) {
        read.singleNode(sourceNodeReference(), cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        read.singleNode(targetNodeReference(), cursor);
    }

    @Override
    public long sourceNodeReference() {
        checkReadFromStore();
        return relationshipTraversalCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        checkReadFromStore();
        return relationshipTraversalCursor.targetNodeReference();
    }

    @Override
    public boolean readFromStore() {
        // We've already ready from store in innerNext(), or placed cursor on tx state data
        return relId != LongReference.NULL;
    }

    @Override
    public void initState(Read read, TxStateHolder txStateHolder, AccessModeProvider accessModeProvider) {
        this.read = read;
        this.txStateHolder = txStateHolder;
        this.accessModeProvider = accessModeProvider;
    }

    private void checkReadFromStore() {
        if (relationshipTraversalCursor.relationshipReference() != relId) {
            throw new IllegalStateException("Relationship hasn't been read from store");
        }
    }

    @Override
    public void release() {
        nodeCursor.close();
        nodeCursor.release();
        relationshipTraversalCursor.close();
        relationshipTraversalCursor.release();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            closeProgressor();
            read = null;
            txStateHolder = null;
            accessModeProvider = null;
            nodeCursor.close();
            relationshipTraversalCursor.close();
            relId = LongReference.NULL;
            nodeFromIndex = LongReference.NULL;
            readState = ReadState.UNAVAILABLE;
        }
        super.closeInternal();
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "RelationshipTypeIndexCursor[closed state, node based]";
        } else {
            return String.format(
                    "RelationshipTypeIndexCursor[relationship=%s, state=%s, node based]",
                    relationshipReference(), readState);
        }
    }

    private enum ReadState {
        TXSTATE_READ,
        INDEX_READ,
        NODE_READ,
        RELATIONSHIP_READ,
        UNAVAILABLE
    }
}
