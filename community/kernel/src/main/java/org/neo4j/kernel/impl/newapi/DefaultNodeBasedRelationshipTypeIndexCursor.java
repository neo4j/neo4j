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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * {@link RelationshipTypeIndexCursor} which is node-based, i.e. the IDs driving the cursor are node IDs that contain
 * relationships of types we're interested in. For each node ID that we get from the underlying lookup index use the node cursor
 * to go there and read the relationships of the given type and iterate over those, then go to the next node ID from the lookup index, a.s.o.
 * @see StorageEngineIndexingBehaviour
 */
public class DefaultNodeBasedRelationshipTypeIndexCursor extends DefaultRelationshipTypeIndexCursor
        implements RelationshipTypeIndexCursor {

    private final DefaultNodeCursor nodeCursor;
    private final DefaultRelationshipTraversalCursor relationshipTraversalCursor;

    private IndexReadState indexReadState = IndexReadState.UNAVAILABLE;
    private RelationshipSelection selection;

    DefaultNodeBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultRelationshipTypeIndexCursor> pool,
            DefaultNodeCursor nodeCursor,
            DefaultRelationshipTraversalCursor relationshipTraversalCursor) {
        super(pool);
        this.nodeCursor = nodeCursor;
        this.relationshipTraversalCursor = relationshipTraversalCursor;
    }

    @Override
    public void initialize(IndexProgressor progressor, int token, IndexOrder order) {
        indexReadState = IndexReadState.INDEX_READ;
        selection = RelationshipSelection.selection(token, Direction.OUTGOING);
        // parent call will initialise the added/removed tx state.
        // this is required as state must be determined on initialisation and NOT on first call to next
        super.initialize(progressor, token, IndexOrder.NONE);
    }

    @Override
    public void initialize(
            IndexProgressor progressor, int token, LongIterator added, LongSet removed, AccessMode accessMode) {
        indexReadState = IndexReadState.INDEX_READ;
        selection = RelationshipSelection.selection(token, Direction.OUTGOING);
        super.initialize(progressor, token, added, removed, accessMode);
    }

    @Override
    public boolean readFromStore() {
        if (relationshipTraversalCursor.relationshipReference() == entity) {
            // previous call to this method for this relationship already seems to have loaded this relationship
            return true;
        }

        if (entityFromIndex != NO_ID) {
            nodeCursor.single(entityFromIndex, read);
            if (nodeCursor.next()) {
                nodeCursor.relationships(relationshipTraversalCursor, selection);
                final var next = relationshipTraversalCursor.next();
                indexReadState = next ? IndexReadState.RELATIONSHIP_READ : IndexReadState.INDEX_READ;
                return next;
            }
        }

        return false;
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
    public void properties(PropertyCursor cursor, PropertySelection propSelection) {
        checkReadFromStore();
        relationshipTraversalCursor.properties(cursor, propSelection);
    }

    @Override
    public Reference propertiesReference() {
        checkReadFromStore();
        return relationshipTraversalCursor.propertiesReference();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            indexReadState = IndexReadState.UNAVAILABLE;
            nodeCursor.close();
            relationshipTraversalCursor.close();
        }
        super.closeInternal();
    }

    @Override
    public void release() {
        nodeCursor.close();
        nodeCursor.release();
        relationshipTraversalCursor.close();
        relationshipTraversalCursor.release();
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "RelationshipTypeIndexCursor[closed state, node based]";
        } else {
            return "RelationshipTypeIndexCursor[relationship=" + relationshipReference() + ", node based]";
        }
    }

    @Override
    protected long nextEntity() {
        return relationshipTraversalCursor.relationshipReference();
    }

    @Override
    protected boolean innerNext() {
        while (true) {
            switch (indexReadState) {
                case INDEX_READ -> indexReadState = indexNext() ? IndexReadState.NODE_READ : IndexReadState.UNAVAILABLE;
                case NODE_READ -> {
                    nodeCursor.single(entityFromIndex, read);
                    if (nodeCursor.next()) {
                        nodeCursor.relationships(relationshipTraversalCursor, selection);
                        indexReadState = IndexReadState.RELATIONSHIP_READ;
                    } else {
                        indexReadState = IndexReadState.INDEX_READ;
                    }
                }
                case RELATIONSHIP_READ -> {
                    while (relationshipTraversalCursor.next()) {
                        if (relationshipTraversalCursor.currentAddedInTx == NO_ID) {
                            return true;
                        }
                    }
                    indexReadState = IndexReadState.INDEX_READ;
                }
                case UNAVAILABLE -> {
                    return false;
                }
            }
        }
    }

    @Override
    protected LongIterator createAddedInTxState(TransactionState txState, int token, IndexOrder order) {
        final var relationships = read.txState()
                .relationshipsWithTypeChanged(token)
                .getAdded()
                .freeze()
                .longIterator();
        return new LongIterator() {
            @Override
            public boolean hasNext() {
                return relationships.hasNext();
            }

            @Override
            public long next() {
                final var relationship = relationships.next();
                // need to position the traversal cursor now we've found a relationship from the tx
                // this is required to allow further access calls (ex readFromStore, sourceNodeReference, etc)
                relationshipTraversalCursor.init(relationship, read);
                // bail if the worst happens and the added relationships were incorrect to start with
                return relationshipTraversalCursor.next() ? relationship : NO_ID;
            }
        };
    }

    @Override
    protected LongSet createDeletedInTxState(TransactionState txState, int token) {
        return read.txState().addedAndRemovedNodes().getRemoved().freeze();
    }

    @Override
    protected boolean allowedToSeeEntity(AccessMode accessMode, long entityReference) {
        // Security is managed by the internal node/relationshipTraversal cursors, so we don't need to do any additional
        // checks here.
        return true;
    }

    private void checkReadFromStore() {
        if (relationshipTraversalCursor.relationshipReference() != entity) {
            throw new IllegalStateException("Relationship hasn't been read from store");
        }
    }

    private enum IndexReadState {
        INDEX_READ,
        NODE_READ,
        RELATIONSHIP_READ,
        UNAVAILABLE
    }
}
