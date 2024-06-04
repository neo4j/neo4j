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
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

/**
 * {@link RelationshipTypeIndexCursor} which is relationship-based, i.e. the IDs driving the cursor are relationship IDs.
 * @see StorageEngineIndexingBehaviour
 */
public class DefaultRelationshipBasedRelationshipTypeIndexCursor
        extends DefaultEntityTokenIndexCursor<DefaultRelationshipBasedRelationshipTypeIndexCursor>
        implements InternalRelationshipTypeIndexCursor {

    private final DefaultRelationshipScanCursor relationshipScanCursor;
    private final boolean applyAccessModeToTxState;

    DefaultRelationshipBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultRelationshipBasedRelationshipTypeIndexCursor> pool,
            DefaultRelationshipScanCursor relationshipScanCursor,
            boolean applyAccessModeToTxState) {
        super(pool, applyAccessModeToTxState);
        this.relationshipScanCursor = relationshipScanCursor;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public final int type() {
        return tokenId;
    }

    @Override
    public final float score() {
        return Float.NaN;
    }

    @Override
    public long relationshipReference() {
        return entityReference();
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
    protected final boolean allowedToSeeAllEntitiesWithToken(int token) {
        AccessMode accessMode = read.getAccessMode();
        return accessMode.allowsTraverseRelType(token) && accessMode.allowsTraverseAllLabels();
    }

    @Override
    protected void traceNext(KernelReadTracer tracer, long entity) {
        tracer.onRelationship(entity);
    }

    @Override
    protected final void traceScan(KernelReadTracer tracer, int token) {
        tracer.onRelationshipTypeScan(token);
    }

    @Override
    public long sourceNodeReference() {
        checkReadFromStore();
        return relationshipScanCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        checkReadFromStore();
        return relationshipScanCursor.targetNodeReference();
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        checkReadFromStore();
        relationshipScanCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        checkReadFromStore();
        return relationshipScanCursor.propertiesReference();
    }

    @Override
    public boolean readFromStore() {
        if (relationshipScanCursor.relationshipReference() == entity) {
            // A security check, or a previous call to this method for this relationship already seems to have loaded
            // this relationship
            return true;
        }

        relationshipScanCursor.single(entity, read);
        return relationshipScanCursor.next();
    }

    @Override
    public void release() {
        relationshipScanCursor.close();
        relationshipScanCursor.release();
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "RelationshipTypeIndexCursor[closed state, relationship based]";
        } else {
            return "RelationshipTypeIndexCursor[relationship=" + relationshipReference() + ", relationship based]";
        }
    }

    @Override
    protected boolean allowedToSeeEntity(long entityReference) {
        if (read.getAccessMode().allowsTraverseAllRelTypes()) {
            return true;
        }
        read.singleRelationship(entityReference, relationshipScanCursor);
        return relationshipScanCursor.next();
    }

    @Override
    protected boolean innerNext() {
        return indexNext();
    }

    @Override
    protected LongIterator createAddedInTxState(TransactionState txState, int token, IndexOrder order) {
        return sortTxState(
                txState.relationshipsWithTypeChanged(token).getAdded().freeze(), order);
    }

    @Override
    protected LongSet createDeletedInTxState(TransactionState txState, int token) {
        return txState.addedAndRemovedRelationships().getRemoved().freeze();
    }

    private void checkReadFromStore() {
        if (relationshipScanCursor.relationshipReference() != entity) {
            throw new IllegalStateException("Relationship hasn't been read from store");
        }
    }
}
