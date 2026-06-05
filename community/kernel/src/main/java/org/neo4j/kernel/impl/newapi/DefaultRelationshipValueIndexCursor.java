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

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageProperty;

public class DefaultRelationshipValueIndexCursor
        extends DefaultEntityValueIndexCursor<DefaultRelationshipValueIndexCursor>
        implements RelationshipValueIndexCursor {
    private final InternalCursorFactory internalCursors;
    private final DefaultRelationshipScanCursor relationshipScanCursor;
    private TraceablePropertyCursor securityPropertyCursor;
    private int[] propertyIds;
    private AccessControlDataProvider accessControlDataProvider;

    DefaultRelationshipValueIndexCursor(
            CursorPool<DefaultRelationshipValueIndexCursor> pool,
            DefaultRelationshipScanCursor relationshipScanCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(pool, applyAccessModeToTxState);
        this.relationshipScanCursor = relationshipScanCursor;
        this.internalCursors = internalCursors;
    }

    @Override
    public void source(NodeCursor cursor) {
        checkReadFromStore();
        read.singleNode(relationshipScanCursor.sourceNodeReference(), cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        checkReadFromStore();
        read.singleNode(relationshipScanCursor.targetNodeReference(), cursor);
    }

    @Override
    public int type() {
        checkReadFromStore();
        return relationshipScanCursor.type();
    }

    @Override
    public long relationshipReference() {
        return entityReference();
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
        relationshipScanCursor.single(entity, read, txStateHolder, accessModeProvider);
        return relationshipScanCursor.next();
    }

    private void checkReadFromStore() {
        if (relationshipScanCursor.relationshipReference() != entity) {
            throw new IllegalStateException("Relationship hasn't been read from store");
        }
    }

    @Override
    protected boolean doStoreValuePassesQueryFilter(
            long reference, PropertySelection propertySelection, PropertyIndexQuery[] query) {
        read.singleRelationship(reference, relationshipScanCursor);
        if (relationshipScanCursor.next()) {
            if (securityPropertyCursor == null) {
                securityPropertyCursor = internalCursors.allocatePropertyCursor();
            }
            relationshipScanCursor.properties(securityPropertyCursor, propertySelection);
            return CursorPredicates.propertiesMatch(securityPropertyCursor, query);
        }
        return false;
    }

    /**
     * Check that the user is allowed to access all relationships and properties given by the index descriptor.
     * <p>
     * We can skip checking permissions on every relationship we get back if the current user is allowed to:
     * <ul>
     *     <li>traverse all relationships of type the index is defined for</li>
     *     <li>traverse all nodes no matter what label the node has</li>
     *     <li>read all the indexed properties</li>
     * </ul>
     */
    @Override
    protected boolean canAccessAllDescribedEntities(IndexDescriptor descriptor) {
        propertyIds = descriptor.schema().getPropertyIds();
        int[] relTypes = descriptor.schema().getEntityTokenIds();

        return accessMode.allowsTraverseAndReadAllMatchingRelProperties(relTypes, propertyIds);
    }

    @Override
    protected LongSetContains removed(TransactionState txState, IndexRemovalSnapshot removedFromIndex) {
        var removed = txState.addedAndRemovedRelationships().getRemoved().toImmutable();
        return (value) ->
                removed.contains(value) || removedFromIndex.isRemoved().test(value);
    }

    @Override
    protected final boolean canAccessEntityAndProperties(long reference) {
        read.singleRelationship(reference, relationshipScanCursor);
        if (!relationshipScanCursor.next()) {
            // This relationship is not visible to this security context
            return false;
        }

        assert accessMode == accessModeProvider.getAccessMode() : "access mode changed while cursor is in use";
        return accessMode.allowsReadRelProperties(
                relationshipScanCursor::type, propertyIds, this::getAccessControlDataProvider);
    }

    /**
     * AccessControlDataProvider when used as SelectedPropertiesProvider will return properties for the node pointed by {@link #relationshipScanCursor}
     * This indirection is here for the sake of ultimate laziness
     */
    private AccessControlDataProvider getAccessControlDataProvider() {
        if (accessControlDataProvider == null) {
            accessControlDataProvider = new AccessControlDataProvider(
                    () -> (propertyCursor, selection) -> propertyCursor.initRelationshipProperties(
                            relationshipScanCursor.propertiesReference(), selection),
                    internalCursors,
                    applyAccessModeToTxState,
                    this::txStateProperties,
                    () -> read);
        }
        return accessControlDataProvider;
    }

    private Iterable<StorageProperty> txStateProperties() {
        if (txStateHolder.hasTxStateWithChanges()) {
            return txStateHolder
                    .txState()
                    .getRelationshipState(relationshipScanCursor.relationshipReference())
                    .addedProperties();
        }
        return Iterables.empty();
    }

    @Override
    void traceOnEntity(KernelReadTracer tracer, long entity) {
        tracer.onRelationship(entity);
    }

    @Override
    String implementationName() {
        return "RelationshipValueIndexCursor";
    }

    @Override
    public void release() {
        if (relationshipScanCursor != null) {
            relationshipScanCursor.close();
            relationshipScanCursor.release();
        }
        if (securityPropertyCursor != null) {
            securityPropertyCursor.close();
            securityPropertyCursor.release();
            securityPropertyCursor = null;
        }
        if (accessControlDataProvider != null) {
            accessControlDataProvider.close();
            accessControlDataProvider.release();
            accessControlDataProvider = null;
        }
    }
}
