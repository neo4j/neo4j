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
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageProperty;

public class DefaultNodeValueIndexCursor extends DefaultEntityValueIndexCursor<DefaultNodeValueIndexCursor>
        implements NodeValueIndexCursor {
    private final InternalCursorFactory internalCursors;
    private DefaultNodeCursor internalNodeCursor;
    private TraceablePropertyCursor propertyCursor;
    private int[] propertyIds;
    private AccessControlDataProvider accessControlDataProvider;

    DefaultNodeValueIndexCursor(
            CursorPool<DefaultNodeValueIndexCursor> pool,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(pool, applyAccessModeToTxState);
        this.internalCursors = internalCursors;
    }

    /**
     * Check that the user is allowed to access all nodes and properties given by the index descriptor.
     * <p>
     * If the current user is allowed to traverse all labels used in this index and read the properties
     * of all nodes in the index, we can skip checking on every node we get back.
     */
    @Override
    protected boolean canAccessAllDescribedEntities(IndexDescriptor descriptor) {
        propertyIds = descriptor.schema().getPropertyIds();
        int[] labelIds = descriptor.schema().getEntityTokenIds();
        return accessMode.allowsTraverseAndReadAllMatchingNodeProperties(labelIds, propertyIds);
    }

    @Override
    void traceOnEntity(KernelReadTracer tracer, long entity) {
        tracer.onNode(entity);
    }

    @Override
    String implementationName() {
        return "NodeValueIndexCursor";
    }

    @Override
    protected final boolean canAccessEntityAndProperties(long reference) {
        return canAccessEntityAndProperties(reference, accessMode, true);
    }

    final boolean canAccessEntityAndProperties(long reference, AccessMode accessMode, boolean assertAccessMode) {
        ensureNodeCursor();
        read.singleNode(reference, internalNodeCursor);
        if (!internalNodeCursor.next()) {
            // This node is not visible to this security context
            return false;
        }

        assert !assertAccessMode || accessMode == accessModeProvider.getAccessMode()
                : "access mode changed while cursor is in use";
        return accessMode.allowsReadNodeProperties(
                () -> AccessControlDataProvider.nodeLabels(internalNodeCursor, applyAccessModeToTxState),
                propertyIds,
                this::getAccessControlDataProvider);
    }

    /**
     * AccessControlDataProvider when used as SelectedPropertiesProvider will return properties for the node pointed by {@link #internalNodeCursor}
     * This indirection is here for the sake of ultimate laziness
     */
    private AccessControlDataProvider getAccessControlDataProvider() {
        if (accessControlDataProvider == null) {
            accessControlDataProvider = new AccessControlDataProvider(
                    () -> (propertyCursor, selection) -> {
                        if (internalNodeCursor.storeCursor.entityReference() != LongReference.NULL) {
                            propertyCursor.initNodeProperties(internalNodeCursor.storeCursor, selection);
                        }
                    },
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
                    .getNodeState(internalNodeCursor.nodeReference())
                    .addedProperties();
        }
        return Iterables.empty();
    }

    @Override
    public void node(NodeCursor cursor) {
        read.singleNode(entityReference(), cursor);
    }

    @Override
    public long nodeReference() {
        return entityReference();
    }

    // NodeCursor interface
    @Override
    public TokenSet labels() {
        checkReadFromStore();
        return internalNodeCursor.labels();
    }

    @Override
    public TokenSet labelsIgnoringTxStateSetRemove() {
        checkReadFromStore();
        return internalNodeCursor.labelsIgnoringTxStateSetRemove();
    }

    @Override
    public boolean hasLabel(int label) {
        checkReadFromStore();
        return internalNodeCursor.hasLabel(label);
    }

    @Override
    public boolean hasLabel() {
        checkReadFromStore();
        return internalNodeCursor.hasLabel();
    }

    @Override
    public void relationships(RelationshipTraversalCursor relationships, RelationshipSelection selection) {
        checkReadFromStore();
        internalNodeCursor.relationships(relationships, selection);
    }

    @Override
    public boolean supportsFastRelationshipsTo() {
        checkReadFromStore();
        return internalNodeCursor.supportsFastRelationshipsTo();
    }

    @Override
    public void relationshipsTo(
            RelationshipTraversalCursor relationships, RelationshipSelection selection, long neighbourNodeReference) {
        checkReadFromStore();
        internalNodeCursor.relationshipsTo(relationships, selection, neighbourNodeReference);
    }

    @Override
    public long relationshipsReference() {
        checkReadFromStore();
        return internalNodeCursor.relationshipsReference();
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        checkReadFromStore();
        return internalNodeCursor.supportsFastDegreeLookup();
    }

    @Override
    public int[] relationshipTypes() {
        checkReadFromStore();
        return internalNodeCursor.relationshipTypes();
    }

    @Override
    public Degrees degrees(RelationshipSelection selection) {
        checkReadFromStore();
        return internalNodeCursor.degrees(selection);
    }

    @Override
    public long degree(RelationshipSelection selection) {
        checkReadFromStore();
        return internalNodeCursor.degree(selection);
    }

    @Override
    public long degreeWithMax(long maxDegree, RelationshipSelection selection) {
        checkReadFromStore();
        return internalNodeCursor.degreeWithMax(maxDegree, selection);
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        checkReadFromStore();
        internalNodeCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        checkReadFromStore();
        return internalNodeCursor.propertiesReference();
    }

    @Override
    public boolean readFromStore() {
        ensureNodeCursor();
        if (internalNodeCursor.nodeReference() == entity) {
            // A security check, or a previous call to this method for this node already seems to have loaded
            // this node
            return true;
        }

        internalNodeCursor.single(entity, read, txStateHolder, accessModeProvider);
        return internalNodeCursor.next();
    }

    private void checkReadFromStore() {
        if (internalNodeCursor.nodeReference() != entity) {
            throw new IllegalStateException("Node hasn't been read from store");
        }
    }

    @Override
    protected LongSetContains removed(TransactionState txState, IndexRemovalSnapshot removedFromIndex) {
        var removed = txState.addedAndRemovedNodes().getRemoved().toImmutable();
        return (value) ->
                removed.contains(value) || removedFromIndex.isRemoved().test(value);
    }

    @Override
    public void release() {
        if (internalNodeCursor != null) {
            internalNodeCursor.close();
            internalNodeCursor.release();
            internalNodeCursor = null;
        }
        if (propertyCursor != null) {
            propertyCursor.close();
            propertyCursor.release();
            propertyCursor = null;
        }
        if (accessControlDataProvider != null) {
            accessControlDataProvider.close();
            accessControlDataProvider.release();
            accessControlDataProvider = null;
        }
    }

    @Override
    protected boolean doStoreValuePassesQueryFilter(
            long reference, PropertySelection propertySelection, PropertyIndexQuery[] query) {
        ensureNodeCursor();
        read.singleNode(reference, internalNodeCursor);
        if (internalNodeCursor.next()) {
            ensurePropertyCursor();
            internalNodeCursor.properties(propertyCursor, propertySelection);
            return CursorPredicates.propertiesMatch(propertyCursor, query);
        }

        return false;
    }

    private void ensureNodeCursor() {
        if (internalNodeCursor == null) {
            internalNodeCursor = internalCursors.allocateNodeCursor();
        }
    }

    private void ensurePropertyCursor() {
        if (propertyCursor == null) {
            propertyCursor = internalCursors.allocatePropertyCursor();
        }
    }
}
