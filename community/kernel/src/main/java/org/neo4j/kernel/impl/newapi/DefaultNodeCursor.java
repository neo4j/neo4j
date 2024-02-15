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

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.ReadSecurityPropertyProvider;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.storageengine.util.SingleDegree;

class DefaultNodeCursor extends TraceableCursorImpl<DefaultNodeCursor> implements NodeCursor {
    Read read;
    boolean checkHasChanges;
    boolean hasChanges;
    private LongIterator addedNodes;
    private boolean singleIsAddedInTx;
    final StorageNodeCursor storeCursor;
    private final InternalCursorFactory internalCursors;
    private StorageNodeCursor securityStoreNodeCursor;
    private StorageRelationshipTraversalCursor securityStoreRelationshipCursor;
    private StoragePropertyCursor securityPropertyCursor;
    private long currentAddedInTx = NO_ID;
    private long single;
    private boolean isSingle;

    DefaultNodeCursor(
            CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor, InternalCursorFactory internalCursors) {
        super(pool);
        this.storeCursor = storeCursor;
        this.internalCursors = internalCursors;
    }

    void scan(Read read) {
        storeCursor.scan();
        this.read = read;
        this.isSingle = false;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if (tracer != null) {
            tracer.onAllNodesScan();
        }
    }

    boolean scanBatch(Read read, AllNodeScan scan, long sizeHint, LongIterator addedNodes, boolean hasChanges) {
        this.read = read;
        this.isSingle = false;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = false;
        this.hasChanges = hasChanges;
        this.addedNodes = addedNodes;
        boolean scanBatch = storeCursor.scanBatch(scan, sizeHint);
        return addedNodes.hasNext() || scanBatch;
    }

    void single(long reference, Read read) {
        storeCursor.single(reference);
        this.read = read;
        this.single = reference;
        this.isSingle = true;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        this.singleIsAddedInTx = false;
    }

    protected boolean currentNodeIsAddedInTx() {
        return currentAddedInTx != NO_ID;
    }

    @Override
    public long nodeReference() {
        if (currentAddedInTx != NO_ID) {
            // Special case where the most recent next() call selected a node that exists only in tx-state.
            // Internal methods getting data about this node will also check tx-state and get the data from there.
            return currentAddedInTx;
        }
        return storeCursor.entityReference();
    }

    @Override
    public TokenSet labels() {
        if (currentAddedInTx != NO_ID) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from(txState.nodeStateLabelDiffSets(currentAddedInTx).getAdded());
        } else if (hasChanges()) {
            TransactionState txState = read.txState();
            final MutableIntSet labels = new IntHashSet(storeCursor.labels());
            // Augment what was found in store with what we have in tx state
            return Labels.from(txState.augmentLabels(labels, txState.getNodeState(storeCursor.entityReference())));
        } else {
            // Nothing in tx state, just read the data.
            return Labels.from(storeCursor.labels());
        }
    }

    @Override
    public TokenSet labelsAndProperties(PropertyCursor propertyCursor, PropertySelection selection) {
        if (currentAddedInTx != NO_ID) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            properties(propertyCursor, selection);
            return Labels.from(txState.nodeStateLabelDiffSets(currentAddedInTx).getAdded());
        } else if (hasChanges()) {
            TransactionState txState = read.txState();
            final MutableIntSet labels = new IntHashSet(storeCursor.labels());
            properties(propertyCursor, selection);
            // Augment what was found in store with what we have in tx state
            return Labels.from(txState.augmentLabels(labels, txState.getNodeState(storeCursor.entityReference())));
        } else {
            // Nothing in tx state, just read the data.
            var defaultPropertyCursor = (DefaultPropertyCursor) propertyCursor;
            int[] labels = storeCursor.labelsAndProperties(defaultPropertyCursor.storeCursor, selection);
            defaultPropertyCursor.initNode(this, selection, read, false);
            return Labels.from(labels);
        }
    }

    /**
     * The normal labels() method takes into account TxState for both created nodes and set/remove labels.
     * Some code paths need to consider created, but not changed labels.
     */
    @Override
    public TokenSet labelsIgnoringTxStateSetRemove() {
        if (currentAddedInTx != NO_ID) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from(txState.nodeStateLabelDiffSets(currentAddedInTx).getAdded());
        } else {
            // Nothing in tx state, just read the data.
            return Labels.from(storeCursor.labels());
        }
    }

    @Override
    public boolean hasLabel(int label) {
        if (hasChanges()) {
            TransactionState txState = read.txState();
            LongDiffSets diffSets = txState.nodeStateLabelDiffSets(nodeReference());
            if (diffSets.getAdded().contains(label)) {
                return true;
            }
            if (currentNodeIsAddedInTx()) {
                return false;
            }
            if (diffSets.getRemoved().contains(label) || currentAddedInTx != NO_ID) {
                return false;
            }
        }

        if (tracer != null) {
            tracer.onHasLabel(label);
        }
        return storeCursor.hasLabel(label);
    }

    @Override
    public boolean hasLabel() {
        if (hasChanges()) {
            TransactionState txState = read.txState();
            LongDiffSets diffSets = txState.nodeStateLabelDiffSets(nodeReference());
            if (diffSets.getAdded().notEmpty()) {
                return true;
            }
            if (currentNodeIsAddedInTx()) {
                return false;
            }
            // If we remove labels in the transaction we need to do a full check so that we don't remove all of the
            // nodes
            if (diffSets.getRemoved().notEmpty()) {
                return labels().numberOfTokens() > 0;
            }
        }

        if (tracer != null) {
            tracer.onHasLabel();
        }

        return storeCursor.hasLabel();
    }

    @Override
    public void relationships(RelationshipTraversalCursor cursor, RelationshipSelection selection) {
        ((DefaultRelationshipTraversalCursor) cursor).init(this, selection, read);
    }

    @Override
    public boolean supportsFastRelationshipsTo() {
        return currentAddedInTx == NO_ID && storeCursor.supportsFastRelationshipsTo();
    }

    @Override
    public void relationshipsTo(
            RelationshipTraversalCursor relationships, RelationshipSelection selection, long neighbourNodeReference) {
        ((DefaultRelationshipTraversalCursor) relationships).init(this, selection, neighbourNodeReference, read);
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        ((DefaultPropertyCursor) cursor).initNode(this, selection, read, true);
    }

    @Override
    public long relationshipsReference() {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.relationshipsReference();
    }

    @Override
    public Reference propertiesReference() {
        return currentAddedInTx != NO_ID ? NULL_REFERENCE : storeCursor.propertiesReference();
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        return (currentAddedInTx != NO_ID || storeCursor.supportsFastDegreeLookup()) && allowsTraverseAll();
    }

    @Override
    public int[] relationshipTypes() {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? read.txState().getNodeState(nodeReference()) : null;
        int[] storedTypes = currentAddedInTx == NO_ID ? storeCursor.relationshipTypes() : null;
        MutableIntSet types = storedTypes != null ? IntSets.mutable.of(storedTypes) : IntSets.mutable.empty();
        if (nodeTxState != null) {
            types.addAll(nodeTxState.getAddedRelationshipTypes());
        }
        return types.toArray();
    }

    @Override
    public Degrees degrees(RelationshipSelection selection) {
        EagerDegrees degrees = new EagerDegrees();
        fillDegrees(selection, degrees);
        return degrees;
    }

    @Override
    public int degree(RelationshipSelection selection) {
        SingleDegree degrees = new SingleDegree();
        fillDegrees(selection, degrees);
        return degrees.getTotal();
    }

    @Override
    public int degreeWithMax(int maxDegree, RelationshipSelection selection) {
        SingleDegree degrees = new SingleDegree(maxDegree);
        fillDegrees(selection, degrees);
        return Math.min(degrees.getTotal(), maxDegree);
    }

    private void fillDegrees(RelationshipSelection selection, Degrees.Mutator degrees) {
        if (hasChanges()) {
            var nodeTxState = read.txState().getNodeState(nodeReference());
            if (nodeTxState != null && !nodeTxState.fillDegrees(selection, degrees)) {
                return;
            }
        }
        if (currentAddedInTx == NO_ID) {
            if (allowsTraverseAll()) {
                storeCursor.degrees(selection, degrees);
            } else {
                readRestrictedDegrees(selection, degrees);
            }
        }
    }

    private void readRestrictedDegrees(RelationshipSelection selection, Degrees.Mutator degrees) {
        // When we read degrees limited by security we need to traverse all relationships and check the "other side" if
        // we can add it
        if (securityStoreRelationshipCursor == null) {
            securityStoreRelationshipCursor = internalCursors.allocateStorageRelationshipTraversalCursor();
        }
        storeCursor.relationships(securityStoreRelationshipCursor, selection);
        while (securityStoreRelationshipCursor.next()) {
            int type = securityStoreRelationshipCursor.type();
            if (read.getAccessMode().allowsTraverseRelType(type)) {
                long source = securityStoreRelationshipCursor.sourceNodeReference();
                long target = securityStoreRelationshipCursor.targetNodeReference();
                boolean loop = source == target;
                boolean outgoing = !loop && source == nodeReference();
                boolean incoming = !loop && !outgoing;
                if (!loop) { // No need to check labels for loops. We already know we are allowed since we have the node
                    // loaded in this cursor
                    if (securityStoreNodeCursor == null) {
                        securityStoreNodeCursor = internalCursors.allocateStorageNodeCursor();
                    }
                    securityStoreNodeCursor.single(outgoing ? target : source);
                    if (!securityStoreNodeCursor.next() || !allowsTraverse(securityStoreNodeCursor)) {
                        continue;
                    }
                }
                if (!degrees.add(type, outgoing ? 1 : 0, incoming ? 1 : 0, loop ? 1 : 0)) {
                    return;
                }
            }
        }
    }

    private boolean allowsTraverse(StorageNodeCursor nodeCursor) {
        AccessMode accessMode = read.getAccessMode();
        if (accessMode.allowsTraverseAllLabels()) {
            return true;
        }

        var labels = nodeCursor.labels();
        if (accessMode.hasTraversePropertyRules()) {
            var securityProperties = accessMode.getTraverseSecurityProperties(labels);
            if (securityProperties.notEmpty()) { // This means there are property-based rules affecting THIS NODE
                var securityPropertyProvider = getSecurityPropertyProvider(nodeCursor, securityProperties);
                return accessMode.allowsTraverseNodeWithPropertyRules(securityPropertyProvider, labels);
            }
        }
        return accessMode.allowsTraverseNode(labels);
    }

    private StoragePropertyCursor lazyInitAndGetSecurityPropertyCursor() {
        if (securityPropertyCursor == null) {
            securityPropertyCursor = internalCursors.allocateStoragePropertyCursor();
        }
        return securityPropertyCursor;
    }

    private ReadSecurityPropertyProvider getSecurityPropertyProvider(
            StorageNodeCursor storageNodeCursor, IntSet securityProperties) {
        storageNodeCursor.properties(
                lazyInitAndGetSecurityPropertyCursor(), PropertySelection.selection(securityProperties.toArray()));
        return new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(securityPropertyCursor);
    }

    @Override
    public boolean next() {
        // Check tx state
        boolean hasChanges = hasChanges();

        if (hasChanges) {
            if (isSingle) {
                if (singleIsAddedInTx) {
                    currentAddedInTx = single;
                    singleIsAddedInTx = false;
                    if (tracer != null) {
                        tracer.onNode(nodeReference());
                    }
                    return true;
                }
            } else {
                if (addedNodes.hasNext()) {
                    currentAddedInTx = addedNodes.next();
                    if (tracer != null) {
                        tracer.onNode(nodeReference());
                    }
                    return true;
                }
            }
            currentAddedInTx = NO_ID;
        }

        while (storeCursor.next()) {
            boolean skip = hasChanges && read.txState().nodeIsDeletedInThisBatch(storeCursor.entityReference());
            if (!skip && allowsTraverse()) {
                if (tracer != null) {
                    tracer.onNode(nodeReference());
                }
                return true;
            }
        }
        return false;
    }

    protected boolean allowsTraverse() {
        return allowsTraverse(storeCursor);
    }

    protected boolean allowsTraverseAll() {
        AccessMode accessMode = read.getAccessMode();
        return accessMode.allowsTraverseAllRelTypes() && accessMode.allowsTraverseAllLabels();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            read = null;
            checkHasChanges = true;
            addedNodes = ImmutableEmptyLongIterator.INSTANCE;
            storeCursor.close();
            storeCursor.reset();
            if (securityStoreNodeCursor != null) {
                securityStoreNodeCursor.reset();
            }
            if (securityStoreRelationshipCursor != null) {
                securityStoreRelationshipCursor.reset();
            }
            if (securityPropertyCursor != null) {
                securityPropertyCursor.reset();
            }
        }
        super.closeInternal();
    }

    @Override
    public boolean isClosed() {
        return read == null;
    }

    /**
     * NodeCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    boolean hasChanges() {
        if (checkHasChanges) {
            computeHasChanges();
        }
        return hasChanges;
    }

    @SuppressWarnings("AssignmentUsedAsCondition")
    private void computeHasChanges() {
        checkHasChanges = false;
        if (hasChanges = read.hasTxStateWithChanges()) {
            if (this.isSingle) {
                singleIsAddedInTx = read.txState().nodeIsAddedInThisBatch(single);
            } else {
                addedNodes = read.txState()
                        .addedAndRemovedNodes()
                        .getAdded()
                        .freeze()
                        .longIterator();
            }
        }
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "NodeCursor[closed state]";
        } else {
            return "NodeCursor[id=" + nodeReference() + ", " + storeCursor + "]";
        }
    }

    @Override
    public void release() {
        final var localSecurityPropertyCursor = securityPropertyCursor;
        final var localSecurityRelationshipCursor = securityStoreRelationshipCursor;
        final var localSecurityNodeCursor = securityStoreNodeCursor;
        try (localSecurityPropertyCursor;
                localSecurityRelationshipCursor;
                localSecurityNodeCursor;
                storeCursor) {
            // A concise and low-cost way of closing all these cursors w/o the overhead of, say IOUtils.closeAll
        } finally {
            securityPropertyCursor = null;
            securityStoreRelationshipCursor = null;
            securityStoreNodeCursor = null;
        }
    }
}
