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

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.apache.commons.lang3.ArrayUtils.remove;
import static org.neo4j.collection.PrimitiveArrays.intersect;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.additional_lock_verification;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.CONSTRAINT_CREATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.INDEX_CREATION;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.impl.locking.ResourceIds.indexEntryResourceId;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;
import static org.neo4j.lock.ResourceType.INDEX_ENTRY;
import static org.neo4j.lock.ResourceType.SCHEMA_NAME;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingIntFunction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.internal.kernel.api.security.AccessMode.Static;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorImplementation;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeRepresentation;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConflictingConstraintException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedLabelInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedRelationshipTypeInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedSchemaComponentException;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.ResourceIds;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Collects all Kernel API operations and guards them from being used outside of transaction.
 *
 * Many methods assume cursors to be initialized before use in private methods, even if they're not passed in explicitly.
 * Keep that in mind: e.g. nodeCursor, propertyCursor and relationshipCursor
 */
public class Operations implements Write, SchemaWrite, Upgrade {

    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder allStoreHolder;
    private final StorageReader storageReader;
    private final CommandCreationContext commandCreationContext;
    private final DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider;
    private final KernelVersionProvider kernelVersionProvider;
    private final StorageLocks storageLocks;
    private final KernelToken token;
    private final IndexTxStateUpdater updater;
    private final DefaultPooledCursors cursors;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final ConstraintSemantics constraintSemantics;
    private final IndexingProvidersService indexProviders;
    private final MemoryTracker memoryTracker;
    private final boolean additionLockVerification;
    private final boolean typeConstraintEnabled;
    private final boolean dependentConstraintsEnabled;
    private DefaultNodeCursor nodeCursor;
    private DefaultNodeCursor restrictedNodeCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultPropertyCursor restrictedPropertyCursor;
    private DefaultRelationshipScanCursor relationshipCursor;

    private DefaultRelationshipScanCursor restrictedRelationshipCursor;

    public Operations(
            AllStoreHolder allStoreHolder,
            StorageReader storageReader,
            IndexTxStateUpdater updater,
            CommandCreationContext commandCreationContext,
            DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider,
            KernelVersionProvider kernelVersionProvider,
            StorageLocks storageLocks,
            KernelTransactionImplementation ktx,
            KernelToken token,
            DefaultPooledCursors cursors,
            ConstraintIndexCreator constraintIndexCreator,
            ConstraintSemantics constraintSemantics,
            IndexingProvidersService indexProviders,
            Config config,
            MemoryTracker memoryTracker) {
        this.storageReader = storageReader;
        this.commandCreationContext = commandCreationContext;
        this.dbmsRuntimeVersionProvider = dbmsRuntimeVersionProvider;
        this.kernelVersionProvider = kernelVersionProvider;
        this.storageLocks = storageLocks;
        this.token = token;
        this.allStoreHolder = allStoreHolder;
        this.ktx = ktx;
        this.updater = updater;
        this.cursors = cursors;
        this.constraintIndexCreator = constraintIndexCreator;
        this.constraintSemantics = constraintSemantics;
        this.indexProviders = indexProviders;
        this.memoryTracker = memoryTracker;
        this.additionLockVerification = config.get(additional_lock_verification);
        this.typeConstraintEnabled = config.get(GraphDatabaseInternalSettings.type_constraints);
        this.dependentConstraintsEnabled = config.get(GraphDatabaseInternalSettings.dependent_constraints_enabled);
    }

    public void initialize(CursorContext cursorContext) {
        this.nodeCursor = cursors.allocateFullAccessNodeCursor(cursorContext);
        this.propertyCursor = cursors.allocateFullAccessPropertyCursor(cursorContext, memoryTracker);
        this.relationshipCursor = cursors.allocateFullAccessRelationshipScanCursor(cursorContext);
        this.restrictedNodeCursor = cursors.allocateNodeCursor(cursorContext, memoryTracker);
        this.restrictedPropertyCursor = cursors.allocatePropertyCursor(cursorContext, memoryTracker);
        this.restrictedRelationshipCursor =
                (DefaultRelationshipScanCursor) cursors.allocateRelationshipScanCursor(cursorContext, memoryTracker);
    }

    @Override
    public long nodeCreate() {
        ktx.securityAuthorizationHandler().assertAllowsCreateNode(ktx.securityContext(), token::labelGetName, null);
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        long nodeId = commandCreationContext.reserveNode();
        storageLocks.acquireExclusiveNodeLock(ktx.lockTracer(), nodeId);
        txState.nodeDoCreate(nodeId);
        return nodeId;
    }

    @Override
    public long nodeCreateWithLabels(int[] labels) throws ConstraintValidationException {
        if (labels == null || labels.length == 0) {
            return nodeCreate();
        }
        ktx.securityAuthorizationHandler().assertAllowsCreateNode(ktx.securityContext(), token::labelGetName, labels);

        // We don't need to check the node for existence, like we do in nodeAddLabel, because we just created it.
        // We also don't need to check if the node already has some of the labels, because we know it has none.
        // And we don't need to take the exclusive lock on the node, because it was created in this transaction and
        // isn't visible to anyone else yet.
        ktx.assertOpen();
        long[] lockingIds = acquireSharedLabelLocks(labels);
        sharedTokenSchemaLock(ResourceType.LABEL);

        TransactionState txState = ktx.txState();
        long nodeId = commandCreationContext.reserveNode();
        storageLocks.acquireExclusiveNodeLock(ktx.lockTracer(), nodeId);
        txState.nodeDoCreate(nodeId);
        nodeCursor.single(nodeId, allStoreHolder);
        nodeCursor.next();

        int prevLabel = NO_SUCH_LABEL;
        for (long lockingId : lockingIds) {
            int label = (int) lockingId;
            if (label != prevLabel) // Filter out duplicates.
            {
                checkConstraintsAndAddLabelToNode(nodeId, label);
                prevLabel = label;
            }
        }
        return nodeId;
    }

    private long[] acquireSharedLabelLocks(int[] labels) {
        int labelCount = labels.length;
        long[] lockingIds = new long[labelCount];
        for (int i = 0; i < labelCount; i++) {
            lockingIds[i] = labels[i];
        }
        Arrays.sort(lockingIds); // Sort to ensure labels are locked and assigned in order.
        ktx.lockClient().acquireShared(ktx.lockTracer(), ResourceType.LABEL, lockingIds);
        return lockingIds;
    }

    @Override
    public boolean nodeDelete(long node) {
        ktx.assertOpen();
        return nodeDelete(node, true);
    }

    @Override
    public int nodeDetachDelete(long nodeId, DetachDeleteConsumer deletedRelationshipConsumer) {
        ktx.assertOpen();
        storageLocks.acquireNodeDeletionLock(ktx.txState(), ktx.lockTracer(), nodeId);
        NodeCursor nodeCursor = ktx.ambientNodeCursor();
        ktx.dataRead().singleNode(nodeId, nodeCursor);
        int deletedRelationships = 0;
        if (nodeCursor.next()) {
            try (var rels = RelationshipSelections.allCursor(ktx.cursors(), nodeCursor, null, ktx.cursorContext())) {
                while (rels.next()) {
                    long relationshipReference = rels.relationshipReference();
                    int type = rels.type();
                    long sourceNodeReference = rels.sourceNodeReference();
                    long targetNodeReference = rels.targetNodeReference();
                    boolean deleted = relationshipDelete(relationshipReference);
                    if (additionLockVerification && !deleted) {
                        throw new RuntimeException(
                                "Relationship chain modified even when node delete lock was held: " + rels);
                    }
                    if (deleted) {
                        deletedRelationshipConsumer.accept(
                                relationshipReference, type, sourceNodeReference, targetNodeReference);
                    }
                    deletedRelationships++;
                }
            }
        }

        // we are already holding the lock
        nodeDelete(nodeId, false);
        return deletedRelationships;
    }

    @Override
    public long relationshipCreate(long sourceNode, int relationshipType, long targetNode)
            throws EntityNotFoundException {
        // We have seen a case where a relationship of type -1 has been 'created' resulting in a
        // relationship group of type 65535 but no relationship (because there is a guard for that
        // for some reason). Throwing here in the hopes of getting to the bottom of how this can ever happen
        if (relationshipType < 0) {
            throw new IllegalArgumentException(
                    "Tried to create relationship with invalid type '%d' between nodes with ids '%d' and '%d'"
                            .formatted(relationshipType, sourceNode, targetNode));
        }

        ktx.securityAuthorizationHandler()
                .assertAllowsCreateRelationship(
                        ktx.securityContext(), token::relationshipTypeGetName, relationshipType);
        ktx.assertOpen();

        sharedSchemaLock(ResourceType.RELATIONSHIP_TYPE, relationshipType);
        sharedTokenSchemaLock(ResourceType.RELATIONSHIP_TYPE);
        TransactionState txState = ktx.txState();
        var sourceNodeAddedInThisBatch = txState.nodeIsAddedInThisBatch(sourceNode);
        var targetNodeAddedInTx =
                sourceNode == targetNode ? sourceNodeAddedInThisBatch : txState.nodeIsAddedInThisBatch(targetNode);
        storageLocks.acquireRelationshipCreationLock(
                ktx.lockTracer(), sourceNode, targetNode, sourceNodeAddedInThisBatch, targetNodeAddedInTx);
        if (!sourceNodeAddedInThisBatch) {
            assertNodeExists(sourceNode);
        }
        if (targetNode != sourceNode && !targetNodeAddedInTx) {
            assertNodeExists(targetNode);
        }

        long id = commandCreationContext.reserveRelationship(
                sourceNode, targetNode, relationshipType, sourceNodeAddedInThisBatch, targetNodeAddedInTx);
        storageLocks.acquireExclusiveRelationshipLock(ktx.lockTracer(), id);

        txState.relationshipDoCreate(id, relationshipType, sourceNode, targetNode);
        return id;
    }

    @Override
    public boolean relationshipDelete(long relationship) {
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        var relationshipIsAddedInThisBatch = txState.relationshipIsAddedInThisBatch(relationship);
        if (relationshipIsAddedInThisBatch) {
            try {
                singleRelationship(relationship);
            } catch (EntityNotFoundException e) {
                throw new IllegalStateException("Relationship " + relationship
                        + " was created in this transaction, but was not found when deleting it");
            }
            updater.onDeleteUncreated(relationshipCursor, propertyCursor);
            txState.relationshipDoDeleteAddedInThisBatch(relationship);
            return true;
        }

        allStoreHolder.singleRelationship(relationship, relationshipCursor); // tx-state aware

        if (!relationshipCursor.next()) {
            return false;
        }
        sharedSchemaLock(ResourceType.RELATIONSHIP_TYPE, relationshipCursor.type());
        sharedTokenSchemaLock(ResourceType.RELATIONSHIP_TYPE);
        var sourceNode = relationshipCursor.sourceNodeReference();
        var targetNode = relationshipCursor.targetNodeReference();
        boolean sourceNodeAddedInBatch = txState.nodeIsAddedInThisBatch(sourceNode);
        boolean targetNodeAddedInBatch = txState.nodeIsAddedInThisBatch(targetNode);
        storageLocks.acquireRelationshipDeletionLock(
                ktx.lockTracer(),
                sourceNode,
                targetNode,
                relationship,
                relationshipIsAddedInThisBatch,
                sourceNodeAddedInBatch,
                targetNodeAddedInBatch);

        if (!allStoreHolder.relationshipExists(relationship)) {
            return false;
        }

        ktx.securityAuthorizationHandler()
                .assertAllowsDeleteRelationship(
                        ktx.securityContext(), token::relationshipTypeGetName, relationshipCursor.type());
        txState.relationshipDoDelete(relationship, relationshipCursor.type(), sourceNode, targetNode);
        return true;
    }

    @Override
    public boolean nodeAddLabel(long node, int nodeLabel)
            throws EntityNotFoundException, ConstraintValidationException {
        sharedSchemaLock(ResourceType.LABEL, nodeLabel);
        sharedTokenSchemaLock(ResourceType.LABEL);
        acquireExclusiveNodeLock(node);
        storageLocks.acquireNodeLabelChangeLock(ktx.lockTracer(), node, nodeLabel);

        singleNode(node);

        if (nodeCursor.hasLabel(nodeLabel)) {
            // label already there, nothing to do
            return false;
        }
        LongSet removed = ktx.txState().nodeStateLabelDiffSets(node).getRemoved();
        if (!removed.contains(nodeLabel)) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetLabel(ktx.securityContext(), token::labelGetName, nodeLabel);
        }

        checkConstraintsAndAddLabelToNode(node, nodeLabel);
        return true;
    }

    private void checkConstraintsAndAddLabelToNode(long node, int nodeLabel)
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException {
        Collection<IndexDescriptor> indexes = checkConstraintsAndGetIndexes(node, nodeLabel);
        ktx.txState().nodeDoAddLabel(nodeLabel, node);
        updater.onLabelChange(nodeCursor, propertyCursor, ADDED_LABEL, indexes);
    }

    private Collection<IndexDescriptor> checkConstraintsAndGetIndexes(long node, int nodeLabel)
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException {
        if (storageReader.hasRelatedSchema(nodeLabel, NODE)) {
            // Load the property key id list for this node. We may need it for constraint validation if there are any
            // related constraints,
            // but regardless we need it for tx state updating
            int[] existingPropertyKeyIds = loadSortedNodePropertyKeyList();

            // Check so that we are not breaking uniqueness constraints
            // We do this by checking if there is an existing node in the index that
            // with the same label and property combination.
            if (existingPropertyKeyIds.length > 0) {
                Collection<IndexDescriptor> indexes =
                        storageReader.valueIndexesGetRelated(new int[] {nodeLabel}, existingPropertyKeyIds, NODE);
                for (IndexDescriptor index : indexes) {
                    if (index.isUnique()) {
                        PropertyIndexQuery.ExactPredicate[] propertyValues = getAllPropertyValues(
                                nodeCursor, index.schema(), StatementConstants.NO_SUCH_PROPERTY_KEY, Values.NO_VALUE);
                        if (propertyValues != null) {
                            validateNoExistingNodeWithExactValues(
                                    (UniquenessConstraintDescriptor)
                                            storageReader.constraintGetForName(index.getName()),
                                    index,
                                    propertyValues,
                                    node);
                        }
                    }
                }

                return indexes;
            }
        }

        return Collections.emptyList();
    }

    private int[] loadSortedNodePropertyKeyList() {
        nodeCursor.properties(propertyCursor, PropertySelection.ALL_PROPERTY_KEYS);
        return doLoadSortedPropertyKeyList();
    }

    private int[] loadSortedRelationshipPropertyKeyList() {
        relationshipCursor.properties(propertyCursor, PropertySelection.ALL_PROPERTY_KEYS);
        return doLoadSortedPropertyKeyList();
    }

    private int[] doLoadSortedPropertyKeyList() {
        if (!propertyCursor.next()) {
            return ArrayUtils.EMPTY_INT_ARRAY;
        }

        int[] propertyKeyIds = new int[4]; // just some arbitrary starting point, it grows on demand
        int cursor = 0;
        boolean isSorted = true;
        do {
            if (cursor == propertyKeyIds.length) {
                propertyKeyIds = Arrays.copyOf(propertyKeyIds, cursor * 2);
            }
            int key = propertyCursor.propertyKey();
            propertyKeyIds[cursor] = key;
            if (cursor > 0 && key < propertyKeyIds[cursor - 1]) {
                isSorted = false;
            }
            cursor++;
        } while (propertyCursor.next());
        if (cursor != propertyKeyIds.length) {
            propertyKeyIds = Arrays.copyOf(propertyKeyIds, cursor);
        }
        if (!isSorted) {
            Arrays.sort(propertyKeyIds);
        }
        return propertyKeyIds;
    }

    private boolean nodeDelete(long node, boolean lock) {
        ktx.assertOpen();

        if (ktx.hasTxStateWithChanges()) {
            TransactionState state = ktx.txState();
            if (state.nodeIsAddedInThisBatch(node)) {
                try {
                    singleNode(node);
                } catch (EntityNotFoundException e) {
                    throw new IllegalStateException("Node " + node
                            + " was created in this transaction, but was not found when it was about to be deleted");
                }
                updater.onDeleteUncreated(nodeCursor, propertyCursor);
                state.nodeDoDelete(node);
                return true;
            }
            if (state.nodeIsDeletedInThisBatch(node)) {
                // already deleted
                return false;
            }
        }

        if (lock) {
            storageLocks.acquireNodeDeletionLock(ktx.txState(), ktx.lockTracer(), node);
        }

        allStoreHolder.singleNode(node, nodeCursor);
        if (nodeCursor.next()) {
            acquireSharedNodeLabelLocks();
            sharedTokenSchemaLock(ResourceType.LABEL);

            ktx.securityAuthorizationHandler()
                    .assertAllowsDeleteNode(ktx.securityContext(), token::labelGetName, nodeCursor::labels);
            ktx.txState().nodeDoDelete(node);
            return true;
        }

        // tried to delete node that does not exist
        return false;
    }

    /**
     * Assuming that the nodeCursor has been initialized to the node that labels are retrieved from
     */
    private int[] acquireSharedNodeLabelLocks() {
        int[] labels = nodeCursor.labels().all();
        acquireSharedLabelLocks(labels);
        return labels;
    }

    /**
     * Assuming that the relationshipCursor has been initialized to the relationship that the type is retrieved from
     */
    private int acquireSharedRelationshipTypeLock() {
        int relType = relationshipCursor.type();
        ktx.lockClient().acquireShared(ktx.lockTracer(), ResourceType.RELATIONSHIP_TYPE, relType);
        return relType;
    }

    private void singleNode(long node) throws EntityNotFoundException {
        allStoreHolder.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            throw new EntityNotFoundException(
                    NODE, ktx.internalTransaction().elementIdMapper().nodeElementId(node));
        }
    }

    private void singleRelationship(long relationship) throws EntityNotFoundException {
        allStoreHolder.singleRelationship(relationship, relationshipCursor);
        if (!relationshipCursor.next()) {
            throw new EntityNotFoundException(
                    RELATIONSHIP, ktx.internalTransaction().elementIdMapper().relationshipElementId(relationship));
        }
    }

    /**
     * Fetch the property values for all properties in schema for a given entity. Return these as an exact predicate
     * array. This is run with no security check.
     */
    private PropertyIndexQuery.ExactPredicate[] getAllPropertyValues(
            EntityCursor cursor, SchemaDescriptor schema, int changedPropertyKeyId, Value changedValue) {
        int[] schemaPropertyIds = schema.getPropertyIds();
        PropertyIndexQuery.ExactPredicate[] values = new PropertyIndexQuery.ExactPredicate[schemaPropertyIds.length];

        int nMatched = 0;
        cursor.properties(propertyCursor, PropertySelection.selection(schemaPropertyIds));
        while (propertyCursor.next()) {
            int entityPropertyId = propertyCursor.propertyKey();
            int k = ArrayUtils.indexOf(schemaPropertyIds, entityPropertyId);
            if (k >= 0) {
                if (entityPropertyId != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                    values[k] = PropertyIndexQuery.exact(entityPropertyId, propertyCursor.propertyValue());
                }
                nMatched++;
            }
        }

        // This is true if we are adding a property
        if (changedPropertyKeyId != NO_SUCH_PROPERTY_KEY) {
            int k = ArrayUtils.indexOf(schemaPropertyIds, changedPropertyKeyId);
            if (k >= 0) {
                values[k] = PropertyIndexQuery.exact(changedPropertyKeyId, changedValue);
                nMatched++;
            }
        }

        if (nMatched < values.length) {
            return null;
        }
        return values;
    }

    /**
     * Fetch the property values for all properties in schema for a given entity. Return these as an exact predicate
     * array. This is run with no security check.
     */
    private PropertyIndexQuery.ExactPredicate[] getAllPropertyValues(
            EntityCursor cursor, SchemaDescriptor schema, IntObjectMap<Value> changedProperties) {
        int[] schemaPropertyIds = schema.getPropertyIds();
        PropertyIndexQuery.ExactPredicate[] values = new PropertyIndexQuery.ExactPredicate[schemaPropertyIds.length];

        int nMatched = 0;
        cursor.properties(propertyCursor, PropertySelection.selection(schemaPropertyIds));
        while (propertyCursor.next()) {
            int entityPropertyId = propertyCursor.propertyKey();
            int k = ArrayUtils.indexOf(schemaPropertyIds, entityPropertyId);
            if (k >= 0) {
                if (entityPropertyId != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                    values[k] = PropertyIndexQuery.exact(entityPropertyId, propertyCursor.propertyValue());
                }
                nMatched++;
            }
        }

        // This is true if we are adding a property
        for (IntObjectPair<Value> changedProperty : changedProperties.keyValuesView()) {
            int changedPropertyKeyId = changedProperty.getOne();
            if (changedPropertyKeyId != NO_SUCH_PROPERTY_KEY) {
                int k = ArrayUtils.indexOf(schemaPropertyIds, changedPropertyKeyId);
                if (k >= 0) {
                    values[k] = PropertyIndexQuery.exact(changedPropertyKeyId, changedProperty.getTwo());
                    nMatched++;
                }
            }
        }

        if (nMatched < values.length) {
            return null;
        }
        return values;
    }

    /**
     * Check so that there is not an existing node with the exact match of label and property
     */
    private void validateNoExistingNodeWithExactValues(
            IndexBackedConstraintDescriptor constraint,
            IndexDescriptor index,
            PropertyIndexQuery.ExactPredicate[] propertyValues,
            long modifiedNode)
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException {
        if (constraint == null || index == null) {
            return;
        }

        long existingNodeId = NO_SUCH_NODE;
        try (var r = ktx.overrideWith(ktx.securityContext().withMode(Static.FULL));
                var valueCursor = cursors.allocateNodeValueIndexCursor(ktx.cursorContext(), memoryTracker);
                IndexReaders indexReaders = new IndexReaders(index, allStoreHolder)) {
            assertOnlineAndLock(constraint, index, propertyValues);

            allStoreHolder.nodeIndexSeekWithFreshIndexReader(valueCursor, indexReaders.createReader(), propertyValues);
            while (valueCursor.next()) {
                if (valueCursor.nodeReference() != modifiedNode) {
                    existingNodeId = valueCursor.nodeReference();
                    break;
                }
            }

        } catch (IndexNotFoundKernelException | IndexBrokenKernelException | IndexNotApplicableKernelException e) {
            throw new UnableToValidateConstraintException(constraint, e, token);
        }

        if (existingNodeId != NO_SUCH_NODE) {
            int[] propertyKeys = getPropertyIds(propertyValues);
            // For nodes, we can grant read access based on property key AND values, so we need to check if we can read
            // all the properties with security context aware cursors
            var allowsReadAllProperties = false;
            try (var nodeCursor = cursors.allocateNodeCursor(ktx.cursorContext(), memoryTracker);
                    var propertyCursor = cursors.allocatePropertyCursor(ktx.cursorContext(), memoryTracker)) {
                nodeCursor.single(existingNodeId, allStoreHolder);
                // First check if the current access mode can read the node
                if (nodeCursor.next()) {
                    nodeCursor.properties(propertyCursor, PropertySelection.selection(propertyKeys));
                    // can we read all properties that the index has from the node
                    int nPropertiesRead = 0;
                    while (propertyCursor.next()) {
                        nPropertiesRead++;
                    }
                    if (nPropertiesRead == propertyKeys.length) {
                        allowsReadAllProperties = true;
                    }
                }
            }

            throw new UniquePropertyValueValidationException(
                    constraint,
                    VALIDATION,
                    new IndexEntryConflictException(
                            index.schema(),
                            allowsReadAllProperties ? existingNodeId : NO_SUCH_NODE,
                            NO_SUCH_NODE,
                            PropertyIndexQuery.asValueTuple(propertyValues)),
                    token);
        }
    }

    private static int[] getPropertyIds(PropertyIndexQuery[] queries) {
        int[] propertyIds = new int[queries.length];
        for (int i = 0; i < queries.length; i++) {
            propertyIds[i] = queries[i].propertyKeyId();
        }
        return propertyIds;
    }

    private void assertOnlineAndLock(
            IndexBackedConstraintDescriptor constraint,
            IndexDescriptor index,
            PropertyIndexQuery.ExactPredicate[] propertyValues)
            throws IndexNotFoundKernelException, IndexBrokenKernelException, UnableToValidateConstraintException {
        assertIndexOnline(index);
        SchemaDescriptor schema = index.schema();
        int[] entityTokenIds = schema.getEntityTokenIds();
        if (entityTokenIds.length != 1) {
            throw new UnableToValidateConstraintException(
                    constraint,
                    new AssertionError(format(
                            "Constraint indexes are not expected to be multi-token indexes, "
                                    + "but the constraint %s was referencing an index with the following schema: %s.",
                            constraint.userDescription(token), schema.userDescription(token))),
                    token);
        }

        // Take a big fat lock, and check for existing entity in index
        ktx.lockClient()
                .acquireExclusive(
                        ktx.lockTracer(), INDEX_ENTRY, indexEntryResourceId(entityTokenIds[0], propertyValues));
    }

    /**
     * Check so that there is not an existing relationship with the exact match of type and property
     */
    private void validateNoExistingRelWithExactValues(
            IndexBackedConstraintDescriptor constraint,
            IndexDescriptor index,
            PropertyIndexQuery.ExactPredicate[] propertyValues,
            long modifiedRel)
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException {
        if (constraint == null || index == null) {
            return;
        }
        long existingRelationshipId = NO_SUCH_RELATIONSHIP;
        try (var r = ktx.overrideWith(ktx.securityContext().withMode(Static.FULL));
                var valueCursor = cursors.allocateRelationshipValueIndexCursor(ktx.cursorContext(), memoryTracker);
                IndexReaders indexReaders = new IndexReaders(index, allStoreHolder)) {
            assertOnlineAndLock(constraint, index, propertyValues);

            allStoreHolder.relationshipIndexSeekWithFreshIndexReader(
                    valueCursor, indexReaders.createReader(), propertyValues);
            while (valueCursor.next()) {
                if (valueCursor.relationshipReference() != modifiedRel) {
                    existingRelationshipId = valueCursor.relationshipReference();
                    break;
                }
            }

        } catch (IndexNotFoundKernelException | IndexBrokenKernelException | IndexNotApplicableKernelException e) {
            throw new UnableToValidateConstraintException(constraint, e, token);
        }

        if (existingRelationshipId != NO_SUCH_RELATIONSHIP) {
            int[] propertyKeys = getPropertyIds(propertyValues);
            var allowsReadAllProperties = false;
            try (var relCursor = (DefaultRelationshipScanCursor)
                            cursors.allocateRelationshipScanCursor(ktx.cursorContext(), memoryTracker);
                    var propertyCursor = cursors.allocatePropertyCursor(ktx.cursorContext(), memoryTracker)) {
                relCursor.single(existingRelationshipId, allStoreHolder);
                //  First check if the current access mode can read the relationship
                if (relCursor.next()) {
                    relCursor.properties(propertyCursor, PropertySelection.selection(propertyKeys));
                    // can we read all properties that the index has from the relationship
                    int nPropertiesRead = 0;
                    while (propertyCursor.next()) {
                        nPropertiesRead++;
                    }
                    if (nPropertiesRead == propertyKeys.length) {
                        allowsReadAllProperties = true;
                    }
                }
            }

            throw new UniquePropertyValueValidationException(
                    constraint,
                    VALIDATION,
                    new IndexEntryConflictException(
                            index.schema(),
                            allowsReadAllProperties ? existingRelationshipId : NO_SUCH_RELATIONSHIP,
                            NO_SUCH_RELATIONSHIP,
                            PropertyIndexQuery.asValueTuple(propertyValues)),
                    token);
        }
    }

    private void assertIndexOnline(IndexDescriptor descriptor)
            throws IndexNotFoundKernelException, IndexBrokenKernelException {
        if (allStoreHolder.indexGetState(descriptor) != InternalIndexState.ONLINE) {
            throw new IndexBrokenKernelException(allStoreHolder.indexGetFailure(descriptor));
        }
    }

    @Override
    public boolean nodeRemoveLabel(long node, int labelId) throws EntityNotFoundException {
        acquireExclusiveNodeLock(node);
        storageLocks.acquireNodeLabelChangeLock(ktx.lockTracer(), node, labelId);
        ktx.assertOpen();

        singleNode(node);

        if (!nodeCursor.hasLabel(labelId)) {
            // the label wasn't there, nothing to do
            return false;
        }
        LongSet added = ktx.txState().nodeStateLabelDiffSets(node).getAdded();
        if (!added.contains(labelId)) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsRemoveLabel(ktx.securityContext(), token::labelGetName, labelId);
        }

        sharedSchemaLock(ResourceType.LABEL, labelId);
        sharedTokenSchemaLock(ResourceType.LABEL);
        ktx.txState().nodeDoRemoveLabel(labelId, node);
        if (storageReader.hasRelatedSchema(labelId, NODE)) {
            int[] existingPropertyKeyIds = loadSortedNodePropertyKeyList();
            updater.onLabelChange(
                    nodeCursor,
                    propertyCursor,
                    REMOVED_LABEL,
                    storageReader.valueIndexesGetRelated(new int[] {labelId}, existingPropertyKeyIds, NODE));
        }
        return true;
    }

    @Override
    public void nodeSetProperty(long node, int propertyKey, Value value)
            throws EntityNotFoundException, ConstraintValidationException {
        assert value != NO_VALUE;
        acquireExclusiveNodeLock(node);
        ktx.assertOpen();

        singleNode(node);
        int[] labels = nodeCursor
                .labelsAndProperties(propertyCursor, PropertySelection.selection(propertyKey))
                .all();
        var existingValue = propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
        acquireSharedLabelLocks(labels);
        int[] existingPropertyKeyIds = null;
        boolean hasRelatedSchema = storageReader.hasRelatedSchema(labels, propertyKey, NODE);
        if (hasRelatedSchema) {
            existingPropertyKeyIds = loadSortedNodePropertyKeyList();
        }

        if (existingValue == NO_VALUE) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(
                            ktx.securityContext(), this::resolvePropertyKey, Labels.from(labels), propertyKey);
            if (hasRelatedSchema) {
                checkUniquenessConstraints(node, propertyKey, value, labels, existingPropertyKeyIds);
            }

            // no existing value, we just add it
            ktx.txState().nodeDoAddProperty(node, propertyKey, value);
            if (hasRelatedSchema) {
                updater.onPropertyAdd(nodeCursor, propertyCursor, labels, propertyKey, existingPropertyKeyIds, value);
            }
        } else if (propertyHasChanged(value, existingValue)) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(
                            ktx.securityContext(), this::resolvePropertyKey, Labels.from(labels), propertyKey);
            if (hasRelatedSchema) {
                checkUniquenessConstraints(node, propertyKey, value, labels, existingPropertyKeyIds);
            }

            // the value has changed to a new value
            ktx.txState().nodeDoChangeProperty(node, propertyKey, value);
            if (hasRelatedSchema) {
                updater.onPropertyChange(
                        nodeCursor, propertyCursor, labels, propertyKey, existingPropertyKeyIds, existingValue, value);
            }
        }
    }

    @Override
    public void nodeApplyChanges(long node, IntSet addedLabels, IntSet removedLabels, IntObjectMap<Value> properties)
            throws EntityNotFoundException, ConstraintValidationException {
        // TODO there are some aspects that could be improved, e.g. index tx state updates are done per label/property
        // change and could
        //      benefit from being done more bulky on the whole change instead.

        assert intersect(addedLabels.toSortedArray(), removedLabels.toSortedArray()).length == 0;

        ktx.assertOpen();

        // lock
        if (!addedLabels.isEmpty() || !removedLabels.isEmpty()) {
            addedLabels.forEach(addedLabelId -> {
                sharedSchemaLock(ResourceType.LABEL, addedLabelId);
                storageLocks.acquireNodeLabelChangeLock(ktx.lockTracer(), node, addedLabelId);
            });
            removedLabels.forEach(removedLabelId -> {
                sharedSchemaLock(ResourceType.LABEL, removedLabelId);
                storageLocks.acquireNodeLabelChangeLock(ktx.lockTracer(), node, removedLabelId);
            });
            sharedTokenSchemaLock(ResourceType.LABEL);
        }
        acquireExclusiveNodeLock(node);
        singleNode(node);

        int[] existingLabels;
        MutableIntObjectMap<Value> existingValuesForChangedProperties = null;
        if (!properties.isEmpty()) {
            // read all affected property values in one go, to speed up changes below
            existingLabels = nodeCursor
                    .labelsAndProperties(
                            propertyCursor,
                            PropertySelection.selection(properties.keySet().toArray()))
                    .all();
            existingValuesForChangedProperties = IntObjectMaps.mutable.empty();
            while (propertyCursor.next()) {
                existingValuesForChangedProperties.put(propertyCursor.propertyKey(), propertyCursor.propertyValue());
            }
        } else {
            existingLabels = nodeCursor.labels().all();
        }
        acquireSharedLabelLocks(existingLabels);

        // create a view of labels/properties as it will look after the changes would have been applied
        int[] labelsAfter = combineLabelIds(existingLabels, addedLabels, removedLabels);
        int[] existingPropertyKeyIds = loadSortedNodePropertyKeyList();
        MutableIntSet afterPropertyKeyIdsSet = IntSets.mutable.of(existingPropertyKeyIds);
        RichIterable<IntObjectPair<Value>> propertiesKeyValueView = properties.keyValuesView();
        MutableIntSet removedPropertyKeyIdsSet = null;
        MutableIntSet changedPropertyKeyIdsSet = null;
        for (IntObjectPair<Value> property : propertiesKeyValueView) {
            int key = property.getOne();
            Value value = property.getTwo();
            if (value == NO_VALUE) {
                if (removedPropertyKeyIdsSet == null) {
                    removedPropertyKeyIdsSet = IntSets.mutable.empty();
                }
                removedPropertyKeyIdsSet.add(key);
                afterPropertyKeyIdsSet.remove(key);
            } else {
                afterPropertyKeyIdsSet.add(key);
                Value existingValue = existingValuesForChangedProperties.get(key);
                if (existingValue == null || propertyHasChanged(value, existingValue)) {
                    if (changedPropertyKeyIdsSet == null) {
                        changedPropertyKeyIdsSet = IntSets.mutable.empty();
                    }
                    changedPropertyKeyIdsSet.add(key);
                }
            }
        }
        int[] afterPropertyKeyIds = afterPropertyKeyIdsSet.toSortedArray();
        int[] changedPropertyKeyIds = changedPropertyKeyIdsSet != null
                ? changedPropertyKeyIdsSet.toSortedArray()
                : ArrayUtils.EMPTY_INT_ARRAY;

        // Check uniqueness constraints for the added labels and _actually_ changed properties
        // TODO Due to previous assumptions around very specific use cases for the schema "get related" lookups and its
        // inherent accidental
        //  complexity we have to provide very specific argument to get it to do what we want it to do.
        //  The schema cache lookup methods should be revisited to naturally accomodate this new scenario.
        int[] uniquenessPropertiesCheck = addedLabels.isEmpty() ? changedPropertyKeyIds : afterPropertyKeyIds;
        Collection<IndexBackedConstraintDescriptor> uniquenessConstraints =
                storageReader.uniquenessConstraintsGetRelated(
                        combineLabelIds(EMPTY_INT_ARRAY, addedLabels, IntSets.immutable.empty()),
                        labelsAfter,
                        uniquenessPropertiesCheck,
                        false,
                        NODE);
        SchemaMatcher.onMatchingSchema(
                uniquenessConstraints.iterator(),
                TokenConstants.ANY_PROPERTY_KEY,
                afterPropertyKeyIds,
                constraint -> validateNoExistingNodeWithExactValues(
                        constraint,
                        storageReader.indexGetForName(constraint.getName()),
                        getAllPropertyValues(nodeCursor, constraint.schema(), properties),
                        node));

        // remove labels
        if (!removedLabels.isEmpty()) {
            LongSet added = ktx.txState().nodeStateLabelDiffSets(node).getAdded();
            IntIterator removedLabelsIterator = removedLabels.intIterator();
            while (removedLabelsIterator.hasNext()) {
                int removedLabelId = removedLabelsIterator.next();
                if (!added.contains(removedLabelId)) {
                    ktx.securityAuthorizationHandler()
                            .assertAllowsRemoveLabel(ktx.securityContext(), token::labelGetName, removedLabelId);
                }
                if (contains(existingLabels, removedLabelId)) {
                    ktx.txState().nodeDoRemoveLabel(removedLabelId, node);
                    if (storageReader.hasRelatedSchema(removedLabelId, NODE)) {
                        updater.onLabelChange(
                                nodeCursor,
                                propertyCursor,
                                REMOVED_LABEL,
                                storageReader.valueIndexesGetRelated(
                                        new int[] {removedLabelId}, existingPropertyKeyIds, NODE));
                    }
                }
            }
        }
        // remove properties
        if (removedPropertyKeyIdsSet != null) {
            int[] existingLabelsMinusRemovedArray = removedLabels.isEmpty()
                    ? existingLabels
                    : combineLabelIds(existingLabels, IntSets.immutable.empty(), removedLabels);
            TokenSet existingLabelsMinusRemoved = Labels.from(existingLabelsMinusRemovedArray);
            for (int key : removedPropertyKeyIdsSet.toArray()) {
                int existingPropertyKeyIdIndex = indexOf(existingPropertyKeyIds, key);
                if (existingPropertyKeyIdIndex >= 0) {
                    // removal of existing property
                    Value existingValue = existingValuesForChangedProperties.getIfAbsent(key, () -> NO_VALUE);
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(
                                    ktx.securityContext(), this::resolvePropertyKey, existingLabelsMinusRemoved, key);
                    ktx.txState().nodeDoRemoveProperty(node, key);
                    existingPropertyKeyIds = ArrayUtils.remove(existingPropertyKeyIds, existingPropertyKeyIdIndex);
                    if (storageReader.hasRelatedSchema(existingLabelsMinusRemovedArray, key, NODE)) {
                        updater.onPropertyRemove(
                                nodeCursor,
                                propertyCursor,
                                existingLabelsMinusRemovedArray,
                                key,
                                existingPropertyKeyIds,
                                existingValue);
                    }
                }
            }
        }

        // add labels
        if (!addedLabels.isEmpty()) {
            IntIterator addedLabelsIterator = addedLabels.intIterator();
            while (addedLabelsIterator.hasNext()) {
                int addedLabelId = addedLabelsIterator.next();
                if (!contains(existingLabels, addedLabelId)) {
                    LongSet removed = ktx.txState().nodeStateLabelDiffSets(node).getRemoved();
                    if (!removed.contains(addedLabelId)) {
                        ktx.securityAuthorizationHandler()
                                .assertAllowsSetLabel(ktx.securityContext(), token::labelGetName, addedLabelId);
                    }
                    ktx.txState().nodeDoAddLabel(addedLabelId, node);
                    if (storageReader.hasRelatedSchema(addedLabelId, NODE)) {
                        updater.onLabelChange(
                                nodeCursor,
                                propertyCursor,
                                ADDED_LABEL,
                                storageReader.valueIndexesGetRelated(
                                        new int[] {addedLabelId}, existingPropertyKeyIds, NODE));
                    }
                }
            }
        }
        // add/change properties
        if (changedPropertyKeyIdsSet != null) {
            Labels labelsAfterSet = Labels.from(labelsAfter);
            MutableIntSet existingPropertyKeyIdsBeforeChange = IntSets.mutable.of(existingPropertyKeyIds);
            for (int key : changedPropertyKeyIds) {
                Value value = properties.get(key);
                Value existingValue = existingValuesForChangedProperties.getIfAbsent(key, () -> NO_VALUE);
                if (existingValue == NO_VALUE) {
                    // adding of new property
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(
                                    ktx.securityContext(), this::resolvePropertyKey, labelsAfterSet, key);
                    ktx.txState().nodeDoAddProperty(node, key, value);
                    boolean hasRelatedSchema = storageReader.hasRelatedSchema(labelsAfter, key, NODE);
                    if (hasRelatedSchema) {
                        updater.onPropertyAdd(
                                nodeCursor,
                                propertyCursor,
                                labelsAfter,
                                key,
                                existingPropertyKeyIdsBeforeChange.toSortedArray(),
                                value);
                    }
                } else // since it's in the changedPropertyKeyIds array we know that it's an actually changed value
                {
                    // changing of existing property
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(
                                    ktx.securityContext(), this::resolvePropertyKey, labelsAfterSet, key);
                    ktx.txState().nodeDoChangeProperty(node, key, value);
                    boolean hasRelatedSchema = storageReader.hasRelatedSchema(labelsAfter, key, NODE);
                    if (hasRelatedSchema) {
                        updater.onPropertyChange(
                                nodeCursor,
                                propertyCursor,
                                labelsAfter,
                                key,
                                existingPropertyKeyIdsBeforeChange.toSortedArray(),
                                existingValue,
                                value);
                    }
                }
                existingPropertyKeyIdsBeforeChange.add(key);
            }
        }
    }

    @Override
    public void relationshipApplyChanges(long relationship, IntObjectMap<Value> properties)
            throws EntityNotFoundException, ConstraintValidationException {
        // TODO there are some aspects that could be improved, e.g. index tx state updates are done per property change
        // and could
        //      benefit from being done more bulky on the whole change instead.

        ktx.assertOpen();

        // lock
        acquireExclusiveRelationshipLock(relationship);
        if (properties.isEmpty()) {
            return;
        }
        singleRelationship(relationship);
        int type = acquireSharedRelationshipTypeLock();

        // read all affected property values in one go, to speed up changes below
        MutableIntObjectMap<Value> existingValuesForChangedProperties = IntObjectMaps.mutable.empty();
        relationshipCursor.properties(
                propertyCursor, PropertySelection.selection(properties.keySet().toArray()));
        while (propertyCursor.next()) {
            existingValuesForChangedProperties.put(propertyCursor.propertyKey(), propertyCursor.propertyValue());
        }

        // create a view of properties as it will look after the changes would have been applied
        int[] existingPropertyKeyIds = loadSortedRelationshipPropertyKeyList();
        MutableIntSet afterPropertyKeyIdsSet = IntSets.mutable.of(existingPropertyKeyIds);
        boolean hasPropertyRemovals = false;
        MutableIntSet changedPropertyKeyIdsSet = null;
        RichIterable<IntObjectPair<Value>> propertiesKeyValueView = properties.keyValuesView();
        for (IntObjectPair<Value> property : propertiesKeyValueView) {
            int key = property.getOne();
            Value value = property.getTwo();
            if (value == NO_VALUE) {
                hasPropertyRemovals = true;
                afterPropertyKeyIdsSet.remove(key);
            } else {
                afterPropertyKeyIdsSet.add(key);
                Value existingValue = existingValuesForChangedProperties.get(key);
                if (existingValue == null || propertyHasChanged(value, existingValue)) {
                    if (changedPropertyKeyIdsSet == null) {
                        changedPropertyKeyIdsSet = IntSets.mutable.empty();
                    }
                    changedPropertyKeyIdsSet.add(key);
                }
            }
        }

        int[] changedPropertyKeyIds = changedPropertyKeyIdsSet != null
                ? changedPropertyKeyIdsSet.toSortedArray()
                : ArrayUtils.EMPTY_INT_ARRAY;

        // Check uniqueness constraints for the _actually_ changed properties
        if (changedPropertyKeyIdsSet != null) {
            int[] afterPropertyKeyIds = afterPropertyKeyIdsSet.toSortedArray();

            Collection<IndexBackedConstraintDescriptor> uniquenessConstraints =
                    storageReader.uniquenessConstraintsGetRelated(
                            new int[] {type}, changedPropertyKeyIds, RELATIONSHIP);
            SchemaMatcher.onMatchingSchema(
                    uniquenessConstraints.iterator(),
                    TokenConstants.ANY_PROPERTY_KEY,
                    afterPropertyKeyIds,
                    constraint -> validateNoExistingRelWithExactValues(
                            constraint,
                            storageReader.indexGetForName(constraint.getName()),
                            getAllPropertyValues(relationshipCursor, constraint.schema(), properties),
                            relationship));
        }

        // remove properties
        if (hasPropertyRemovals) {
            for (IntObjectPair<Value> property : propertiesKeyValueView) {
                int key = property.getOne();
                Value value = property.getTwo();
                if (value != NO_VALUE) {
                    continue;
                }
                int existingPropertyKeyIdIndex = indexOf(existingPropertyKeyIds, key);
                if (existingPropertyKeyIdIndex >= 0) {
                    // removal of existing property
                    Value existingValue = existingValuesForChangedProperties.getIfAbsent(key, () -> NO_VALUE);
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, key);
                    ktx.txState()
                            .relationshipDoRemoveProperty(
                                    relationship,
                                    type,
                                    relationshipCursor.sourceNodeReference(),
                                    relationshipCursor.targetNodeReference(),
                                    key);
                    existingPropertyKeyIds = remove(existingPropertyKeyIds, existingPropertyKeyIdIndex);
                    if (storageReader.hasRelatedSchema(new int[] {type}, key, RELATIONSHIP)) {
                        updater.onPropertyRemove(
                                relationshipCursor, propertyCursor, type, key, existingPropertyKeyIds, existingValue);
                    }
                }
            }
        }

        // add/change properties
        if (changedPropertyKeyIdsSet != null) {
            MutableIntSet existingPropertyKeyIdsBeforeChange = IntSets.mutable.of(existingPropertyKeyIds);
            for (int key : changedPropertyKeyIds) {
                Value value = properties.get(key);
                Value existingValue = existingValuesForChangedProperties.getIfAbsent(key, () -> NO_VALUE);
                if (existingValue == NO_VALUE) {
                    // adding of new property
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, key);
                    ktx.txState()
                            .relationshipDoReplaceProperty(
                                    relationship,
                                    relationshipCursor.type(),
                                    relationshipCursor.sourceNodeReference(),
                                    relationshipCursor.targetNodeReference(),
                                    key,
                                    NO_VALUE,
                                    value);
                    boolean hasRelatedSchema = storageReader.hasRelatedSchema(new int[] {type}, key, RELATIONSHIP);
                    if (hasRelatedSchema) {
                        updater.onPropertyAdd(
                                relationshipCursor,
                                propertyCursor,
                                type,
                                key,
                                existingPropertyKeyIdsBeforeChange.toSortedArray(),
                                value);
                    }
                } else // since it's in the changedPropertyKeyIds array we know that it's an actually changed value
                {
                    // changing of existing property
                    ktx.securityAuthorizationHandler()
                            .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, key);
                    ktx.txState()
                            .relationshipDoReplaceProperty(
                                    relationship,
                                    relationshipCursor.type(),
                                    relationshipCursor.sourceNodeReference(),
                                    relationshipCursor.targetNodeReference(),
                                    key,
                                    existingValue,
                                    value);
                    boolean hasRelatedSchema = storageReader.hasRelatedSchema(new int[] {type}, key, RELATIONSHIP);
                    if (hasRelatedSchema) {
                        updater.onPropertyChange(
                                relationshipCursor,
                                propertyCursor,
                                type,
                                key,
                                existingPropertyKeyIdsBeforeChange.toSortedArray(),
                                existingValue,
                                value);
                    }
                }
                existingPropertyKeyIdsBeforeChange.add(key);
            }
        }
    }

    private static int[] combineLabelIds(int[] existingLabels, IntSet addedLabels, IntSet removedLabels) {
        if (addedLabels.isEmpty() && removedLabels.isEmpty()) {
            return existingLabels;
        }

        MutableIntSet result = IntSets.mutable.of(existingLabels);
        addedLabels.forEach(result::add);
        removedLabels.forEach(result::remove);
        return result.toSortedArray();
    }

    private String resolvePropertyKey(int propertyKey) {
        String propKeyName;
        try {
            propKeyName = token.propertyKeyName(propertyKey);
        } catch (PropertyKeyIdNotFoundKernelException e) {
            propKeyName = "<unknown>";
        }
        return propKeyName;
    }

    private void checkUniquenessConstraints(
            long node, int propertyKey, Value value, int[] labels, int[] existingPropertyKeyIds)
            throws ConstraintValidationException {
        Collection<IndexBackedConstraintDescriptor> uniquenessConstraints =
                storageReader.uniquenessConstraintsGetRelated(labels, propertyKey, NODE);
        SchemaMatcher.onMatchingSchema(
                uniquenessConstraints.iterator(),
                propertyKey,
                existingPropertyKeyIds,
                constraint -> validateNoExistingNodeWithExactValues(
                        constraint,
                        storageReader.indexGetForName(constraint.getName()),
                        getAllPropertyValues(nodeCursor, constraint.schema(), propertyKey, value),
                        node));
    }

    private void checkRelationshipUniquenessConstraints(
            long rel, int propertyKey, Value value, int type, int[] existingPropertyKeyIds)
            throws ConstraintValidationException {
        Collection<IndexBackedConstraintDescriptor> uniquenessConstraints =
                storageReader.uniquenessConstraintsGetRelated(new int[] {type}, propertyKey, RELATIONSHIP);
        SchemaMatcher.onMatchingSchema(
                uniquenessConstraints.iterator(),
                propertyKey,
                existingPropertyKeyIds,
                constraint -> validateNoExistingRelWithExactValues(
                        constraint,
                        storageReader.indexGetForName(constraint.getName()),
                        getAllPropertyValues(relationshipCursor, constraint.schema(), propertyKey, value),
                        rel));
    }

    @Override
    public Value nodeRemoveProperty(long node, int propertyKey) throws EntityNotFoundException {
        acquireExclusiveNodeLock(node);
        ktx.assertOpen();
        singleNode(node);
        Value existingValue = readNodeProperty(propertyKey);

        if (existingValue != NO_VALUE) {
            int[] labels = acquireSharedNodeLabelLocks();
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(
                            ktx.securityContext(), this::resolvePropertyKey, Labels.from(labels), propertyKey);
            ktx.txState().nodeDoRemoveProperty(node, propertyKey);
            if (storageReader.hasRelatedSchema(labels, propertyKey, NODE)) {
                updater.onPropertyRemove(
                        nodeCursor,
                        propertyCursor,
                        labels,
                        propertyKey,
                        loadSortedNodePropertyKeyList(),
                        existingValue);
            }
        }

        return existingValue;
    }

    @Override
    public void relationshipSetProperty(long relationship, int propertyKey, Value value)
            throws EntityNotFoundException, ConstraintValidationException {
        acquireExclusiveRelationshipLock(relationship);
        ktx.assertOpen();
        singleRelationship(relationship);
        int type = acquireSharedRelationshipTypeLock();
        Value existingValue = readRelationshipProperty(propertyKey);
        int[] existingPropertyKeyIds = null;
        boolean hasRelatedSchema = storageReader.hasRelatedSchema(new int[] {type}, propertyKey, RELATIONSHIP);
        if (hasRelatedSchema) {
            existingPropertyKeyIds = loadSortedRelationshipPropertyKeyList();
        }
        if (existingValue == NO_VALUE) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, propertyKey);
            if (hasRelatedSchema) {
                checkRelationshipUniquenessConstraints(relationship, propertyKey, value, type, existingPropertyKeyIds);
            }
            ktx.txState()
                    .relationshipDoReplaceProperty(
                            relationship,
                            relationshipCursor.type(),
                            relationshipCursor.sourceNodeReference(),
                            relationshipCursor.targetNodeReference(),
                            propertyKey,
                            NO_VALUE,
                            value);
            if (hasRelatedSchema) {
                updater.onPropertyAdd(
                        relationshipCursor, propertyCursor, type, propertyKey, existingPropertyKeyIds, value);
            }
            return;
        }
        if (propertyHasChanged(existingValue, value)) {
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, propertyKey);
            if (hasRelatedSchema) {
                checkRelationshipUniquenessConstraints(relationship, propertyKey, value, type, existingPropertyKeyIds);
            }
            ktx.txState()
                    .relationshipDoReplaceProperty(
                            relationship,
                            relationshipCursor.type(),
                            relationshipCursor.sourceNodeReference(),
                            relationshipCursor.targetNodeReference(),
                            propertyKey,
                            existingValue,
                            value);
            if (hasRelatedSchema) {
                updater.onPropertyChange(
                        relationshipCursor,
                        propertyCursor,
                        type,
                        propertyKey,
                        existingPropertyKeyIds,
                        existingValue,
                        value);
            }
        }
    }

    @Override
    public Value relationshipRemoveProperty(long relationship, int propertyKey) throws EntityNotFoundException {
        acquireExclusiveRelationshipLock(relationship);
        ktx.assertOpen();
        singleRelationship(relationship);
        Value existingValue = readRelationshipProperty(propertyKey);

        if (existingValue != NO_VALUE) {
            int type = acquireSharedRelationshipTypeLock();
            ktx.securityAuthorizationHandler()
                    .assertAllowsSetProperty(ktx.securityContext(), this::resolvePropertyKey, type, propertyKey);
            ktx.txState()
                    .relationshipDoRemoveProperty(
                            relationship,
                            relationshipCursor.type(),
                            relationshipCursor.sourceNodeReference(),
                            relationshipCursor.targetNodeReference(),
                            propertyKey);
            if (storageReader.hasRelatedSchema(new int[] {type}, propertyKey, RELATIONSHIP)) {
                updater.onPropertyRemove(
                        relationshipCursor,
                        propertyCursor,
                        type,
                        propertyKey,
                        loadSortedRelationshipPropertyKeyList(),
                        existingValue);
            }
        }

        return existingValue;
    }

    private Value readNodeProperty(int propertyKey) {
        nodeCursor.properties(propertyCursor, PropertySelection.selection(propertyKey));

        // Find out if the property had a value
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    private Value readRelationshipProperty(int propertyKey) {
        relationshipCursor.properties(propertyCursor, PropertySelection.selection(propertyKey));

        // Find out if the property had a value
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    public CursorFactory cursors() {
        return cursors;
    }

    public Procedures procedures() {
        return allStoreHolder;
    }

    public void release() {
        if (nodeCursor != null) {
            nodeCursor.close();
            nodeCursor = null;
        }
        if (restrictedNodeCursor != null) {
            restrictedNodeCursor.close();
            restrictedNodeCursor = null;
        }
        if (propertyCursor != null) {
            propertyCursor.close();
            propertyCursor = null;
        }
        if (restrictedPropertyCursor != null) {
            restrictedPropertyCursor.close();
            restrictedPropertyCursor = null;
        }
        if (relationshipCursor != null) {
            relationshipCursor.close();
            relationshipCursor = null;
        }
        if (restrictedRelationshipCursor != null) {
            restrictedRelationshipCursor.close();
            restrictedRelationshipCursor = null;
        }

        cursors.assertClosed();
        cursors.release();
    }

    public Token token() {
        return token;
    }

    public SchemaRead schemaRead() {
        return allStoreHolder;
    }

    public Read dataRead() {
        return allStoreHolder;
    }

    public DefaultNodeCursor nodeCursor() {
        return restrictedNodeCursor;
    }

    public DefaultRelationshipScanCursor relationshipCursor() {
        return restrictedRelationshipCursor;
    }

    public DefaultPropertyCursor propertyCursor() {
        return restrictedPropertyCursor;
    }

    @Override
    public IndexProviderDescriptor indexProviderByName(String providerName) {
        ktx.assertOpen();
        return indexProviders.indexProviderByName(providerName);
    }

    @Override
    public IndexType indexTypeByProviderName(String providerName) {
        ktx.assertOpen();
        return indexProviders.indexTypeByProviderName(providerName);
    }

    @Override
    public List<IndexProviderDescriptor> indexProvidersByType(IndexType indexType) {
        ktx.assertOpen();
        return indexProviders.indexProvidersByType(indexType);
    }

    @Override
    public IndexDescriptor indexCreate(IndexPrototype prototype) throws KernelException {
        // can use index provider
        prototype = ensureIndexPrototypeHasIndexProvider(prototype);

        final var indexProviderDescriptor = prototype.getIndexProvider();
        final var indexProvider = indexProviders.getIndexProvider(indexProviderDescriptor);
        assertSupportedInVersion(
                indexProvider.getMinimumRequiredVersion(),
                "Failed to create index with provider '%s'.",
                indexProviderDescriptor.name());

        // valid schema checks
        final var indexType = prototype.getIndexType();
        exclusiveSchemaLock(prototype.schema());

        if (indexType == IndexType.VECTOR) {
            switch (prototype.schema().entityType()) {
                case NODE -> assertSupportedInVersion(
                        KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED, "Failed to create node vector index.");
                case RELATIONSHIP -> {
                    boolean supported = true;
                    final var descriptor = prototype.getIndexProvider();
                    final var version = VectorIndexVersion.fromDescriptor(descriptor);
                    final var unsupportedMessage =
                            new StringBuilder(384).append("Failed to create relationship vector index.");
                    if (version == VectorIndexVersion.V1_0) {
                        supported = false;
                        final var latestDescriptor = VectorIndexVersion.latestSupportedVersion(
                                        dbmsRuntimeVersionProvider.getVersion().kernelVersion())
                                .descriptor();
                        unsupportedMessage
                                .append(" Relationship vector indexes with provider '")
                                .append(descriptor.name())
                                .append("' are not supported");

                        if (!latestDescriptor.equals(descriptor)) {
                            unsupportedMessage
                                    .append(", use a newer vector index provider such as '")
                                    .append(latestDescriptor.name())
                                    .append('\'');
                        }

                        unsupportedMessage.append('.');
                    }

                    supported &= checkSupportedInVerson(unsupportedMessage, KernelVersion.VERSION_VECTOR_2_INTRODUCED);
                    if (!supported) {
                        throw new UnsupportedOperationException(unsupportedMessage.toString());
                    }
                }
            }
        }

        assertValidDescriptor(prototype.schema(), INDEX_CREATION);

        if ((indexType == IndexType.TEXT || indexType == IndexType.POINT || indexType == IndexType.VECTOR)
                && prototype.schema().getPropertyIds().length > 1) {
            throw new UnsupportedOperationException(
                    "Composite indexes are not supported for " + indexType.name() + " index type.");
        }

        // ensure named
        prototype = ensureIndexPrototypeHasName(prototype);
        final var name = prototype.getName().orElseThrow();

        // take locks
        ktx.assertOpen();
        exclusiveSchemaLock(prototype.schema());
        exclusiveSchemaNameLock(name);
        assertNoBlockingSchemaRulesExists(prototype);

        return indexDoCreate(prototype);
    }

    // Note: this will be sneakily executed by an internal transaction, so no additional locking is required.
    public IndexDescriptor indexUniqueCreate(IndexPrototype prototype) {
        return indexDoCreate(prototype);
    }

    private IndexDescriptor indexDoCreate(IndexPrototype prototype) {
        indexProviders.validateIndexPrototype(prototype);
        TransactionState transactionState = ktx.txState();
        long schemaRecordId = commandCreationContext.reserveSchema();
        IndexDescriptor index = prototype.materialise(schemaRecordId);
        index = indexProviders.completeConfiguration(index);
        transactionState.indexDoAdd(index);
        return index;
    }

    private IndexPrototype ensureIndexPrototypeHasName(IndexPrototype prototype) throws KernelException {
        return prototype.getName().isEmpty() ? prototype.withName(generateNameFrom(prototype)) : prototype;
    }

    private static <E extends Exception> String[] resolveTokenNames(
            ThrowingIntFunction<String, E> resolver, int[] tokenIds) throws E {
        String[] names = new String[tokenIds.length];
        for (int i = 0; i < tokenIds.length; i++) {
            names[i] = resolver.apply(tokenIds[i]);
        }
        return names;
    }

    private IndexPrototype ensureIndexPrototypeHasIndexProvider(IndexPrototype prototype) {
        return prototype.getIndexProvider() == IndexProviderDescriptor.UNDECIDED
                ? prototype.withIndexProvider(
                        switch (prototype.getIndexType()) {
                            case LOOKUP -> indexProviders.getTokenIndexProvider();
                            case FULLTEXT -> indexProviders.getFulltextProvider();
                            case TEXT -> indexProviders.getTextIndexProvider();
                            case RANGE -> indexProviders.getDefaultProvider();
                            case POINT -> indexProviders.getPointIndexProvider();
                            case VECTOR -> indexProviders.getVectorIndexProvider();
                        })
                : prototype;
    }

    @Override
    public void indexDrop(IndexDescriptor index) throws SchemaKernelException {
        if (index == IndexDescriptor.NO_INDEX) {
            throw new DropIndexFailureException("No index was specified.");
        }
        exclusiveSchemaLock(index.schema());
        exclusiveSchemaNameLock(index.getName());
        assertIndexExistsForDrop(index);
        if (index.isUnique() && allStoreHolder.indexGetOwningUniquenessConstraintId(index) != null) {
            IndexBelongsToConstraintException cause = new IndexBelongsToConstraintException(index.schema());
            throw new DropIndexFailureException("Unable to drop index: " + cause.getUserMessage(token), cause);
        }
        ktx.txState().indexDoDrop(index);
    }

    private void assertIndexExistsForDrop(IndexDescriptor index) throws DropIndexFailureException {
        try {
            allStoreHolder.assertIndexExists(index);
        } catch (IndexNotFoundKernelException e) {
            throw new DropIndexFailureException("Unable to drop index: " + e.getUserMessage(token), e);
        }
    }

    @Override
    public void indexDrop(String indexName) throws SchemaKernelException {
        exclusiveSchemaNameLock(indexName);
        IndexDescriptor index = allStoreHolder.indexGetForName(indexName);
        if (index == IndexDescriptor.NO_INDEX) {
            throw new DropIndexFailureException(
                    "Unable to drop index called `" + indexName + "`. There is no such index.");
        }
        exclusiveSchemaLock(index.schema());
        assertIndexExistsForDrop(index);
        if (index.isUnique() && allStoreHolder.indexGetOwningUniquenessConstraintId(index) != null) {
            IndexBelongsToConstraintException cause = new IndexBelongsToConstraintException(indexName, index.schema());
            throw new DropIndexFailureException("Unable to drop index: " + cause.getUserMessage(token), cause);
        }
        ktx.txState().indexDoDrop(index);
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate(IndexPrototype prototype) throws KernelException {
        SchemaDescriptor schema = prototype.schema();

        if (schema.entityType() == RELATIONSHIP) {
            assertSupportedInVersion(
                    KernelVersion.VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED,
                    "Failed to create Relationship Uniqueness constraint.");
        }

        exclusiveSchemaLock(schema);
        ktx.assertOpen();
        prototype = ensureIndexPrototypeHasIndexProvider(prototype);

        UniquenessConstraintDescriptor constraint =
                ConstraintDescriptorFactory.uniqueForSchema(schema, prototype.getIndexType());
        try {
            assertValidDescriptor(schema, SchemaKernelException.OperationContext.CONSTRAINT_CREATION);
            if (prototype.getName().isEmpty()) {
                constraint = ensureConstraintHasName(constraint);
                prototype = prototype.withName(constraint.getName());
            } else {
                constraint = constraint.withName(prototype.getName().get());
            }
            exclusiveSchemaNameLock(constraint.getName());
            assertNoBlockingSchemaRulesExists(constraint);
        } catch (KernelException e) {
            exclusiveSchemaUnlock(
                    schema); // Try not to hold on to exclusive schema locks when we don't strictly need them.
            throw e;
        }

        // Create constraints
        constraint = indexBackedConstraintCreate(constraint, prototype, ignored -> {});
        return constraint;
    }

    private void assertNoBlockingSchemaRulesExists(IndexPrototype prototype)
            throws EquivalentSchemaRuleAlreadyExistsException, IndexWithNameAlreadyExistsException,
                    ConstraintWithNameAlreadyExistsException, AlreadyIndexedException, AlreadyConstrainedException {
        Optional<String> prototypeName = prototype.getName();
        if (prototypeName.isEmpty()) {
            throw new IllegalStateException("Expected index to always have a name by this point");
        }
        String name = prototypeName.get();

        // Equivalent index
        var indexWithSameSchemaAndType = allStoreHolder.index(prototype.schema(), prototype.getIndexType());

        if (indexWithSameSchemaAndType.getName().equals(name)
                && indexWithSameSchemaAndType.isUnique() == prototype.isUnique()) {
            throw new EquivalentSchemaRuleAlreadyExistsException(indexWithSameSchemaAndType, INDEX_CREATION, token);
        }

        // Name conflict with other schema rule
        assertSchemaRuleWithNameDoesNotExist(name);

        // Already constrained
        final Iterator<ConstraintDescriptor> constraintWithSameSchema =
                allStoreHolder.constraintsGetForSchema(prototype.schema());
        while (constraintWithSameSchema.hasNext()) {
            final ConstraintDescriptor constraint = constraintWithSameSchema.next();
            if (constraint.isIndexBackedConstraint()) {
                // Index-backed constraints only blocks indexes of the same type.
                if (constraint.asIndexBackedConstraint().indexType() == prototype.getIndexType()) {
                    throw new AlreadyConstrainedException(constraint, INDEX_CREATION, token);
                }
            }
        }

        // Already indexed
        if (indexWithSameSchemaAndType != IndexDescriptor.NO_INDEX) {
            throw new AlreadyIndexedException(prototype.schema(), INDEX_CREATION, token);
        }
    }

    private void assertNoBlockingSchemaRulesExists(ConstraintDescriptor constraint)
            throws EquivalentSchemaRuleAlreadyExistsException, IndexWithNameAlreadyExistsException,
                    ConstraintWithNameAlreadyExistsException, ConflictingConstraintException,
                    AlreadyConstrainedException, AlreadyIndexedException {
        final String name = constraint.getName();
        if (name == null) {
            throw new IllegalStateException("Expected constraint to always have a name by this point");
        }

        // Equivalent constraint
        final List<ConstraintDescriptor> constraintsWithSameSchema =
                Iterators.asList(allStoreHolder.constraintsGetForSchema(constraint.schema()));
        for (ConstraintDescriptor constraintWithSameSchema : constraintsWithSameSchema) {
            if (constraint.equals(constraintWithSameSchema)
                    && constraint.getName().equals(constraintWithSameSchema.getName())) {
                throw new EquivalentSchemaRuleAlreadyExistsException(
                        constraintWithSameSchema, CONSTRAINT_CREATION, token);
            }
        }

        // Name conflict with other schema rule
        assertSchemaRuleWithNameDoesNotExist(name);

        // Already constrained
        for (ConstraintDescriptor constraintWithSameSchema : constraintsWithSameSchema) {
            // For Type Constraints, verify that no prior constraints conflict with the addition.
            if (constraint.isPropertyTypeConstraint()
                    && constraintWithSameSchema.isPropertyTypeConstraint()
                    && !constraint.equals(constraintWithSameSchema)) {
                throw new ConflictingConstraintException(constraintWithSameSchema, token);
            }

            boolean creatingIndexBackedConstraint = constraint.isIndexBackedConstraint();
            boolean existingIndexBackedConstraint = constraintWithSameSchema.isIndexBackedConstraint();
            if (creatingIndexBackedConstraint == existingIndexBackedConstraint) {
                // Only index-backed constraints of the same index type block each other.
                if (creatingIndexBackedConstraint
                                && constraintWithSameSchema
                                                .asIndexBackedConstraint()
                                                .indexType()
                                        == constraint.asIndexBackedConstraint().indexType()
                        || !creatingIndexBackedConstraint && constraint.type() == constraintWithSameSchema.type()) {
                    throw new AlreadyConstrainedException(constraintWithSameSchema, CONSTRAINT_CREATION, token);
                }
            }
        }

        // Already indexed
        // A node-key or uniqueness constraint with relationship schema is not counted as index-backed so we don't check
        // blocking indexes for them. But that will fail later anyway since we don't support such constraints.
        if (constraint.isIndexBackedConstraint()) {
            IndexDescriptor existingIndex = allStoreHolder.index(
                    constraint.schema(), constraint.asIndexBackedConstraint().indexType());
            // An index of the same type on the schema blocks constraint creation.
            if (existingIndex != IndexDescriptor.NO_INDEX) {
                throw new AlreadyIndexedException(existingIndex.schema(), CONSTRAINT_CREATION, token);
            }
        }

        // Constraint backed by similar index dropped in this transaction.
        // We cannot allow this because if we crash while new backing index
        // is being populated we will end up with two indexes on the same schema and type.
        if (constraint.isIndexBackedConstraint() && ktx.hasTxStateWithChanges()) {
            for (ConstraintDescriptor droppedConstraint :
                    ktx.txState().constraintsChanges().getRemoved()) {
                // If dropped and new constraint have similar backing index we cannot allow this constraint creation
                if (droppedConstraint.isIndexBackedConstraint()
                        && constraint.schema().equals(droppedConstraint.schema())
                        && droppedConstraint.asIndexBackedConstraint().indexType()
                                == constraint.asIndexBackedConstraint().indexType()) {
                    throw new UnsupportedOperationException(format(
                            "Trying to create constraint '%s' in same transaction as dropping '%s'. "
                                    + "This is not supported because they are both backed by similar indexes. "
                                    + "Please drop constraint in a separate transaction before creating the new one.",
                            constraint.getName(), droppedConstraint.getName()));
                }
            }
        }
    }

    private void assertSchemaRuleWithNameDoesNotExist(String name)
            throws IndexWithNameAlreadyExistsException, ConstraintWithNameAlreadyExistsException {
        // Check constraints first because some of them will also be backed by indexes
        final ConstraintDescriptor constraintWithSameName = allStoreHolder.constraintGetForName(name);
        if (constraintWithSameName != null) {
            throw new ConstraintWithNameAlreadyExistsException(name);
        }
        final IndexDescriptor indexWithSameName = allStoreHolder.indexGetForName(name);
        if (indexWithSameName != IndexDescriptor.NO_INDEX) {
            throw new IndexWithNameAlreadyExistsException(name);
        }
    }

    private void assertSupportedInVersion(KernelVersion minimumVersionForSupport, String message, Object... args) {
        final var unsupportedMessage = new StringBuilder(256).append(message.formatted(args));
        if (!checkSupportedInVerson(unsupportedMessage, minimumVersionForSupport)) {
            throw new UnsupportedOperationException(unsupportedMessage.toString());
        }
    }

    private boolean checkSupportedInVerson(StringBuilder sb, KernelVersion minimumVersionForSupport) {
        final var currentStoreVersion = kernelVersionProvider.kernelVersion();
        if (currentStoreVersion.isAtLeast(minimumVersionForSupport)) {
            // New or upgraded store, good to go
            return true;
        }

        final var currentDbmsVersion = dbmsRuntimeVersionProvider.getVersion().kernelVersion();
        if (currentDbmsVersion.isAtLeast(minimumVersionForSupport)) {
            // Dbms runtime version is good, current transaction will trigger the upgrade transaction
            // we will double-check the kernel version during commit
            return true;
        }

        if (!sb.isEmpty()) {
            sb.append(' ');
        }
        sb.append("Version was ")
                .append(currentDbmsVersion.name())
                .append(", but required version for operation is ")
                .append(minimumVersionForSupport.name())
                .append(". Please upgrade dbms using 'dbms.upgrade()'.");

        return false;
    }

    @Override
    public ConstraintDescriptor keyConstraintCreate(IndexPrototype prototype) throws KernelException {
        SchemaDescriptor schema = prototype.schema();

        if (schema.entityType() == RELATIONSHIP) {
            assertSupportedInVersion(
                    KernelVersion.VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED,
                    "Failed to create Relationship Key constraint.");
        }

        exclusiveSchemaLock(schema);
        ktx.assertOpen();
        prototype = ensureIndexPrototypeHasIndexProvider(prototype);
        KeyConstraintDescriptor constraint = ConstraintDescriptorFactory.keyForSchema(schema, prototype.getIndexType());

        try {
            // Check data integrity
            assertValidDescriptor(schema, SchemaKernelException.OperationContext.CONSTRAINT_CREATION);

            if (prototype.getName().isEmpty()) {
                constraint = ensureConstraintHasName(constraint);
                prototype = prototype.withName(constraint.getName());
            } else {
                constraint = constraint.withName(prototype.getName().get());
            }

            exclusiveSchemaNameLock(constraint.getName());
            assertNoBlockingSchemaRulesExists(constraint);
        } catch (SchemaKernelException e) {
            exclusiveSchemaUnlock(schema);
            throw e;
        }

        // Check that key constraints are supported before we start doing any work
        constraintSemantics.assertKeyConstraintAllowed(constraint.schema());

        // create constraint and enforce it after index population when we have the lock again
        indexBackedConstraintCreate(constraint, prototype, this::enforceKeyConstraint);
        return constraint;
    }

    private void enforceKeyConstraint(SchemaDescriptor schema) throws KernelException {
        if (schema.entityType() == NODE) {
            enforceNodeKeyConstraint(schema);
        } else {
            enforceRelKeyConstraint(schema);
        }
    }

    private void enforceNodeKeyConstraint(SchemaDescriptor schema) throws KernelException {
        IndexDescriptor index = allStoreHolder.findUsableTokenIndex(NODE);
        if (index != IndexDescriptor.NO_INDEX) {
            try (var cursor = cursors.allocateFullAccessNodeLabelIndexCursor(ktx.cursorContext())) {
                var session = allStoreHolder.tokenReadSession(index);
                allStoreHolder.nodeLabelScan(
                        session, cursor, unconstrained(), new TokenPredicate(schema.getLabelId()), ktx.cursorContext());
                constraintSemantics.validateNodeKeyConstraint(
                        cursor, nodeCursor, propertyCursor, schema.asLabelSchemaDescriptor(), token);
            }
        } else {
            try (var cursor = cursors.allocateFullAccessNodeCursor(ktx.cursorContext())) {
                allStoreHolder.allNodesScan(cursor);
                constraintSemantics.validateNodeKeyConstraint(
                        new FilteringNodeCursorWrapper(cursor, CursorPredicates.hasLabel(schema.getLabelId())),
                        propertyCursor,
                        schema.asLabelSchemaDescriptor(),
                        token);
            }
        }
    }

    private void enforceRelKeyConstraint(SchemaDescriptor schema) throws KernelException {
        IndexDescriptor index = allStoreHolder.findUsableTokenIndex(RELATIONSHIP);
        if (index != IndexDescriptor.NO_INDEX) {
            try (var cursor = cursors.allocateFullAccessRelationshipTypeIndexCursor(ktx.cursorContext())) {
                var session = allStoreHolder.tokenReadSession(index);
                allStoreHolder.relationshipTypeScan(
                        session,
                        cursor,
                        unconstrained(),
                        new TokenPredicate(schema.getRelTypeId()),
                        ktx.cursorContext());
                constraintSemantics.validateRelKeyConstraint(
                        cursor, relationshipCursor, propertyCursor, schema.asRelationshipTypeSchemaDescriptor(), token);
            }
        } else {
            try (var cursor = cursors.allocateFullAccessRelationshipScanCursor(ktx.cursorContext())) {
                allStoreHolder.allRelationshipsScan(cursor);
                constraintSemantics.validateRelKeyConstraint(
                        new FilteringRelationshipScanCursorWrapper(
                                cursor, CursorPredicates.hasType(schema.getRelTypeId())),
                        propertyCursor,
                        schema.asRelationshipTypeSchemaDescriptor(),
                        token);
            }
        }
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate(
            LabelSchemaDescriptor schema, String name, boolean isDependent) throws KernelException {

        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint(schema, name, isDependent);

        // enforce constraints
        enforceNodePropertyExistenceConstraint(schema, isDependent);

        // create constraint
        ktx.txState().constraintDoAdd(constraint);
        return constraint;
    }

    private void enforceNodePropertyConstraint(
            LabelSchemaDescriptor schema,
            NodeValidatorWithIndex nodeValidatorWithIndex,
            NodeValidatorWithoutIndex nodeValidatorWithoutIndex)
            throws KernelException {
        IndexDescriptor index = allStoreHolder.findUsableTokenIndex(NODE);
        if (index != IndexDescriptor.NO_INDEX) {
            try (var cursor = cursors.allocateFullAccessNodeLabelIndexCursor(ktx.cursorContext())) {
                var session = allStoreHolder.tokenReadSession(index);
                allStoreHolder.nodeLabelScan(
                        session, cursor, unconstrained(), new TokenPredicate(schema.getLabelId()), ktx.cursorContext());
                nodeValidatorWithIndex.validate(cursor, nodeCursor, propertyCursor, token);
            }
        } else {
            try (var cursor = cursors.allocateFullAccessNodeCursor(ktx.cursorContext())) {
                allStoreHolder.allNodesScan(cursor);
                nodeValidatorWithoutIndex.validate(
                        new FilteringNodeCursorWrapper(cursor, CursorPredicates.hasLabel(schema.getLabelId())),
                        propertyCursor,
                        token);
            }
        }
    }

    private void enforceNodePropertyExistenceConstraint(LabelSchemaDescriptor schema, boolean isDependent)
            throws KernelException {
        enforceNodePropertyConstraint(
                schema,
                (allNodes, nodeCursor, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateNodePropertyExistenceConstraint(
                                allNodes, nodeCursor, propertyCursor, schema, tokenNameLookup, isDependent),
                (nodeCursor, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateNodePropertyExistenceConstraint(
                                nodeCursor, propertyCursor, schema, tokenNameLookup, isDependent));
    }

    private void enforceNodePropertyTypeConstraint(TypeConstraintDescriptor descriptor) throws KernelException {
        enforceNodePropertyConstraint(
                descriptor.schema().asLabelSchemaDescriptor(),
                (allNodes, nodeCursor, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateNodePropertyTypeConstraint(
                                allNodes, nodeCursor, propertyCursor, descriptor, tokenNameLookup),
                (nodeCursor, propertyCursor, tokenNameLookup) -> constraintSemantics.validateNodePropertyTypeConstraint(
                        nodeCursor, propertyCursor, descriptor, tokenNameLookup));
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            RelationTypeSchemaDescriptor schema, String name, boolean isDependent) throws KernelException {

        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint(schema, name, isDependent);

        // enforce constraints
        enforceRelationshipPropertyExistenceConstraint(schema, isDependent);

        // Create
        ktx.txState().constraintDoAdd(constraint);
        return constraint;
    }

    private void enforceRelationshipPropertyConstraint(
            RelationTypeSchemaDescriptor schema,
            RelValidatorWithIndex relValidatorWithIndex,
            RelValidatorWithoutIndex relValidatorWithoutIndex)
            throws KernelException {
        var index = allStoreHolder.findUsableTokenIndex(RELATIONSHIP);
        if (index != IndexDescriptor.NO_INDEX) {
            try (var fullAccessIndexCursor =
                    cursors.allocateFullAccessRelationshipTypeIndexCursor(ktx.cursorContext())) {
                var session = allStoreHolder.tokenReadSession(index);
                allStoreHolder.relationshipTypeScan(
                        session,
                        fullAccessIndexCursor,
                        unconstrained(),
                        new TokenPredicate(schema.getRelTypeId()),
                        ktx.cursorContext());
                relValidatorWithIndex.validate(fullAccessIndexCursor, propertyCursor, token);
            }
        } else {
            // fallback to all relationship scan
            try (var fullAccessCursor = cursors.allocateFullAccessRelationshipScanCursor(ktx.cursorContext())) {
                allStoreHolder.allRelationshipsScan(fullAccessCursor);
                relValidatorWithoutIndex.validate(
                        new FilteringRelationshipScanCursorWrapper(
                                fullAccessCursor, CursorPredicates.hasType(schema.getRelTypeId())),
                        propertyCursor,
                        token);
            }
        }
    }

    private void enforceRelationshipPropertyExistenceConstraint(
            RelationTypeSchemaDescriptor schema, boolean isDependent) throws KernelException {
        enforceRelationshipPropertyConstraint(
                schema,
                (relationships, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateRelationshipPropertyExistenceConstraint(
                                relationships, propertyCursor, schema, tokenNameLookup, isDependent),
                (relationships, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateRelationshipPropertyExistenceConstraint(
                                relationships, propertyCursor, schema, tokenNameLookup, isDependent));
    }

    private void enforceRelationshipPropertyTypeConstraint(TypeConstraintDescriptor descriptor) throws KernelException {
        enforceRelationshipPropertyConstraint(
                descriptor.schema().asRelationshipTypeSchemaDescriptor(),
                (relationships, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateRelationshipPropertyTypeConstraint(
                                relationships, propertyCursor, descriptor, tokenNameLookup),
                (relationships, propertyCursor, tokenNameLookup) ->
                        constraintSemantics.validateRelationshipPropertyTypeConstraint(
                                relationships, propertyCursor, descriptor, tokenNameLookup));
    }

    @Override
    public ConstraintDescriptor propertyTypeConstraintCreate(
            SchemaDescriptor schema, String name, PropertyTypeSet propertyType, boolean isDependent)
            throws KernelException {
        if (schema.getPropertyIds().length != 1) {
            throw new UnsupportedOperationException("Composite property type constraints are not supported.");
        }
        assertSupportedInVersion(
                KernelVersion.VERSION_TYPE_CONSTRAINTS_INTRODUCED, "Failed to create property type constraint.");

        ConstraintDescriptor constraint =
                lockAndValidatePropertyTypeConstraint(schema, name, propertyType, isDependent);

        TypeConstraintDescriptor descriptor = constraint.asPropertyTypeConstraint();

        if (descriptor.schema().isRelationshipTypeSchemaDescriptor()) {
            enforceRelationshipPropertyTypeConstraint(descriptor);
        } else {
            enforceNodePropertyTypeConstraint(descriptor);
        }

        ktx.txState().constraintDoAdd(constraint);
        return constraint;
    }

    private ConstraintDescriptor lockAndValidatePropertyExistenceConstraint(
            SchemaDescriptor descriptor, String name, boolean isDependent) throws KernelException {
        return lockAndValidateNonIndexPropertyConstraint(
                descriptor, schema -> ConstraintDescriptorFactory.existsForSchema(schema, isDependent), name);
    }

    private ConstraintDescriptor lockAndValidatePropertyTypeConstraint(
            SchemaDescriptor descriptor, String name, PropertyTypeSet propertyType, boolean isDependent)
            throws KernelException {

        TypeRepresentation.validate(propertyType);

        var isUnion = TypeRepresentation.isUnion(propertyType);
        var hasListType = TypeRepresentation.hasListTypes(propertyType);

        // Only the basic type constraint was introduced in KernelVersion.VERSION_TYPE_CONSTRAINTS_INTRODUCED.
        // For expanded support, we require the kernel version below:
        if (!typeConstraintEnabled && (isUnion || hasListType)) {
            assertSupportedInVersion(
                    KernelVersion.VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED,
                    "Failed to create property type constraint with %s.",
                    propertyType.userDescription());
        }

        return lockAndValidateNonIndexPropertyConstraint(
                descriptor, desc -> ConstraintDescriptorFactory.typeForSchema(desc, propertyType, isDependent), name);
    }

    private ConstraintDescriptor lockAndValidateNonIndexPropertyConstraint(
            SchemaDescriptor descriptor,
            Function<SchemaDescriptor, ConstraintDescriptor> constraintFunction,
            String name)
            throws KernelException {
        // Lock constraint schema.
        exclusiveSchemaLock(descriptor);
        ktx.assertOpen();

        try {
            // Verify data integrity.
            assertValidDescriptor(descriptor, SchemaKernelException.OperationContext.CONSTRAINT_CREATION);
            ConstraintDescriptor constraint =
                    constraintFunction.apply(descriptor).withName(name);
            if (!dependentConstraintsEnabled && constraint.graphTypeDependence() == GraphTypeDependence.DEPENDENT) {
                throw new UnsupportedOperationException("Graph dependent constraints are not enabled, setting: "
                        + GraphDatabaseInternalSettings.dependent_constraints_enabled.name());
            }
            constraint = ensureConstraintHasName(constraint);
            exclusiveSchemaNameLock(constraint.getName());
            assertNoBlockingSchemaRulesExists(constraint);
            return constraint;
        } catch (SchemaKernelException e) {
            exclusiveSchemaUnlock(descriptor);
            throw e;
        }
    }

    @Override
    public void constraintDrop(String name, boolean canDropDependent) throws SchemaKernelException {
        exclusiveSchemaNameLock(name);
        ConstraintDescriptor constraint = allStoreHolder.constraintGetForName(name);
        if (constraint == null) {
            throw new DropConstraintFailureException(name, new NoSuchConstraintException(name));
        }
        constraintDrop(constraint, canDropDependent);
    }

    @Override
    public void constraintDrop(ConstraintDescriptor constraint, boolean canDropDependent) throws SchemaKernelException {
        // Lock
        SchemaDescriptor schema = constraint.schema();
        exclusiveLock(schema.keyType(), schema.lockingKeys());
        exclusiveSchemaNameLock(constraint.getName());
        ktx.assertOpen();

        if (constraint.graphTypeDependence() == GraphTypeDependence.DEPENDENT && !canDropDependent) {
            throw new DropConstraintFailureException(
                    constraint, "Cannot drop a constraint with dependence: " + GraphTypeDependence.DEPENDENT);
        }

        // verify data integrity
        try {
            assertConstraintExists(constraint);
        } catch (NoSuchConstraintException e) {
            throw new DropConstraintFailureException(constraint, e);
        }

        // Drop it like it's hot
        TransactionState txState = ktx.txState();
        txState.constraintDoDrop(constraint);
        if (constraint.enforcesUniqueness()) {
            IndexDescriptor index = allStoreHolder.indexGetForName(constraint.getName());
            if (index != IndexDescriptor.NO_INDEX) {
                txState.indexDoDrop(index);
            }
        }
    }

    private void exclusiveLock(ResourceType resource, long[] resourceIds) {
        ktx.lockClient().acquireExclusive(ktx.lockTracer(), resource, resourceIds);
    }

    private void acquireExclusiveNodeLock(long node) {
        if (!ktx.hasTxStateWithChanges() || !ktx.txState().nodeIsAddedInThisBatch(node)) {
            ktx.lockClient().acquireExclusive(ktx.lockTracer(), ResourceType.NODE, node);
        }
    }

    private void acquireExclusiveRelationshipLock(long relationshipId) {
        if (!ktx.hasTxStateWithChanges() || !ktx.txState().relationshipIsAddedInThisBatch(relationshipId)) {
            ktx.lockClient().acquireExclusive(ktx.lockTracer(), ResourceType.RELATIONSHIP, relationshipId);
        }
    }

    private void sharedSchemaLock(ResourceType type, long tokenId) {
        ktx.lockClient().acquireShared(ktx.lockTracer(), type, tokenId);
    }

    private void sharedTokenSchemaLock(ResourceType rt) {
        // this guards label or relationship type token indexes from being dropped during write operations
        sharedSchemaLock(rt, SchemaDescriptorImplementation.TOKEN_INDEX_LOCKING_ID);
    }

    private void exclusiveSchemaLock(SchemaDescriptor schema) {
        ktx.lockClient().acquireExclusive(ktx.lockTracer(), schema.keyType(), schema.lockingKeys());
    }

    private void exclusiveSchemaUnlock(SchemaDescriptor schema) {
        ktx.lockClient().releaseExclusive(schema.keyType(), schema.lockingKeys());
    }

    private void exclusiveSchemaNameLock(String schemaName) {
        long lockingId = ResourceIds.schemaNameResourceId(schemaName);
        ktx.lockClient().acquireExclusive(ktx.lockTracer(), SCHEMA_NAME, lockingId);
    }

    private static boolean propertyHasChanged(Value lhs, Value rhs) {
        // It is not enough to check equality here since by our equality semantics `int == toFloat(int)` is `true`
        // so by only checking for equality users cannot change type of property without also "changing" the value.
        // Hence the extra type check here.
        return !lhs.isSameValueTypeAs(rhs) || !lhs.equals(rhs);
    }

    private void assertNodeExists(long sourceNode) throws EntityNotFoundException {
        if (!allStoreHolder.nodeExists(sourceNode)) {
            throw new EntityNotFoundException(
                    NODE, ktx.internalTransaction().elementIdMapper().nodeElementId(sourceNode));
        }
    }

    public Locks locks() {
        return allStoreHolder;
    }

    private void assertConstraintExists(ConstraintDescriptor constraint) throws NoSuchConstraintException {
        if (!allStoreHolder.constraintExists(constraint)) {
            throw new NoSuchConstraintException(constraint, token);
        }
    }

    private void assertValidDescriptor(SchemaDescriptor descriptor, SchemaKernelException.OperationContext context)
            throws RepeatedSchemaComponentException {
        long numUniqueProp =
                Arrays.stream(descriptor.getPropertyIds()).distinct().count();
        long numUniqueEntityTokens =
                Arrays.stream(descriptor.getEntityTokenIds()).distinct().count();

        if (numUniqueProp != descriptor.getPropertyIds().length) {
            throw new RepeatedPropertyInSchemaException(descriptor, context, token);
        }
        if (numUniqueEntityTokens != descriptor.getEntityTokenIds().length) {
            if (descriptor.entityType() == NODE) {
                throw new RepeatedLabelInSchemaException(descriptor, context, token);
            } else {
                throw new RepeatedRelationshipTypeInSchemaException(descriptor, context, token);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends IndexBackedConstraintDescriptor> T indexBackedConstraintCreate(
            T constraint,
            IndexPrototype prototype,
            ConstraintIndexCreator.PropertyExistenceEnforcer propertyExistenceEnforcer)
            throws KernelException {
        try {
            if (allStoreHolder.constraintExists(constraint)) {
                throw new AlreadyConstrainedException(constraint, CONSTRAINT_CREATION, token);
            }
            IndexType indexType = prototype.getIndexType();
            if (indexType != IndexType.RANGE) {
                throw new CreateConstraintFailureException(
                        constraint, "Cannot create backing constraint index with index type " + indexType + ".");
            }
            if (prototype.schema().isFulltextSchemaDescriptor()) {
                throw new CreateConstraintFailureException(
                        constraint,
                        "Cannot create backing constraint index using a full-text schema: "
                                + prototype.schema().userDescription(token));
            }
            if (prototype.schema().isAnyTokenSchemaDescriptor()) {
                throw new CreateConstraintFailureException(
                        constraint,
                        "Cannot create backing constraint index using an any token schema: "
                                + prototype.schema().userDescription(token));
            }
            if (!prototype.isUnique()) {
                throw new CreateConstraintFailureException(
                        constraint,
                        "Cannot create index backed constraint using an index prototype that is not unique: "
                                + prototype.userDescription(token));
            }

            IndexDescriptor index = constraintIndexCreator.createUniquenessConstraintIndex(
                    ktx, constraint, prototype, propertyExistenceEnforcer);
            if (!allStoreHolder.constraintExists(constraint)) {
                // This looks weird, but since we release the label lock while awaiting population of the index
                // backing this constraint there can be someone else getting ahead of us, creating this exact
                // constraint
                // before we do, so now getting out here under the lock we must check again and if it exists
                // we must at this point consider this an idempotent operation because we verified earlier
                // that it didn't exist and went on to create it.
                constraint = (T) constraint.withOwnedIndexId(index.getId());
                ktx.txState().constraintDoAdd(constraint, index);
            } else {
                Iterator<ConstraintDescriptor> constraintsWithSchema =
                        allStoreHolder.constraintsGetForSchema(constraint.schema());
                while (constraintsWithSchema.hasNext()) {
                    ConstraintDescriptor next = constraintsWithSchema.next();
                    if (next.isIndexBackedConstraint()
                            && next.asIndexBackedConstraint().indexType() == constraint.indexType()) {
                        constraint = (T) constraintsWithSchema;
                        break;
                    }
                }
            }
            return constraint;
        } catch (UniquePropertyValueValidationException | TransactionFailureException | AlreadyConstrainedException e) {
            throw new CreateConstraintFailureException(constraint, e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends ConstraintDescriptor> T ensureConstraintHasName(T constraint) throws KernelException {
        return constraint.getName() == null ? (T) constraint.withName(generateNameFrom(constraint)) : constraint;
    }

    private String generateNameFrom(SchemaDescriptorSupplier schemaDescriptorSupplier)
            throws LabelNotFoundKernelException, RelationshipTypeIdNotFoundKernelException,
                    PropertyKeyIdNotFoundKernelException {
        SchemaDescriptor schema = schemaDescriptorSupplier.schema();

        int[] entityTokenIds = schema.getEntityTokenIds();
        String[] entityTokenNames =
                switch (schema.entityType()) {
                    case NODE -> resolveTokenNames(token::nodeLabelName, entityTokenIds);
                    case RELATIONSHIP -> resolveTokenNames(token::relationshipTypeName, entityTokenIds);
                };
        int[] propertyIds = schema.getPropertyIds();
        String[] propertyNames = resolveTokenNames(token::propertyKeyName, propertyIds);

        return SchemaNameUtil.generateName(schemaDescriptorSupplier, entityTokenNames, propertyNames);
    }

    @Override
    public void upgradeKernel(KernelUpgrade kernelUpgrade) {
        ktx.txState().kernelDoUpgrade(kernelUpgrade);
    }

    @FunctionalInterface
    private interface NodeValidatorWithIndex {
        void validate(
                NodeLabelIndexCursor allNodes,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                TokenNameLookup tokenNameLookup)
                throws CreateConstraintFailureException;
    }

    @FunctionalInterface
    private interface NodeValidatorWithoutIndex {
        void validate(NodeCursor nodeCursor, PropertyCursor propertyCursor, TokenNameLookup tokenNameLookup)
                throws CreateConstraintFailureException;
    }

    @FunctionalInterface
    private interface RelValidatorWithIndex {
        void validate(
                RelationshipTypeIndexCursor relationships,
                PropertyCursor propertyCursor,
                TokenNameLookup tokenNameLookup)
                throws CreateConstraintFailureException;
    }

    @FunctionalInterface
    private interface RelValidatorWithoutIndex {
        void validate(
                RelationshipScanCursor relationshipCursor,
                PropertyCursor propertyCursor,
                TokenNameLookup tokenNameLookup)
                throws CreateConstraintFailureException;
    }
}
