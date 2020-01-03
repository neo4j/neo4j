/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedLabelInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedRelationshipTypeInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedSchemaComponentException;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.CONSTRAINT_CREATION;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.schemaTokenLockingIds;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.UNIQUE;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Collects all Kernel API operations and guards them from being used outside of transaction.
 *
 * Many methods assume cursors to be initialized before use in private methods, even if they're not passed in explicitly.
 * Keep that in mind: e.g. nodeCursor, propertyCursor and relationshipCursor
 */
public class Operations implements Write, ExplicitIndexWrite, SchemaWrite
{
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder allStoreHolder;
    private final KernelToken token;
    private final StorageReader statement;
    private final AutoIndexing autoIndexing;
    private final IndexTxStateUpdater updater;
    private final DefaultCursors cursors;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final ConstraintSemantics constraintSemantics;
    private final IndexingService indexingService;
    private final Config config;
    private DefaultNodeCursor nodeCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultRelationshipScanCursor relationshipCursor;

    public Operations( AllStoreHolder allStoreHolder, IndexTxStateUpdater updater, StorageReader statement, KernelTransactionImplementation ktx,
            KernelToken token, DefaultCursors cursors, AutoIndexing autoIndexing, ConstraintIndexCreator constraintIndexCreator,
            ConstraintSemantics constraintSemantics, IndexingService indexingService, Config config )
    {
        this.token = token;
        this.autoIndexing = autoIndexing;
        this.allStoreHolder = allStoreHolder;
        this.ktx = ktx;
        this.statement = statement;
        this.updater = updater;
        this.cursors = cursors;
        this.constraintIndexCreator = constraintIndexCreator;
        this.constraintSemantics = constraintSemantics;
        this.indexingService = indexingService;
        this.config = config;
    }

    public void initialize()
    {
        this.nodeCursor = cursors.allocateNodeCursor();
        this.propertyCursor = cursors.allocatePropertyCursor();
        this.relationshipCursor = cursors.allocateRelationshipScanCursor();
    }

    @Override
    public long nodeCreate()
    {
        ktx.assertOpen();
        long nodeId = statement.reserveNode();
        ktx.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public long nodeCreateWithLabels( int[] labels ) throws ConstraintValidationException
    {
        if ( labels == null || labels.length == 0 )
        {
            return nodeCreate();
        }

        // We don't need to check the node for existence, like we do in nodeAddLabel, because we just created it.
        // We also don't need to check if the node already has some of the labels, because we know it has none.
        // And we don't need to take the exclusive lock on the node, because it was created in this transaction and
        // isn't visible to anyone else yet.
        ktx.assertOpen();
        long[] lockingIds = SchemaDescriptor.schemaTokenLockingIds( labels );
        Arrays.sort( lockingIds ); // Sort to ensure labels are locked and assigned in order.
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), ResourceTypes.LABEL, lockingIds );
        long nodeId = statement.reserveNode();
        ktx.txState().nodeDoCreate( nodeId );
        nodeCursor.single( nodeId, allStoreHolder );
        nodeCursor.next();

        int prevLabel = NO_SUCH_LABEL;
        for ( long lockingId : lockingIds )
        {
            int label = (int) lockingId;
            if ( label != prevLabel ) // Filter out duplicates.
            {
                checkConstraintsAndAddLabelToNode( nodeId, label );
                prevLabel = label;
            }
        }
        return nodeId;
    }

    @Override
    public boolean nodeDelete( long node ) throws AutoIndexingKernelException
    {
        ktx.assertOpen();
        return nodeDelete( node, true );
    }

    @Override
    public int nodeDetachDelete( final long nodeId ) throws KernelException
    {
        final MutableInt count = new MutableInt();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking(
                relId ->
                {
                    ktx.assertOpen();
                    if ( relationshipDelete( relId, false ) )
                    {
                        count.increment();
                    }
                }, ktx.statementLocks().optimistic(), ktx.lockTracer() );

        locking.lockAllNodesAndConsumeRelationships( nodeId, ktx, ktx.ambientNodeCursor() );
        ktx.assertOpen();

        //we are already holding the lock
        nodeDelete( nodeId, false );
        return count.intValue();
    }

    @Override
    public long relationshipCreate( long sourceNode, int relationshipType, long targetNode )
            throws EntityNotFoundException
    {
        ktx.assertOpen();

        sharedSchemaLock( ResourceTypes.RELATIONSHIP_TYPE, relationshipType );
        lockRelationshipNodes( sourceNode, targetNode );

        assertNodeExists( sourceNode );
        assertNodeExists( targetNode );

        long id = statement.reserveRelationship();
        ktx.txState().relationshipDoCreate( id, relationshipType, sourceNode, targetNode );
        return id;
    }

    @Override
    public boolean relationshipDelete( long relationship ) throws AutoIndexingKernelException
    {
        ktx.assertOpen();
        return relationshipDelete( relationship, true );
    }

    @Override
    public boolean nodeAddLabel( long node, int nodeLabel )
            throws EntityNotFoundException, ConstraintValidationException
    {
        sharedSchemaLock( ResourceTypes.LABEL, nodeLabel );
        acquireExclusiveNodeLock( node );

        singleNode( node );

        if ( nodeCursor.hasLabel( nodeLabel ) )
        {
            //label already there, nothing to do
            return false;
        }

        checkConstraintsAndAddLabelToNode( node, nodeLabel );
        return true;
    }

    private void checkConstraintsAndAddLabelToNode( long node, int nodeLabel )
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException
    {
        // Load the property key id list for this node. We may need it for constraint validation if there are any related constraints,
        // but regardless we need it for tx state updating
        int[] existingPropertyKeyIds = loadSortedPropertyKeyList();

        //Check so that we are not breaking uniqueness constraints
        //We do this by checking if there is an existing node in the index that
        //with the same label and property combination.
        if ( existingPropertyKeyIds.length > 0 )
        {
            for ( IndexBackedConstraintDescriptor uniquenessConstraint : indexingService.getRelatedUniquenessConstraints( new long[]{nodeLabel},
                    existingPropertyKeyIds, NODE ) )
            {
                IndexQuery.ExactPredicate[] propertyValues = getAllPropertyValues( uniquenessConstraint.schema(),
                        StatementConstants.NO_SUCH_PROPERTY_KEY, Values.NO_VALUE );
                if ( propertyValues != null )
                {
                    validateNoExistingNodeWithExactValues( uniquenessConstraint, propertyValues, node );
                }
            }
        }

        //node is there and doesn't already have the label, let's add
        ktx.txState().nodeDoAddLabel( nodeLabel, node );
        updater.onLabelChange( nodeLabel, existingPropertyKeyIds, nodeCursor, propertyCursor, ADDED_LABEL );
    }

    private int[] loadSortedPropertyKeyList()
    {
        nodeCursor.properties( propertyCursor );
        if ( !propertyCursor.next() )
        {
            return EMPTY_INT_ARRAY;
        }

        int[] propertyKeyIds = new int[4]; // just some arbitrary starting point, it grows on demand
        int cursor = 0;
        do
        {
            if ( cursor == propertyKeyIds.length )
            {
                propertyKeyIds = Arrays.copyOf( propertyKeyIds, cursor * 2 );
            }
            propertyKeyIds[cursor++] = propertyCursor.propertyKey();
        }
        while ( propertyCursor.next() );
        if ( cursor != propertyKeyIds.length )
        {
            propertyKeyIds = Arrays.copyOf( propertyKeyIds, cursor );
        }
        Arrays.sort( propertyKeyIds );
        return propertyKeyIds;
    }

    private boolean nodeDelete( long node, boolean lock ) throws AutoIndexingKernelException
    {
        ktx.assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( ktx.txState().nodeIsAddedInThisTx( node ) )
            {
                autoIndexing.nodes().entityRemoved( this, node );
                ktx.txState().nodeDoDelete( node );
                return true;
            }
            if ( ktx.txState().nodeIsDeletedInThisTx( node ) )
            {
                // already deleted
                return false;
            }
        }

        if ( lock )
        {
            ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), ResourceTypes.NODE, node );
        }

        allStoreHolder.singleNode( node, nodeCursor );
        if ( nodeCursor.next() )
        {
            acquireSharedNodeLabelLocks();

            autoIndexing.nodes().entityRemoved( this, node );
            ktx.txState().nodeDoDelete( node );
            return true;
        }

        // tried to delete node that does not exist
        return false;
    }

    /**
     * Assuming that the nodeCursor have been initialized to the node that labels are retrieved from
     */
    private long[] acquireSharedNodeLabelLocks()
    {
        long[] labels = nodeCursor.labels().all();
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), ResourceTypes.LABEL, labels );
        return labels;
    }

    private boolean relationshipDelete( long relationship, boolean lock ) throws AutoIndexingKernelException
    {
        allStoreHolder.singleRelationship( relationship, relationshipCursor ); // tx-state aware

        if ( relationshipCursor.next() )
        {
            if ( lock )
            {
                lockRelationshipNodes( relationshipCursor.sourceNodeReference(),
                        relationshipCursor.targetNodeReference() );
                acquireExclusiveRelationshipLock( relationship );
            }
            if ( !allStoreHolder.relationshipExists( relationship ) )
            {
                return false;
            }

            ktx.assertOpen();

            autoIndexing.relationships().entityRemoved( this, relationship );

            TransactionState txState = ktx.txState();
            if ( txState.relationshipIsAddedInThisTx( relationship ) )
            {
                txState.relationshipDoDeleteAddedInThisTx( relationship );
            }
            else
            {
                txState.relationshipDoDelete( relationship, relationshipCursor.type(),
                        relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() );
            }
            return true;
        }

        // tried to delete relationship that does not exist
        return false;
    }

    private void singleNode( long node ) throws EntityNotFoundException
    {
        allStoreHolder.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( NODE, node );
        }
    }

    private void singleRelationship( long relationship ) throws EntityNotFoundException
    {
        allStoreHolder.singleRelationship( relationship, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationship );
        }
    }

    /**
     * Fetch the property values for all properties in schema for a given node. Return these as an exact predicate
     * array.
     */
    private IndexQuery.ExactPredicate[] getAllPropertyValues( SchemaDescriptor schema, int changedPropertyKeyId,
            Value changedValue )
    {
        int[] schemaPropertyIds = schema.getPropertyIds();
        IndexQuery.ExactPredicate[] values = new IndexQuery.ExactPredicate[schemaPropertyIds.length];

        int nMatched = 0;
        nodeCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            int nodePropertyId = propertyCursor.propertyKey();
            int k = ArrayUtils.indexOf( schemaPropertyIds, nodePropertyId );
            if ( k >= 0 )
            {
                if ( nodePropertyId != StatementConstants.NO_SUCH_PROPERTY_KEY )
                {
                    values[k] = IndexQuery.exact( nodePropertyId, propertyCursor.propertyValue() );
                }
                nMatched++;
            }
        }

        //This is true if we are adding a property
        if ( changedPropertyKeyId != NO_SUCH_PROPERTY_KEY )
        {
            int k = ArrayUtils.indexOf( schemaPropertyIds, changedPropertyKeyId );
            if ( k >= 0 )
            {
                values[k] = IndexQuery.exact( changedPropertyKeyId, changedValue );
                nMatched++;
            }
        }

        if ( nMatched < values.length )
        {
            return null;
        }
        return values;
    }

    /**
     * Check so that there is not an existing node with the exact match of label and property
     */
    private void validateNoExistingNodeWithExactValues( IndexBackedConstraintDescriptor constraint,
            IndexQuery.ExactPredicate[] propertyValues, long modifiedNode
    ) throws UniquePropertyValueValidationException, UnableToValidateConstraintException
    {
        IndexDescriptor schemaIndexDescriptor = constraint.ownedIndexDescriptor();
        IndexReference indexReference = allStoreHolder.indexGetCapability( schemaIndexDescriptor );
        try ( DefaultNodeValueIndexCursor valueCursor = cursors.allocateNodeValueIndexCursor();
              IndexReaders indexReaders = new IndexReaders( indexReference, allStoreHolder ) )
        {
            assertIndexOnline( schemaIndexDescriptor );
            int labelId = schemaIndexDescriptor.schema().keyId();

            //Take a big fat lock, and check for existing node in index
            ktx.statementLocks().optimistic().acquireExclusive(
                    ktx.lockTracer(), INDEX_ENTRY,
                    indexEntryResourceId( labelId, propertyValues )
            );

            allStoreHolder.nodeIndexSeekWithFreshIndexReader( valueCursor, indexReaders.createReader(), propertyValues );
            if ( valueCursor.next() && valueCursor.nodeReference() != modifiedNode )
            {
                throw new UniquePropertyValueValidationException( constraint, VALIDATION,
                        new IndexEntryConflictException( valueCursor.nodeReference(), NO_SUCH_NODE,
                                IndexQuery.asValueTuple( propertyValues ) ) );
            }
        }
        catch ( IndexNotFoundKernelException | IndexBrokenKernelException | IndexNotApplicableKernelException e )
        {
            throw new UnableToValidateConstraintException( constraint, e );
        }
    }

    private void assertIndexOnline( IndexDescriptor descriptor )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        if ( allStoreHolder.indexGetState( descriptor ) != InternalIndexState.ONLINE )
        {
            throw new IndexBrokenKernelException( allStoreHolder.indexGetFailure( descriptor ) );
        }
    }

    @Override
    public boolean nodeRemoveLabel( long node, int labelId ) throws EntityNotFoundException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();

        singleNode( node );

        if ( !nodeCursor.hasLabel( labelId ) )
        {
            //the label wasn't there, nothing to do
            return false;
        }

        sharedSchemaLock( ResourceTypes.LABEL, labelId );
        ktx.txState().nodeDoRemoveLabel( labelId, node );
        if ( indexingService.hasRelatedSchema( labelId, NODE ) )
        {
            updater.onLabelChange( labelId, loadSortedPropertyKeyList(), nodeCursor, propertyCursor, REMOVED_LABEL );
        }
        return true;
    }

    @Override
    public Value nodeSetProperty( long node, int propertyKey, Value value )
            throws EntityNotFoundException, ConstraintValidationException, AutoIndexingKernelException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();

        singleNode( node );
        long[] labels = acquireSharedNodeLabelLocks();
        Value existingValue = readNodeProperty( propertyKey );
        int[] existingPropertyKeyIds = null;
        boolean hasRelatedSchema = indexingService.hasRelatedSchema( labels, propertyKey, NODE );
        if ( hasRelatedSchema )
        {
            existingPropertyKeyIds = loadSortedPropertyKeyList();
        }

        if ( hasRelatedSchema && !existingValue.equals( value ) )
        {
            // The value changed and there may be relevant constraints to check so let's check those now.
            Collection<IndexBackedConstraintDescriptor> uniquenessConstraints = indexingService.getRelatedUniquenessConstraints( labels, propertyKey, NODE );
            NodeSchemaMatcher.onMatchingSchema( uniquenessConstraints.iterator(), propertyKey, existingPropertyKeyIds,
                    uniquenessConstraint ->
                    {
                        validateNoExistingNodeWithExactValues( uniquenessConstraint, getAllPropertyValues( uniquenessConstraint.schema(), propertyKey, value ),
                                node );
                    });
        }

        if ( existingValue == NO_VALUE )
        {
            //no existing value, we just add it
            autoIndexing.nodes().propertyAdded( this, node, propertyKey, value );
            ktx.txState().nodeDoAddProperty( node, propertyKey, value );
            if ( hasRelatedSchema )
            {
                updater.onPropertyAdd( nodeCursor, propertyCursor, labels, propertyKey, existingPropertyKeyIds, value );
            }
            return NO_VALUE;
        }
        else
        {
            // We need to auto-index even if not actually changing the value.
            autoIndexing.nodes().propertyChanged( this, node, propertyKey, existingValue, value );
            if ( propertyHasChanged( value, existingValue ) )
            {
                //the value has changed to a new value
                ktx.txState().nodeDoChangeProperty( node, propertyKey, value );
                if ( hasRelatedSchema )
                {
                    updater.onPropertyChange( nodeCursor, propertyCursor, labels, propertyKey, existingPropertyKeyIds, existingValue, value );
                }
            }
            return existingValue;
        }
    }

    @Override
    public Value nodeRemoveProperty( long node, int propertyKey )
            throws EntityNotFoundException, AutoIndexingKernelException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();
        singleNode( node );
        Value existingValue = readNodeProperty( propertyKey );

        if ( existingValue != NO_VALUE )
        {
            long[] labels = acquireSharedNodeLabelLocks();
            autoIndexing.nodes().propertyRemoved( this, node, propertyKey );
            ktx.txState().nodeDoRemoveProperty( node, propertyKey );
            if ( indexingService.hasRelatedSchema( labels, propertyKey, NODE ) )
            {
                updater.onPropertyRemove( nodeCursor, propertyCursor, labels, propertyKey, loadSortedPropertyKeyList(), existingValue );
            }
        }

        return existingValue;
    }

    @Override
    public Value relationshipSetProperty( long relationship, int propertyKey, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException
    {
        acquireExclusiveRelationshipLock( relationship );
        ktx.assertOpen();
        singleRelationship( relationship );
        Value existingValue = readRelationshipProperty( propertyKey );
        if ( existingValue == NO_VALUE )
        {
            autoIndexing.relationships().propertyAdded( this, relationship, propertyKey, value );
            ktx.txState().relationshipDoReplaceProperty( relationship, propertyKey, NO_VALUE, value );
            return NO_VALUE;
        }
        else
        {
            // We need to auto-index even if not actually changing the value.
            autoIndexing.relationships().propertyChanged( this, relationship, propertyKey, existingValue, value );
            if ( propertyHasChanged( existingValue, value ) )
            {

                ktx.txState().relationshipDoReplaceProperty( relationship, propertyKey, existingValue, value );
            }

            return existingValue;
        }
    }

    @Override
    public Value relationshipRemoveProperty( long relationship, int propertyKey )
            throws EntityNotFoundException, AutoIndexingKernelException
    {
        acquireExclusiveRelationshipLock( relationship );
        ktx.assertOpen();
        singleRelationship( relationship );
        Value existingValue = readRelationshipProperty( propertyKey );

        if ( existingValue != NO_VALUE )
        {
            autoIndexing.relationships().propertyRemoved( this, relationship, propertyKey );
            ktx.txState().relationshipDoRemoveProperty( relationship, propertyKey );
        }

        return existingValue;
    }

    @Override
    public Value graphSetProperty( int propertyKey, Value value )
    {
        ktx.statementLocks().optimistic()
                .acquireExclusive( ktx.lockTracer(), ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        ktx.assertOpen();

        Value existingValue = readGraphProperty( propertyKey );
        if ( !existingValue.equals( value ) )
        {
            ktx.txState().graphDoReplaceProperty( propertyKey, existingValue, value );
        }
        return existingValue;
    }

    @Override
    public Value graphRemoveProperty( int propertyKey )
    {
        ktx.statementLocks().optimistic()
                .acquireExclusive( ktx.lockTracer(), ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        ktx.assertOpen();

        Value existingValue = readGraphProperty( propertyKey );
        if ( existingValue != Values.NO_VALUE )
        {
            ktx.txState().graphDoRemoveProperty( propertyKey );
        }
        return existingValue;
    }

    @Override
    public void nodeAddToExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().nodeChanges( indexName ).addNode( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().nodeChanges( indexName ).remove( node );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().nodeChanges( indexName ).remove( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().nodeChanges( indexName ).remove( node, key );
    }

    @Override
    public void nodeExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().createIndex( IndexEntityType.Node, indexName, customConfig );
    }

    @Override
    public void nodeExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    @Override
    public void nodeExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ExplicitIndexTransactionState txState = allStoreHolder.explicitIndexTxState();
        txState.nodeChanges( indexName ).drop();
        txState.deleteIndex( IndexEntityType.Node, indexName );
    }

    @Override
    public String nodeExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return allStoreHolder.explicitIndexStore().setNodeIndexConfiguration( indexName, key, value );
    }

    @Override
    public String nodeExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return allStoreHolder.explicitIndexStore().removeNodeIndexConfiguration( indexName, key );
    }

    @Override
    public void relationshipAddToExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        ktx.assertOpen();
        allStoreHolder.singleRelationship( relationship, relationshipCursor );
        if ( relationshipCursor.next() )
        {
            allStoreHolder.explicitIndexTxState().relationshipChanges( indexName ).addRelationship( relationship, key, value,
                    relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() );
        }
        else
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationship );
        }
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship, key );

    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship );
    }

    @Override
    public void relationshipExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.explicitIndexTxState().createIndex( IndexEntityType.Relationship, indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ExplicitIndexTransactionState txState = allStoreHolder.explicitIndexTxState();
        txState.relationshipChanges( indexName ).drop();
        txState.deleteIndex( IndexEntityType.Relationship, indexName );
    }

    private Value readNodeProperty( int propertyKey )
    {
        nodeCursor.properties( propertyCursor );

        //Find out if the property had a value
        Value existingValue = NO_VALUE;
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKey )
            {
                existingValue = propertyCursor.propertyValue();
                break;
            }
        }
        return existingValue;
    }

    private Value readRelationshipProperty( int propertyKey )
    {
        relationshipCursor.properties( propertyCursor );

        //Find out if the property had a value
        Value existingValue = NO_VALUE;
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKey )
            {
                existingValue = propertyCursor.propertyValue();
                break;
            }
        }
        return existingValue;
    }

    private Value readGraphProperty( int propertyKey )
    {
        allStoreHolder.graphProperties( propertyCursor );

        //Find out if the property had a value
        Value existingValue = NO_VALUE;
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKey )
            {
                existingValue = propertyCursor.propertyValue();
                break;
            }
        }
        return existingValue;
    }

    public CursorFactory cursors()
    {
        return cursors;
    }

    public Procedures procedures()
    {
        return allStoreHolder;
    }

    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.close();
            nodeCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.close();
            propertyCursor = null;
        }
        if ( relationshipCursor != null )
        {
            relationshipCursor.close();
            relationshipCursor = null;
        }

        cursors.assertClosed();
        cursors.release();
    }

    public Token token()
    {
        return token;
    }

    public ExplicitIndexRead indexRead()
    {
        return allStoreHolder;
    }

    public SchemaRead schemaRead()
    {
        return allStoreHolder;
    }

    public Read dataRead()
    {
        return allStoreHolder;
    }

    public DefaultNodeCursor nodeCursor()
    {
        return nodeCursor;
    }

    public DefaultRelationshipScanCursor relationshipCursor()
    {
        return relationshipCursor;
    }

    public DefaultPropertyCursor propertyCursor()
    {
        return propertyCursor;
    }

    @Override
    public IndexReference indexCreate( SchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return indexCreate( descriptor, config.get( GraphDatabaseSettings.default_schema_provider ), Optional.empty() );
    }

    @Override
    public IndexReference indexCreate( SchemaDescriptor descriptor, Optional<String> indexName ) throws SchemaKernelException
    {
        return indexCreate( descriptor, config.get( GraphDatabaseSettings.default_schema_provider ), indexName );
    }

    @Override
    public IndexReference indexCreate( SchemaDescriptor descriptor, String provider, Optional<String> name ) throws SchemaKernelException
    {
        exclusiveSchemaLock( descriptor );
        ktx.assertOpen();
        assertValidDescriptor( descriptor, SchemaKernelException.OperationContext.INDEX_CREATION );
        assertIndexDoesNotExist( SchemaKernelException.OperationContext.INDEX_CREATION, descriptor, name );

        IndexProviderDescriptor providerDescriptor = indexingService.indexProviderByName( provider );
        IndexDescriptor index = IndexDescriptorFactory.forSchema( descriptor, name, providerDescriptor );
        index = indexingService.getBlessedDescriptorFromProvider( index );
        ktx.txState().indexDoAdd( index );
        return index;
    }

    // Note: this will be sneakily executed by an internal transaction, so no additional locking is required.
    public IndexDescriptor indexUniqueCreate( SchemaDescriptor schema, String provider ) throws SchemaKernelException
    {
        IndexProviderDescriptor providerDescriptor = indexingService.indexProviderByName( provider );
        IndexDescriptor index =
                IndexDescriptorFactory.uniqueForSchema( schema,
                        Optional.empty(),
                        providerDescriptor );
        index = indexingService.getBlessedDescriptorFromProvider( index );
        ktx.txState().indexDoAdd( index );
        return index;
    }

    @Override
    public void indexDrop( IndexReference indexReference ) throws SchemaKernelException
    {
        assertValidIndex( indexReference );
        IndexDescriptor index = (IndexDescriptor) indexReference;
        SchemaDescriptor schema = index.schema();

        exclusiveSchemaLock( schema );
        ktx.assertOpen();
        try
        {
            IndexDescriptor existingIndex = allStoreHolder.indexGetForSchema( schema );

            if ( existingIndex == null )
            {
                throw new NoSuchIndexException( schema );
            }

            if ( existingIndex.type() == UNIQUE )
            {
                if ( allStoreHolder.indexGetOwningUniquenessConstraintId( existingIndex ) != null )
                {
                    throw new IndexBelongsToConstraintException( schema );
                }
            }
        }
        catch ( IndexBelongsToConstraintException | NoSuchIndexException e )
        {
            throw new DropIndexFailureException( schema, e );
        }
        ktx.txState().indexDoDrop( index );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return uniquePropertyConstraintCreate( descriptor, config.get( GraphDatabaseSettings.default_schema_provider ) );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String provider ) throws SchemaKernelException
    {
        //Lock
        exclusiveSchemaLock( descriptor );
        ktx.assertOpen();
        UniquenessConstraintDescriptor constraint;

        try
        {
            //Check data integrity
            assertValidDescriptor( descriptor, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );
            constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
            assertConstraintDoesNotExist( constraint );
            // It is not allowed to create uniqueness constraints on indexed label/property pairs
            assertIndexDoesNotExist( SchemaKernelException.OperationContext.CONSTRAINT_CREATION, descriptor, Optional.empty() );
        }
        catch ( SchemaKernelException e )
        {
            exclusiveSchemaUnlock( descriptor ); // Try not to hold on to exclusive schema locks when we don't strictly need them.
            throw e;
        }

        // Create constraints
        indexBackedConstraintCreate( constraint, provider );
        return constraint;
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return nodeKeyConstraintCreate( descriptor, config.get( GraphDatabaseSettings.default_schema_provider ) );
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String provider ) throws SchemaKernelException
    {
        //Lock
        exclusiveSchemaLock( descriptor );
        ktx.assertOpen();
        NodeKeyConstraintDescriptor constraint;

        try
        {
            //Check data integrity
            assertValidDescriptor( descriptor, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );
            constraint = ConstraintDescriptorFactory.nodeKeyForSchema( descriptor );
            assertConstraintDoesNotExist( constraint );
            // It is not allowed to create node key constraints on indexed label/property pairs
            assertIndexDoesNotExist( SchemaKernelException.OperationContext.CONSTRAINT_CREATION, descriptor, Optional.empty() );
        }
        catch ( SchemaKernelException e )
        {
            exclusiveSchemaUnlock( descriptor );
            throw e;
        }

        //enforce constraints
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            allStoreHolder.nodeLabelScan( descriptor.getLabelId(), nodes );
            constraintSemantics.validateNodeKeyConstraint( nodes, nodeCursor, propertyCursor, descriptor );
        }

        //create constraint
        indexBackedConstraintCreate( constraint, provider );
        return constraint;
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor ) throws SchemaKernelException
    {
        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint( descriptor );

        //enforce constraints
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            allStoreHolder.nodeLabelScan( descriptor.getLabelId(), nodes );
            constraintSemantics.validateNodePropertyExistenceConstraint( nodes, nodeCursor, propertyCursor, descriptor );
        }

        //create constraint
        ktx.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor descriptor ) throws SchemaKernelException
    {
        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint( descriptor );

        //enforce constraints
        allStoreHolder.relationshipTypeScan( descriptor.getRelTypeId(), relationshipCursor );
        constraintSemantics.validateRelationshipPropertyExistenceConstraint( relationshipCursor, propertyCursor, descriptor );

        //Create
        ktx.txState().constraintDoAdd( constraint );
        return constraint;
    }

    private ConstraintDescriptor lockAndValidatePropertyExistenceConstraint( SchemaDescriptor descriptor ) throws SchemaKernelException
    {
        // Lock constraint schema.
        exclusiveSchemaLock( descriptor );
        ktx.assertOpen();

        try
        {
            // Verify data integrity.
            assertValidDescriptor( descriptor, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );
            ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor );
            assertConstraintDoesNotExist( constraint );
            return constraint;
        }
        catch ( SchemaKernelException e )
        {
            exclusiveSchemaUnlock( descriptor );
            throw e;
        }
    }

    @Override
    public String relationshipExplicitIndexSetConfiguration( String indexName, String key, String value ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return allStoreHolder.explicitIndexStore().setRelationshipIndexConfiguration( indexName, key, value );
    }

    @Override
    public String relationshipExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return allStoreHolder.explicitIndexStore().removeRelationshipIndexConfiguration( indexName, key );
    }

    @Override
    public void constraintDrop( ConstraintDescriptor descriptor ) throws SchemaKernelException
    {
        //Lock
        SchemaDescriptor schema = descriptor.schema();
        exclusiveOptimisticLock( schema.keyType(), schema.keyId() );
        ktx.assertOpen();

        //verify data integrity
        try
        {
            assertConstraintExists( descriptor );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( descriptor, e );
        }

        //Drop it like it's hot
        ktx.txState().constraintDoDrop( descriptor );
    }

    private void assertIndexDoesNotExist( SchemaKernelException.OperationContext context, SchemaDescriptor descriptor, Optional<String> name )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        IndexDescriptor existingIndex = allStoreHolder.indexGetForSchema( descriptor );
        if ( existingIndex == null && name.isPresent() )
        {
            IndexReference indexReference = allStoreHolder.indexGetForName( name.get() );
            if ( indexReference != IndexReference.NO_INDEX )
            {
                existingIndex = (IndexDescriptor) indexReference;
            }
        }
        if ( existingIndex != null )
        {
            // OK so we found a matching constraint index. We check whether or not it has an owner
            // because this may have been a left-over constraint index from a previously failed
            // constraint creation, due to crash or similar, hence the missing owner.
            if ( existingIndex.type() == UNIQUE )
            {
                if ( context != CONSTRAINT_CREATION || constraintIndexHasOwner( existingIndex ) )
                {
                    throw new AlreadyConstrainedException( ConstraintDescriptorFactory.uniqueForSchema( descriptor ),
                            context, new SilentTokenNameLookup( token ) );
                }
            }
            else
            {
                throw new AlreadyIndexedException( descriptor, context );
            }
        }
    }

    private void exclusiveOptimisticLock( ResourceType resource, long resourceId )
    {
        ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), resource, resourceId );
    }

    private void acquireExclusiveNodeLock( long node )
    {
        if ( !ktx.hasTxStateWithChanges() || !ktx.txState().nodeIsAddedInThisTx( node ) )
        {
            ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), ResourceTypes.NODE, node );
        }
    }

    private void acquireExclusiveRelationshipLock( long relationshipId )
    {
        if ( !ktx.hasTxStateWithChanges() || !ktx.txState().relationshipIsAddedInThisTx( relationshipId ) )
        {
            ktx.statementLocks().optimistic()
                    .acquireExclusive( ktx.lockTracer(), ResourceTypes.RELATIONSHIP, relationshipId );
        }
    }

    private void sharedSchemaLock( ResourceType type, int tokenId )
    {
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), type, tokenId );
    }

    private void exclusiveSchemaLock( SchemaDescriptor schema )
    {
        long[] lockingIds = schemaTokenLockingIds( schema );
        ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), schema.keyType(), lockingIds );
    }

    private void exclusiveSchemaUnlock( SchemaDescriptor schema )
    {
        long[] lockingIds = schemaTokenLockingIds( schema );
        ktx.statementLocks().optimistic().releaseExclusive( schema.keyType(), lockingIds );
    }

    private void lockRelationshipNodes( long startNodeId, long endNodeId )
    {
        // Order the locks to lower the risk of deadlocks with other threads creating/deleting rels concurrently
        acquireExclusiveNodeLock( min( startNodeId, endNodeId ) );
        if ( startNodeId != endNodeId )
        {
            acquireExclusiveNodeLock( max( startNodeId, endNodeId ) );
        }
    }

    private static boolean propertyHasChanged( Value lhs, Value rhs )
    {
        //It is not enough to check equality here since by our equality semantics `int == tofloat(int)` is `true`
        //so by only checking for equality users cannot change type of property without also "changing" the value.
        //Hence the extra type check here.
        return lhs.getClass() != rhs.getClass() || !lhs.equals( rhs );
    }

    private void assertNodeExists( long sourceNode ) throws EntityNotFoundException
    {
        if ( !allStoreHolder.nodeExists( sourceNode ) )
        {
            throw new EntityNotFoundException( NODE, sourceNode );
        }
    }

    private boolean constraintIndexHasOwner( IndexDescriptor descriptor )
    {
        return allStoreHolder.indexGetOwningUniquenessConstraintId( descriptor ) != null;
    }

    private void assertConstraintDoesNotExist( ConstraintDescriptor constraint )
            throws AlreadyConstrainedException
    {
        if ( allStoreHolder.constraintExists( constraint ) )
        {
            throw new AlreadyConstrainedException( constraint,
                    SchemaKernelException.OperationContext.CONSTRAINT_CREATION,
                    new SilentTokenNameLookup( token ) );
        }
    }

    public Locks locks()
    {
        return allStoreHolder;
    }

    private void assertConstraintExists( ConstraintDescriptor constraint )
            throws NoSuchConstraintException
    {
        if ( !allStoreHolder.constraintExists( constraint ) )
        {
            throw new NoSuchConstraintException( constraint );
        }
    }

    private static void assertValidDescriptor( SchemaDescriptor descriptor, SchemaKernelException.OperationContext context )
            throws RepeatedSchemaComponentException
    {
        long numUniqueProp = Arrays.stream( descriptor.getPropertyIds() ).distinct().count();
        long numUniqueEntityTokens = Arrays.stream( descriptor.getEntityTokenIds() ).distinct().count();

        if ( numUniqueProp != descriptor.getPropertyIds().length )
        {
            throw new RepeatedPropertyInSchemaException( descriptor, context );
        }
        if ( numUniqueEntityTokens != descriptor.getEntityTokenIds().length )
        {
            if ( descriptor.entityType() == NODE )
            {
                throw new RepeatedLabelInSchemaException( descriptor, context );
            }
            else
            {
                throw new RepeatedRelationshipTypeInSchemaException( descriptor, context );
            }
        }
    }

    private void indexBackedConstraintCreate( IndexBackedConstraintDescriptor constraint, String provider )
            throws CreateConstraintFailureException
    {
        SchemaDescriptor descriptor = constraint.schema();
        try
        {
            if ( ktx.hasTxStateWithChanges() &&
                    ktx.txState().indexDoUnRemove( constraint.ownedIndexDescriptor() ) ) // ..., DROP, *CREATE*
            { // creation is undoing a drop
                if ( !ktx.txState().constraintDoUnRemove( constraint ) ) // CREATE, ..., DROP, *CREATE*
                { // ... the drop we are undoing did itself undo a prior create...
                    ktx.txState().constraintDoAdd(
                            constraint, ktx.txState().indexCreatedForConstraint( constraint ) );
                }
            }
            else // *CREATE*
            { // create from scratch
                Iterator<ConstraintDescriptor> it = allStoreHolder.constraintsGetForSchema( descriptor );
                while ( it.hasNext() )
                {
                    if ( it.next().equals( constraint ) )
                    {
                        return;
                    }
                }
                long indexId = constraintIndexCreator.createUniquenessConstraintIndex( ktx, descriptor, provider );
                if ( !allStoreHolder.constraintExists( constraint ) )
                {
                    // This looks weird, but since we release the label lock while awaiting population of the index
                    // backing this constraint there can be someone else getting ahead of us, creating this exact
                    // constraint
                    // before we do, so now getting out here under the lock we must check again and if it exists
                    // we must at this point consider this an idempotent operation because we verified earlier
                    // that it didn't exist and went on to create it.
                    ktx.txState().constraintDoAdd( constraint, indexId );
                }
            }
        }
        catch ( UniquePropertyValueValidationException | TransactionFailureException | AlreadyConstrainedException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
    }

    private static void assertValidIndex( IndexReference index ) throws NoSuchIndexException
    {
        if ( index == IndexReference.NO_INDEX )
        {
            throw new NoSuchIndexException( index.schema() );
        }
    }
}
