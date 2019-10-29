/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.List;
import java.util.Optional;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnspecifiedKernelException;
import org.neo4j.function.ThrowingIntFunction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQuery;
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
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
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
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.ResourceIds;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.CONSTRAINT_CREATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.INDEX_CREATION;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.impl.locking.ResourceIds.indexEntryResourceId;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;
import static org.neo4j.lock.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.lock.ResourceTypes.SCHEMA_NAME;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Collects all Kernel API operations and guards them from being used outside of transaction.
 *
 * Many methods assume cursors to be initialized before use in private methods, even if they're not passed in explicitly.
 * Keep that in mind: e.g. nodeCursor, propertyCursor and relationshipCursor
 */
public class Operations implements Write, SchemaWrite
{
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder allStoreHolder;
    private final StorageReader storageReader;
    private final CommandCreationContext commandCreationContext;
    private final KernelToken token;
    private final TokenNameLookup tokenNameLookup;
    private final IndexTxStateUpdater updater;
    private final DefaultPooledCursors cursors;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final ConstraintSemantics constraintSemantics;
    private final IndexingProvidersService indexProviders;
    private final Config config;
    private DefaultNodeCursor nodeCursor;
    private DefaultNodeCursor restrictedNodeCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultPropertyCursor restrictedPropertyCursor;
    private DefaultRelationshipScanCursor relationshipCursor;

    public Operations( AllStoreHolder allStoreHolder, StorageReader storageReader, IndexTxStateUpdater updater,
            CommandCreationContext commandCreationContext, KernelTransactionImplementation ktx,
            KernelToken token, DefaultPooledCursors cursors, ConstraintIndexCreator constraintIndexCreator,
            ConstraintSemantics constraintSemantics, IndexingProvidersService indexProviders, Config config )
    {
        this.storageReader = storageReader;
        this.commandCreationContext = commandCreationContext;
        this.token = token;
        this.tokenNameLookup = new SilentTokenNameLookup( token );
        this.allStoreHolder = allStoreHolder;
        this.ktx = ktx;
        this.updater = updater;
        this.cursors = cursors;
        this.constraintIndexCreator = constraintIndexCreator;
        this.constraintSemantics = constraintSemantics;
        this.indexProviders = indexProviders;
        this.config = config;
    }

    public void initialize()
    {
        this.nodeCursor = cursors.allocateFullAccessNodeCursor();
        this.propertyCursor = cursors.allocateFullAccessPropertyCursor();
        this.relationshipCursor = cursors.allocateRelationshipScanCursor();
        this.restrictedNodeCursor = cursors.allocateNodeCursor();
        this.restrictedPropertyCursor = cursors.allocatePropertyCursor();
    }

    @Override
    public long nodeCreate()
    {
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        long nodeId = commandCreationContext.reserveNode();
        txState.nodeDoCreate( nodeId );
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
        int labelCount = labels.length;
        long[] lockingIds = new long[labelCount];
        for ( int i = 0; i < labelCount; i++ )
        {
            lockingIds[i] = labels[i];
        }
        Arrays.sort( lockingIds ); // Sort to ensure labels are locked and assigned in order.
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), ResourceTypes.LABEL, lockingIds );
        TransactionState txState = ktx.txState();
        long nodeId = commandCreationContext.reserveNode();
        txState.nodeDoCreate( nodeId );
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
    public boolean nodeDelete( long node )
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

        TransactionState txState = ktx.txState();
        long id = commandCreationContext.reserveRelationship();
        txState.relationshipDoCreate( id, relationshipType, sourceNode, targetNode );
        return id;
    }

    @Override
    public boolean relationshipDelete( long relationship )
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
            for ( IndexBackedConstraintDescriptor uniquenessConstraint : storageReader.uniquenessConstraintsGetRelated( new long[]{nodeLabel},
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

    private boolean nodeDelete( long node, boolean lock )
    {
        ktx.assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            TransactionState state = ktx.txState();
            if ( state.nodeIsAddedInThisTx( node ) )
            {
                state.nodeDoDelete( node );
                return true;
            }
            if ( state.nodeIsDeletedInThisTx( node ) )
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

    private boolean relationshipDelete( long relationship, boolean lock )
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
     * array. This is run with no security check.
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
            IndexQuery.ExactPredicate[] propertyValues, long modifiedNode )
            throws UniquePropertyValueValidationException, UnableToValidateConstraintException
    {
        IndexDescriptor index = allStoreHolder.indexGetForName( constraint.getName() );
        try ( DefaultNodeValueIndexCursor valueCursor = cursors.allocateNodeValueIndexCursor();
              IndexReaders indexReaders = new IndexReaders( index, allStoreHolder ) )
        {
            assertIndexOnline( index );
            SchemaDescriptor schema = index.schema();
            long[] labelIds = schema.lockingKeys();
            if ( labelIds.length != 1 )
            {
                throw new UnableToValidateConstraintException( constraint, new AssertionError( "Constraint indexes are not expected to be multi-token " +
                        "indexes, but the constraint " + constraint.prettyPrint( tokenNameLookup ) + " was referencing an index with the following schema: " +
                        schema.userDescription( tokenNameLookup ) + "." ) );
            }

            //Take a big fat lock, and check for existing node in index
            ktx.statementLocks().optimistic().acquireExclusive(
                    ktx.lockTracer(), INDEX_ENTRY,
                    indexEntryResourceId( labelIds[0], propertyValues )
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
        if ( storageReader.hasRelatedSchema( labelId, NODE ) )
        {
            updater.onLabelChange( labelId, loadSortedPropertyKeyList(), nodeCursor, propertyCursor, REMOVED_LABEL );
        }
        return true;
    }

    @Override
    public Value nodeSetProperty( long node, int propertyKey, Value value )
            throws EntityNotFoundException, ConstraintValidationException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();

        singleNode( node );
        long[] labels = acquireSharedNodeLabelLocks();
        Value existingValue = readNodeProperty( propertyKey );
        int[] existingPropertyKeyIds = null;
        boolean hasRelatedSchema = storageReader.hasRelatedSchema( labels, propertyKey, NODE );
        if ( hasRelatedSchema )
        {
            existingPropertyKeyIds = loadSortedPropertyKeyList();
        }

        if ( !existingValue.equals( value ) )
        {
            // The value changed and there may be relevant constraints to check so let's check those now.
            Collection<IndexBackedConstraintDescriptor> uniquenessConstraints = storageReader.uniquenessConstraintsGetRelated( labels, propertyKey, NODE );
            NodeSchemaMatcher.onMatchingSchema( uniquenessConstraints.iterator(), propertyKey, existingPropertyKeyIds, constraint ->
                    validateNoExistingNodeWithExactValues( constraint, getAllPropertyValues( constraint.schema(), propertyKey, value ), node ) );
        }

        if ( existingValue == NO_VALUE )
        {
            //no existing value, we just add it
            ktx.txState().nodeDoAddProperty( node, propertyKey, value );
            if ( hasRelatedSchema )
            {
                updater.onPropertyAdd( nodeCursor, propertyCursor, labels, propertyKey, existingPropertyKeyIds, value );
            }
            return NO_VALUE;
        }
        else
        {
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
            throws EntityNotFoundException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();
        singleNode( node );
        Value existingValue = readNodeProperty( propertyKey );

        if ( existingValue != NO_VALUE )
        {
            long[] labels = acquireSharedNodeLabelLocks();
            ktx.txState().nodeDoRemoveProperty( node, propertyKey );
            if ( storageReader.hasRelatedSchema( labels, propertyKey, NODE ) )
            {
                updater.onPropertyRemove( nodeCursor, propertyCursor, labels, propertyKey, loadSortedPropertyKeyList(), existingValue );
            }
        }

        return existingValue;
    }

    @Override
    public Value relationshipSetProperty( long relationship, int propertyKey, Value value )
            throws EntityNotFoundException
    {
        acquireExclusiveRelationshipLock( relationship );
        ktx.assertOpen();
        singleRelationship( relationship );
        Value existingValue = readRelationshipProperty( propertyKey );
        if ( existingValue == NO_VALUE )
        {
            ktx.txState().relationshipDoReplaceProperty( relationship, propertyKey, NO_VALUE, value );
            return NO_VALUE;
        }
        else
        {
            if ( propertyHasChanged( existingValue, value ) )
            {
                ktx.txState().relationshipDoReplaceProperty( relationship, propertyKey, existingValue, value );
            }

            return existingValue;
        }
    }

    @Override
    public Value relationshipRemoveProperty( long relationship, int propertyKey ) throws EntityNotFoundException
    {
        acquireExclusiveRelationshipLock( relationship );
        ktx.assertOpen();
        singleRelationship( relationship );
        Value existingValue = readRelationshipProperty( propertyKey );

        if ( existingValue != NO_VALUE )
        {
            ktx.txState().relationshipDoRemoveProperty( relationship, propertyKey );
        }

        return existingValue;
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
        if ( restrictedNodeCursor != null )
        {
            restrictedNodeCursor.close();
            restrictedNodeCursor = null;
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
        if ( restrictedPropertyCursor != null )
        {
            restrictedPropertyCursor.close();
            restrictedPropertyCursor = null;
        }

        cursors.assertClosed();
        cursors.release();
    }

    public Token token()
    {
        return token;
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
        return restrictedNodeCursor;
    }

    public DefaultRelationshipScanCursor relationshipCursor()
    {
        return relationshipCursor;
    }

    public DefaultPropertyCursor propertyCursor()
    {
        return restrictedPropertyCursor;
    }

    @Override
    public IndexProviderDescriptor indexProviderByName( String providerName )
    {
        ktx.assertOpen();
        return indexProviders.indexProviderByName( providerName );
    }

    @Override
    public IndexDescriptor indexCreate( IndexPrototype prototype ) throws KernelException
    {
        exclusiveSchemaLock( prototype.schema() );
        ktx.assertOpen();
        assertValidDescriptor( prototype.schema(), INDEX_CREATION );
        prototype = ensureIndexPrototypeHasName( prototype );
        prototype = ensureIndexPrototypeHasIndexProvider( prototype );
        Optional<String> nameOptional = prototype.getName();
        assert nameOptional.isPresent();
        String name = nameOptional.get();
        exclusiveSchemaNameLock( name );
        assertNoBlockingSchemaRulesExists( prototype );

        return indexDoCreate( prototype );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor schema, String indexName ) throws KernelException
    {
        return indexCreate( schema, config.get( GraphDatabaseSettings.default_schema_provider ), indexName );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor schema, String provider, String name ) throws KernelException
    {
        IndexProviderDescriptor providerDescriptor = indexProviders.indexProviderByName( provider );
        IndexPrototype prototype = IndexPrototype.forSchema( schema, providerDescriptor ).withName( name );
        return indexCreate( prototype );
    }

    // Note: this will be sneakily executed by an internal transaction, so no additional locking is required.
    public IndexDescriptor indexUniqueCreate( IndexPrototype prototype )
    {
        return indexDoCreate( prototype );
    }

    private IndexDescriptor indexDoCreate( IndexPrototype prototype )
    {
        indexProviders.validateIndexPrototype( prototype );
        TransactionState transactionState = ktx.txState();
        long schemaRecordId = commandCreationContext.reserveSchema();
        IndexDescriptor index = prototype.materialise( schemaRecordId );
        index = indexProviders.completeConfiguration( index );
        transactionState.indexDoAdd( index );
        return index;
    }

    private IndexPrototype ensureIndexPrototypeHasName( IndexPrototype prototype ) throws KernelException
    {
        if ( prototype.getName().isEmpty() )
        {
            SchemaDescriptor schema = prototype.schema();

            int[] entityTokenIds = schema.getEntityTokenIds();
            String[] entityTokenNames;
            switch ( schema.entityType() )
            {
            case NODE:
                entityTokenNames = resolveTokenNames( token::nodeLabelName, entityTokenIds );
                break;
            case RELATIONSHIP:
                entityTokenNames = resolveTokenNames( token::relationshipTypeName, entityTokenIds );
                break;
            default:
                throw new UnspecifiedKernelException( Status.General.UnknownError, "Cannot create index for entity type %s in the schema %s.",
                        schema.entityType(), schema );
            }
            int[] propertyIds = schema.getPropertyIds();
            String[] propertyNames = resolveTokenNames( token::propertyKeyName, propertyIds );

            prototype = prototype.withName( SchemaRule.generateName( prototype, entityTokenNames, propertyNames ) );
        }
        return prototype;
    }

    private <E extends Exception> String[] resolveTokenNames( ThrowingIntFunction<String, E> resolver, int[] tokenIds ) throws E
    {
        String[] names = new String[tokenIds.length];
        for ( int i = 0; i < tokenIds.length; i++ )
        {
            names[i] = resolver.apply( tokenIds[i] );
        }
        return names;
    }

    private IndexPrototype ensureIndexPrototypeHasIndexProvider( IndexPrototype prototype )
    {
        if ( prototype.getIndexProvider() == IndexProviderDescriptor.UNDECIDED )
        {
            IndexProviderDescriptor provider;
            if ( prototype.getIndexType() == IndexType.FULLTEXT )
            {
                provider = indexProviders.getFulltextProvider();
            }
            else
            {
                provider = indexProviders.getDefaultProvider();
            }
            prototype = prototype.withIndexProvider( provider );
        }
        return prototype;
    }

    @Override
    public void indexDrop( IndexDescriptor index ) throws SchemaKernelException
    {
        if ( index == IndexDescriptor.NO_INDEX )
        {
            throw new DropIndexFailureException( "No index was specified." );
        }
        exclusiveSchemaLock( index.schema() );
        exclusiveSchemaNameLock( index.getName() );
        assertIndexExistsForDrop( index );
        if ( index.isUnique() )
        {
            if ( allStoreHolder.indexGetOwningUniquenessConstraintId( index ) != null )
            {
                IndexBelongsToConstraintException cause = new IndexBelongsToConstraintException( index.schema() );
                throw new DropIndexFailureException( "Unable to drop index: " + cause.getUserMessage( tokenNameLookup ), cause );
            }
        }
        ktx.txState().indexDoDrop( index );
    }

    private void assertIndexExistsForDrop( IndexDescriptor index ) throws DropIndexFailureException
    {
        try
        {
            allStoreHolder.assertIndexExists( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new DropIndexFailureException( "Unable to drop index: " + e.getUserMessage( tokenNameLookup ), e );
        }
    }

    @Override
    public void indexDrop( SchemaDescriptor schema ) throws SchemaKernelException
    {
        exclusiveSchemaLock( schema );
        Iterator<IndexDescriptor> iterator = Iterators.filter(
                index -> index.getIndexType() == IndexType.BTREE,
                allStoreHolder.index( schema ) );

        if ( !iterator.hasNext() )
        {
            String description = schema.userDescription( tokenNameLookup );
            throw new DropIndexFailureException( "Unable to drop index on " + description + ". There is no such index." );
        }

        do
        {
            IndexDescriptor existingIndex = iterator.next();
            indexDrop( existingIndex );
        }
        while ( iterator.hasNext() );
    }

    @Override
    public void indexDrop( String indexName ) throws SchemaKernelException
    {
        exclusiveSchemaNameLock( indexName );
        IndexDescriptor index = allStoreHolder.indexGetForName( indexName );
        if ( index == IndexDescriptor.NO_INDEX )
        {
            throw new DropIndexFailureException( "Unable to drop index called `" + indexName + "`. There is no such index." );
        }
        exclusiveSchemaLock( index.schema() );
        assertIndexExistsForDrop( index );
        if ( index.isUnique() )
        {
            if ( allStoreHolder.indexGetOwningUniquenessConstraintId( index ) != null )
            {
                IndexBelongsToConstraintException cause = new IndexBelongsToConstraintException( indexName, index.schema() );
                throw new DropIndexFailureException( "Unable to drop index: " + cause.getUserMessage( tokenNameLookup ), cause );
            }
        }
        ktx.txState().indexDoDrop( index );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( IndexPrototype prototype ) throws KernelException
    {
        SchemaDescriptor schema = prototype.schema();
        exclusiveSchemaLock( schema );
        ktx.assertOpen();
        prototype = ensureIndexPrototypeHasIndexProvider( prototype );

        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( schema );
        try
        {
            assertValidDescriptor( schema, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );
            if ( prototype.getName().isEmpty() )
            {
                constraint = ensureConstraintHasName( constraint );
                prototype = prototype.withName( constraint.getName() );
            }
            else
            {
                constraint = constraint.withName( prototype.getName().get() );
            }
            exclusiveSchemaNameLock( constraint.getName() );
            assertNoBlockingSchemaRulesExists( constraint );
        }
        catch ( KernelException e )
        {
            exclusiveSchemaUnlock( schema ); // Try not to hold on to exclusive schema locks when we don't strictly need them.
            throw e;
        }

        // Create constraints
        constraint = indexBackedConstraintCreate( constraint, prototype );
        return constraint;
    }

    private void assertNoBlockingSchemaRulesExists( IndexPrototype prototype )
            throws EquivalentSchemaRuleAlreadyExistsException, IndexWithNameAlreadyExistsException, ConstraintWithNameAlreadyExistsException,
            AlreadyIndexedException, AlreadyConstrainedException
    {
        Optional<String> prototypeName = prototype.getName();
        if ( prototypeName.isEmpty() )
        {
            throw new IllegalStateException( "Expected index to always have a name by this point" );
        }
        String name = prototypeName.get();

        // Equivalent index
        IndexDescriptor indexWithSameSchema = IndexDescriptor.NO_INDEX;
        Iterator<IndexDescriptor> indexesWithSameSchema = allStoreHolder.index( prototype.schema() );
        while ( indexesWithSameSchema.hasNext() )
        {
            indexWithSameSchema = indexesWithSameSchema.next();
            if ( indexWithSameSchema.getName().equals( name ) && indexWithSameSchema.isUnique() == prototype.isUnique() )
            {
                throw new EquivalentSchemaRuleAlreadyExistsException( indexWithSameSchema, INDEX_CREATION, tokenNameLookup );
            }
        }

        // Name conflict with other schema rule
        assertSchemaRuleWithNameDoesNotExist( name );

        // Already constrained
        final Iterator<ConstraintDescriptor> constraintWithSameSchema = allStoreHolder.constraintsGetForSchema( prototype.schema() );
        while ( constraintWithSameSchema.hasNext() )
        {
            final ConstraintDescriptor constraint = constraintWithSameSchema.next();
            if ( constraint.type() != ConstraintType.EXISTS )
            {
                throw new AlreadyConstrainedException( constraint, INDEX_CREATION, tokenNameLookup );
            }
        }

        // Already indexed
        if ( indexWithSameSchema != IndexDescriptor.NO_INDEX )
        {
            throw new AlreadyIndexedException( prototype.schema(), INDEX_CREATION );
        }
    }

    private void assertNoBlockingSchemaRulesExists( ConstraintDescriptor constraint )
            throws EquivalentSchemaRuleAlreadyExistsException, IndexWithNameAlreadyExistsException, ConstraintWithNameAlreadyExistsException,
            AlreadyConstrainedException, AlreadyIndexedException
    {
        final String name = constraint.getName();
        if ( name == null )
        {
            throw new IllegalStateException( "Expected constraint to always have a name by this point" );
        }

        // Equivalent constraint
        final List<ConstraintDescriptor> constraintsWithSameSchema = Iterators.asList( allStoreHolder.constraintsGetForSchema( constraint.schema() ) );
        for ( ConstraintDescriptor constraintWithSameSchema : constraintsWithSameSchema )
        {
            if ( constraint.equals( constraintWithSameSchema ) &&
                 constraint.getName().equals( constraintWithSameSchema.getName() ) )
            {
                throw new EquivalentSchemaRuleAlreadyExistsException( constraintWithSameSchema, CONSTRAINT_CREATION, tokenNameLookup );
            }
        }

        // Name conflict with other schema rule
        assertSchemaRuleWithNameDoesNotExist( name );

        // Already constrained
        for ( ConstraintDescriptor constraintWithSameSchema : constraintsWithSameSchema )
        {
            final boolean creatingExistenceConstraint = constraint.type() == ConstraintType.EXISTS;
            final boolean existingIsExistenceConstraint = constraintWithSameSchema.type() == ConstraintType.EXISTS;
            if ( creatingExistenceConstraint == existingIsExistenceConstraint )
            {
                throw new AlreadyConstrainedException( constraintWithSameSchema, CONSTRAINT_CREATION, tokenNameLookup );
            }
        }
        // Already indexed
        if ( constraint.type() != ConstraintType.EXISTS )
        {
            Iterator<IndexDescriptor> existingIndexes = allStoreHolder.index( constraint.schema() );
            if ( existingIndexes.hasNext() )
            {
                IndexDescriptor existingIndex = existingIndexes.next();
                throw new AlreadyIndexedException( existingIndex.schema(), CONSTRAINT_CREATION );
            }
        }
    }

    private void assertSchemaRuleWithNameDoesNotExist( String name ) throws IndexWithNameAlreadyExistsException, ConstraintWithNameAlreadyExistsException
    {
        // Check constraints first because some of them will also be backed by indexes
        final ConstraintDescriptor constraintWithSameName = allStoreHolder.constraintGetForName( name );
        if ( constraintWithSameName != null )
        {
            throw new ConstraintWithNameAlreadyExistsException( name );
        }
        final IndexDescriptor indexWithSameName = allStoreHolder.indexGetForName( name );
        if ( indexWithSameName != IndexDescriptor.NO_INDEX )
        {
            throw new IndexWithNameAlreadyExistsException( name );
        }
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( IndexPrototype prototype ) throws KernelException
    {
        SchemaDescriptor schema = prototype.schema();
        exclusiveSchemaLock( schema );
        ktx.assertOpen();
        prototype = ensureIndexPrototypeHasIndexProvider( prototype );
        NodeKeyConstraintDescriptor constraint = ConstraintDescriptorFactory.nodeKeyForSchema( schema );

        try
        {
            //Check data integrity
            assertValidDescriptor( schema, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );

            if ( prototype.getName().isEmpty() )
            {
                constraint = ensureConstraintHasName( constraint );
                prototype = prototype.withName( constraint.getName() );
            }
            else
            {
                constraint = constraint.withName( prototype.getName().get() );
            }

            exclusiveSchemaNameLock( constraint.getName() );
            assertNoBlockingSchemaRulesExists( constraint );
        }
        catch ( SchemaKernelException e )
        {
            exclusiveSchemaUnlock( schema );
            throw e;
        }

        //enforce constraints
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            allStoreHolder.nodeLabelScan( schema.getLabelId(), nodes );
            constraintSemantics.validateNodeKeyConstraint( nodes, nodeCursor, propertyCursor, schema.asLabelSchemaDescriptor() );
        }

        //create constraint
        indexBackedConstraintCreate( constraint, prototype );
        return constraint;
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor schema, String name ) throws KernelException
    {
        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint( schema, name );

        //enforce constraints
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            allStoreHolder.nodeLabelScan( schema.getLabelId(), nodes );
            constraintSemantics.validateNodePropertyExistenceConstraint( nodes, nodeCursor, propertyCursor, schema );
        }

        //create constraint
        ktx.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor schema, String name ) throws KernelException
    {
        ConstraintDescriptor constraint = lockAndValidatePropertyExistenceConstraint( schema, name );

        //enforce constraints
        allStoreHolder.relationshipTypeScan( schema.getRelTypeId(), relationshipCursor );
        constraintSemantics.validateRelationshipPropertyExistenceConstraint( relationshipCursor, propertyCursor, schema );

        //Create
        ktx.txState().constraintDoAdd( constraint );
        return constraint;
    }

    private ConstraintDescriptor lockAndValidatePropertyExistenceConstraint( SchemaDescriptor descriptor, String name ) throws KernelException
    {
        // Lock constraint schema.
        exclusiveSchemaLock( descriptor );
        ktx.assertOpen();

        try
        {
            // Verify data integrity.
            assertValidDescriptor( descriptor, SchemaKernelException.OperationContext.CONSTRAINT_CREATION );
            ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor ).withName( name );
            constraint = ensureConstraintHasName( constraint );
            exclusiveSchemaNameLock( constraint.getName() );
            assertNoBlockingSchemaRulesExists( constraint );
            return constraint;
        }
        catch ( SchemaKernelException e )
        {
            exclusiveSchemaUnlock( descriptor );
            throw e;
        }
    }

    @Override
    public void constraintDrop( SchemaDescriptor schema, ConstraintType type ) throws SchemaKernelException
    {
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetForSchema( schema );
        constraints = Iterators.filter( constraint -> constraint.type() == type, constraints );
        if ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            if ( !constraints.hasNext() )
            {
                constraintDrop( constraint );
            }
            else
            {
                String schemaDescription = schema.userDescription( tokenNameLookup );
                String constraintDescription = constraints.next().userDescription( tokenNameLookup );
                throw new DropConstraintFailureException( constraint, new IllegalArgumentException(
                        "More than one " + type + " constraint was found with the '" + schemaDescription + "' schema: " + constraintDescription +
                                ", please drop constraint by name instead." ) );
            }
        }
        else
        {
            throw new DropConstraintFailureException( schema, new NoSuchConstraintException( schema, tokenNameLookup ) );
        }
    }

    @Override
    public void constraintDrop( String name ) throws SchemaKernelException
    {
        exclusiveSchemaNameLock( name );
        ConstraintDescriptor constraint = allStoreHolder.constraintGetForName( name );
        if ( constraint == null )
        {
            throw new DropConstraintFailureException( name, new NoSuchConstraintException( name ) );
        }
        constraintDrop( constraint );
    }

    @Override
    public void constraintDrop( ConstraintDescriptor constraint ) throws SchemaKernelException
    {
        //Lock
        SchemaDescriptor schema = constraint.schema();
        exclusiveOptimisticLock( schema.keyType(), schema.lockingKeys() );
        exclusiveSchemaNameLock( constraint.getName() );
        ktx.assertOpen();

        //verify data integrity
        try
        {
            assertConstraintExists( constraint );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( constraint, e );
        }

        //Drop it like it's hot
        TransactionState txState = ktx.txState();
        txState.constraintDoDrop( constraint );
        if ( constraint.enforcesUniqueness() )
        {
            IndexDescriptor index = allStoreHolder.indexGetForName( constraint.getName() );
            if ( index != IndexDescriptor.NO_INDEX )
            {
                txState.indexDoDrop( index );
            }
        }
    }

    private void exclusiveOptimisticLock( ResourceType resource, long[] resourceIds )
    {
        ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), resource, resourceIds );
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
        long[] lockingIds = schema.lockingKeys();
        ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), schema.keyType(), lockingIds );
    }

    private void exclusiveSchemaUnlock( SchemaDescriptor schema )
    {
        long[] lockingIds = schema.lockingKeys();
        ktx.statementLocks().optimistic().releaseExclusive( schema.keyType(), lockingIds );
    }

    private void exclusiveSchemaNameLock( String schemaName )
    {
        long lockingId = ResourceIds.schemaNameResourceId( schemaName );
        ktx.statementLocks().optimistic().acquireExclusive( ktx.lockTracer(), SCHEMA_NAME, lockingId );
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
        //It is not enough to check equality here since by our equality semantics `int == toFloat(int)` is `true`
        //so by only checking for equality users cannot change type of property without also "changing" the value.
        //Hence the extra type check here.
        return !lhs.isSameValueTypeAs( rhs ) || !lhs.equals( rhs );
    }

    private void assertNodeExists( long sourceNode ) throws EntityNotFoundException
    {
        if ( !allStoreHolder.nodeExists( sourceNode ) )
        {
            throw new EntityNotFoundException( NODE, sourceNode );
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
            throw new NoSuchConstraintException( constraint, tokenNameLookup );
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

    @SuppressWarnings( "unchecked" )
    private <T extends IndexBackedConstraintDescriptor> T indexBackedConstraintCreate( T constraint, IndexPrototype prototype )
            throws KernelException
    {
        try
        {
            if ( allStoreHolder.constraintExists( constraint ) )
            {
                throw new AlreadyConstrainedException( constraint, CONSTRAINT_CREATION, tokenNameLookup );
            }
            if ( prototype.getIndexType() != IndexType.BTREE )
            {
                throw new CreateConstraintFailureException(
                        constraint, "Cannot create backing constraint index with index type " + prototype.getIndexType() + "." );
            }
            if ( prototype.schema().isFulltextSchemaDescriptor() )
            {
                throw new CreateConstraintFailureException( constraint, "Cannot create backing constraint index using a full-text schema: " +
                        prototype.schema().userDescription( tokenNameLookup ) );
            }
            if ( prototype.schema().isRelationshipTypeSchemaDescriptor() )
            {
                throw new CreateConstraintFailureException( constraint, "Cannot create backing constraint index using a relationship type schema: " +
                        prototype.schema().userDescription( tokenNameLookup ) );
            }
            if ( !prototype.isUnique() )
            {
                throw new CreateConstraintFailureException( constraint,
                        "Cannot create index backed constraint using an index prototype that is not unique: " + prototype.userDescription( tokenNameLookup ) );
            }

            IndexDescriptor index = constraintIndexCreator.createUniquenessConstraintIndex( ktx, constraint, prototype );
            if ( !allStoreHolder.constraintExists( constraint ) )
            {
                // This looks weird, but since we release the label lock while awaiting population of the index
                // backing this constraint there can be someone else getting ahead of us, creating this exact
                // constraint
                // before we do, so now getting out here under the lock we must check again and if it exists
                // we must at this point consider this an idempotent operation because we verified earlier
                // that it didn't exist and went on to create it.
                constraint = (T) constraint.withOwnedIndexId( index.getId() );
                ktx.txState().constraintDoAdd( constraint, index );
            }
            else
            {
                constraint = (T) allStoreHolder.constraintsGetForSchema( constraint.schema() );
            }
            return constraint;
        }
        catch ( UniquePropertyValueValidationException | TransactionFailureException | AlreadyConstrainedException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T extends ConstraintDescriptor> T ensureConstraintHasName( T constraint ) throws KernelException
    {
        if ( constraint.getName() == null )
        {
            SchemaDescriptor schema = constraint.schema();

            int[] entityTokenIds = schema.getEntityTokenIds();
            String[] entityTokenNames;
            switch ( schema.entityType() )
            {
            case NODE:
                entityTokenNames = resolveTokenNames( token::nodeLabelName, entityTokenIds );
                break;
            case RELATIONSHIP:
                entityTokenNames = resolveTokenNames( token::relationshipTypeName, entityTokenIds );
                break;
            default:
                throw new UnspecifiedKernelException( Status.General.UnknownError, "Cannot create constraint for entity type %s in the schema %s.",
                        schema.entityType(), schema );
            }
            int[] propertyIds = schema.getPropertyIds();
            String[] propertyNames = resolveTokenNames( token::propertyKeyName, propertyIds );

            constraint = (T) constraint.withName( SchemaRule.generateName( constraint, entityTokenNames, propertyNames ) );
        }

        return constraint;
    }
}
