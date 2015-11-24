/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.state.AugmentWithLocalStateExpandCursor;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class StateHandlingStatementOperations implements
        KeyReadOperations,
        KeyWriteOperations,
        EntityOperations,
        SchemaReadOperations,
        SchemaWriteOperations,
        CountsOperations,
        LegacyIndexReadOperations,
        LegacyIndexWriteOperations
{
    private final StoreReadLayer storeLayer;
    private final LegacyPropertyTrackers legacyPropertyTrackers;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final LegacyIndexStore legacyIndexStore;

    public StateHandlingStatementOperations(
            StoreReadLayer storeLayer, LegacyPropertyTrackers propertyTrackers,
            ConstraintIndexCreator constraintIndexCreator,
            LegacyIndexStore legacyIndexStore )
    {
        this.storeLayer = storeLayer;
        this.legacyPropertyTrackers = propertyTrackers;
        this.constraintIndexCreator = constraintIndexCreator;
        this.legacyIndexStore = legacyIndexStore;
    }

    @Override
    public long nodeCreate( KernelStatement state )
    {
        long nodeId = storeLayer.reserveNode();
        state.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        assertNodeExists( state, nodeId );
        legacyPropertyTrackers.nodeDelete( nodeId );
        state.txState().nodeDoDelete( nodeId );
    }

    private void assertNodeExists( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        if ( !nodeExists( state, nodeId ) )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
    }

    @Override
    public long relationshipCreate( KernelStatement state, int relationshipTypeId, long startNodeId, long endNodeId )
            throws EntityNotFoundException
    {
        assertNodeExists( state, startNodeId );
        assertNodeExists( state, endNodeId );
        long id = storeLayer.reserveRelationship();
        state.txState().relationshipDoCreate( id, relationshipTypeId, startNodeId, endNodeId );
        return id;
    }

    @Override
    public void relationshipDelete( final KernelStatement state, long relationshipId ) throws EntityNotFoundException
    {
        assertRelationshipExists( state, relationshipId );

        // NOTE: We implicitly delegate to neoStoreTransaction via txState.legacyState here. This is because that
        // call returns modified properties, which node manager uses to update legacy tx state. This will be cleaned up
        // once we've removed legacy tx state.
        legacyPropertyTrackers.relationshipDelete( relationshipId );
        final TransactionState txState = state.txState();
        if ( txState.relationshipIsAddedInThisTx( relationshipId ) )
        {
            txState.relationshipDoDeleteAddedInThisTx( relationshipId );
        }
        else
        {
            try
            {
                storeLayer.relationshipVisit( relationshipId, new RelationshipVisitor<RuntimeException>()
                {
                    @Override
                    public void visit( long relId, int type, long startNode, long endNode )
                    {
                        txState.relationshipDoDelete( relId, type, startNode, endNode );
                    }
                });
            }
            catch ( EntityNotFoundException e )
            {
                // If it doesn't exist, it doesn't exist, and the user got what she wanted.
                return;
            }
        }
    }

    private void assertRelationshipExists( KernelStatement state, long relationshipId ) throws EntityNotFoundException
    {
        if ( !relationshipExists( state, relationshipId ) )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
    }

    @Override
    public boolean nodeExists( KernelStatement state, long nodeId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                return false;
            }

            if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
            {
                return true;
            }
        }

        return storeLayer.nodeExists( nodeId );
    }

    @Override
    public boolean relationshipExists( KernelStatement state, long relId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().relationshipIsDeletedInThisTx( relId ) )
            {
                return false;
            }

            if ( state.txState().relationshipIsAddedInThisTx( relId ) )
            {
                return true;
            }
        }

        return storeLayer.relationshipExists( relId );
    }

    @Override
    public boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                return false;
            }

            switch ( state.txState().labelState( nodeId, labelId ) )
            {
            case ADDED:
                return true;
            case REMOVED:
                return false;
            default: // i.e. UNTOUCHED
                if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
                {
                    return false;
                }
                // else fall through and return from the store layer
            }
        }

        return storeLayer.nodeHasLabel( nodeId, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            return nodeGetLabels( storeLayer, state.txState(), nodeId );
        }
        return storeLayer.nodeGetLabels( nodeId );
    }

    public static PrimitiveIntIterator nodeGetLabels( StoreReadLayer storeLayer, ReadableTxState txState, long nodeId )
            throws EntityNotFoundException
    {
        if ( txState.nodeIsDeletedInThisTx( nodeId ) )
        {
            return PrimitiveIntCollections.emptyIterator();
        }
        if ( txState.nodeIsAddedInThisTx( nodeId ) )
        {
            return PrimitiveIntCollections.toPrimitiveIterator(
                    txState.nodeStateLabelDiffSets( nodeId ).getAdded().iterator() );
        }
        return txState.nodeStateLabelDiffSets( nodeId ).augment( storeLayer.nodeGetLabels( nodeId ) );
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        return state.txState().augmentNodesGetAll( storeLayer.nodesGetAll() );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll( KernelStatement state )
    {
        return state.txState().augmentRelationshipsGetAll( storeLayer.relationshipsGetAll() );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        if ( nodeHasLabel( state, nodeId, labelId ) )
        {
            // Label is already in state or in store, no-op
            return false;
        }

        state.txState().nodeDoAddLabel( labelId, nodeId );
        for ( Iterator<DefinedProperty> properties = nodeGetAllProperties( state, nodeId ); properties.hasNext(); )
        {
            DefinedProperty property = properties.next();
            indexUpdateProperty( state, nodeId, labelId, property.propertyKeyId(), null, property );
        }
        return true;
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        if ( !nodeHasLabel( state, nodeId, labelId ) )
        {
            // Label does not exist in state nor in store, no-op
            return false;
        }

        state.txState().nodeDoRemoveLabel( labelId, nodeId );
        for ( Iterator<DefinedProperty> properties = nodeGetAllProperties( state, nodeId ); properties.hasNext(); )
        {
            DefinedProperty property = properties.next();
            indexUpdateProperty( state, nodeId, labelId, property.propertyKeyId(), property, null );
        }
        return true;
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            PrimitiveLongIterator wLabelChanges =
                    state.txState().nodesWithLabelChanged( labelId ).augment(
                            storeLayer.nodesGetForLabel( state, labelId ) );
            return state.txState().addedAndRemovedNodes().augmentWithRemovals( wLabelChanges );
        }

        return storeLayer.nodesGetForLabel( state, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, int labelId, int propertyKey )
    {
        IndexDescriptor rule = new IndexDescriptor( labelId, propertyKey );
        state.txState().indexRuleDoAdd( rule );
        return rule;
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().indexDoDrop( descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().constraintIndexDoDrop( descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( KernelStatement state, int labelId, int propertyKeyId )
            throws CreateConstraintFailureException
    {
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
        try
        {
            IndexDescriptor index = new IndexDescriptor( labelId, propertyKeyId );
            if ( state.txState().constraintIndexDoUnRemove( index ) ) // ..., DROP, *CREATE*
            { // creation is undoing a drop
                if ( !state.txState().constraintDoUnRemove( constraint ) ) // CREATE, ..., DROP, *CREATE*
                { // ... the drop we are undoing did itself undo a prior create...
                    state.txState().constraintDoAdd(
                            constraint, state.txState().indexCreatedForConstraint( constraint ) );
                }
            }
            else // *CREATE*
            { // create from scratch
                for ( Iterator<UniquenessConstraint> it = storeLayer.constraintsGetForLabelAndPropertyKey(
                        labelId, propertyKeyId ); it.hasNext(); )
                {
                    if ( it.next().equals( labelId, propertyKeyId ) )
                    {
                        return constraint;
                    }
                }
                long indexId = constraintIndexCreator.createUniquenessConstraintIndex(
                        state, this, labelId, propertyKeyId );
                state.txState().constraintDoAdd( constraint, indexId );
            }
            return constraint;
        }
        catch ( ConstraintVerificationFailedKernelException | DropIndexFailureException | TransactionFailureException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state,
            int labelId, int propertyKeyId )
    {
        return applyConstraintsDiff( state, storeLayer.constraintsGetForLabelAndPropertyKey(
                labelId, propertyKeyId ), labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        return applyConstraintsDiff( state, storeLayer.constraintsGetForLabel( labelId ), labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( KernelStatement state )
    {
        return applyConstraintsDiff( state, storeLayer.constraintsGetAll() );
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( KernelStatement state,
                                                                 Iterator<UniquenessConstraint> constraints,
                                                                 int labelId, int propertyKeyId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForLabelAndProperty( labelId, propertyKeyId ).apply( constraints );
        }
        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( KernelStatement state,
                                                                 Iterator<UniquenessConstraint> constraints,
                                                                 int labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForLabel( labelId ).apply( constraints );
        }
        return constraints;
    }

    private Iterator<UniquenessConstraint> applyConstraintsDiff( KernelStatement state,
                                                                 Iterator<UniquenessConstraint> constraints )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChanges().apply( constraints );
        }
        return constraints;
    }

    @Override
    public void constraintDrop( KernelStatement state, UniquenessConstraint constraint )
    {
        state.txState().constraintDoDrop( constraint );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
    {
        IndexDescriptor indexDescriptor = storeLayer.indexesGetForLabelAndPropertyKey( labelId, propertyKey );

        Iterator<IndexDescriptor> rules = iterator( indexDescriptor );
        if ( state.hasTxStateWithChanges() )
        {
            rules = filterByPropertyKeyId(
                    state.txState().indexDiffSetsByLabel( labelId ).apply( rules ),
                    propertyKey );
        }
        return singleOrNull( rules );
    }

    private Iterator<IndexDescriptor> filterByPropertyKeyId(
            Iterator<IndexDescriptor> descriptorIterator,
            final int propertyKey )
    {
        Predicate<IndexDescriptor> predicate = new Predicate<IndexDescriptor>()
        {
            @Override
            public boolean accept( IndexDescriptor item )
            {
                return item.getPropertyKeyId() == propertyKey;
            }
        };
        return filter( predicate, descriptorIterator );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor, state.txState().indexDiffSetsByLabel( descriptor.getLabelId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
            ReadableDiffSets<IndexDescriptor> changes =
                    state.txState().constraintIndexDiffSetsByLabel( descriptor.getLabelId() );
            if ( checkIndexState( descriptor, changes ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return storeLayer.indexGetState( descriptor );
    }

    private boolean checkIndexState( IndexDescriptor indexRule, ReadableDiffSets<IndexDescriptor> diffSet )
            throws IndexNotFoundKernelException
    {
        if ( diffSet.isAdded( indexRule ) )
        {
            return true;
        }
        if ( diffSet.isRemoved( indexRule ) )
        {
            throw new IndexNotFoundKernelException( String.format( "Index for label id %d on property id %d has been " +
                            "dropped in this transaction.",
                    indexRule.getLabelId(),
                    indexRule.getPropertyKeyId() ) );
        }
        return false;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().indexDiffSetsByLabel( labelId )
                    .apply( storeLayer.indexesGetForLabel( labelId ) );
        }

        return storeLayer.indexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().indexChanges().apply( storeLayer.indexesGetAll() );
        }

        return storeLayer.indexesGetAll();
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintIndexDiffSetsByLabel( labelId )
                    .apply( storeLayer.uniqueIndexesGetForLabel( labelId ) );
        }

        return storeLayer.uniqueIndexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintIndexChanges()
                    .apply( storeLayer.uniqueIndexesGetAll() );
        }

        return storeLayer.uniqueIndexesGetAll();
    }

    @Override
    public long nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        PrimitiveLongResourceIterator committed = storeLayer.nodeGetUniqueFromIndexLookup( state, index, value );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        PrimitiveLongIterator changeFilteredMatches = filterIndexStateChanges( state, index, value, exactMatches );
        return single( resourceIterator( changeFilteredMatches, committed ), NO_SUCH_NODE );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException
    {
        PrimitiveLongResourceIterator committed = storeLayer.nodesGetFromIndexLookup( state, index, value );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        PrimitiveLongIterator changeFilteredMatches = filterIndexStateChanges( state, index, value, exactMatches );
        return resourceIterator( changeFilteredMatches, committed );
    }

    private PrimitiveLongIterator filterExactIndexMatches( final KernelStatement state, IndexDescriptor index,
            Object value, PrimitiveLongResourceIterator committed )
    {
        return LookupFilter.exactIndexMatches( this, state, committed, index.getPropertyKeyId(), value );
    }

    private PrimitiveLongIterator filterIndexStateChanges( KernelStatement state, IndexDescriptor index,
            Object value, PrimitiveLongIterator nodeIds )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges = state.txState().indexUpdates( index, value );
            ReadableDiffSets<Long> nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChanges.augment( nodeIds ) );
        }
        return nodeIds;
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws EntityNotFoundException
    {
        Property existingProperty = nodeGetProperty( state, nodeId, property.propertyKeyId() );
        if ( !existingProperty.isDefined() )
        {
            legacyPropertyTrackers.nodeAddStoreProperty( nodeId, property );
        }
        else
        {
            legacyPropertyTrackers.nodeChangeStoreProperty( nodeId, (DefinedProperty) existingProperty, property );
        }
        state.txState().nodeDoReplaceProperty( nodeId, existingProperty, property );
        indexesUpdateProperty( state, nodeId, property.propertyKeyId(),
                               existingProperty instanceof DefinedProperty ? (DefinedProperty) existingProperty : null,
                               property );
        return existingProperty;
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state, long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        Property existingProperty = relationshipGetProperty( state, relationshipId, property.propertyKeyId() );
        if ( !existingProperty.isDefined() )
        {
            legacyPropertyTrackers.relationshipAddStoreProperty( relationshipId, property );
        }
        else
        {
            legacyPropertyTrackers.relationshipChangeStoreProperty( relationshipId, (DefinedProperty)
                    existingProperty, property );
        }
        state.txState().relationshipDoReplaceProperty( relationshipId, existingProperty, property );
        return existingProperty;
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        Property existingProperty = graphGetProperty( state, property.propertyKeyId() );
        state.txState().graphDoReplaceProperty( existingProperty, property );
        return existingProperty;
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        Property existingProperty = nodeGetProperty( state, nodeId, propertyKeyId );
        if ( existingProperty.isDefined() )
        {
            legacyPropertyTrackers.nodeRemoveStoreProperty( nodeId, (DefinedProperty) existingProperty );
            state.txState().nodeDoRemoveProperty( nodeId, (DefinedProperty) existingProperty );
            indexesUpdateProperty( state, nodeId, propertyKeyId, (DefinedProperty) existingProperty, null );
        }
        return existingProperty;
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        Property existingProperty = relationshipGetProperty( state, relationshipId, propertyKeyId );
        if ( existingProperty.isDefined() )
        {
            legacyPropertyTrackers.relationshipRemoveStoreProperty( relationshipId, (DefinedProperty)
                    existingProperty );
            state.txState().relationshipDoRemoveProperty( relationshipId, (DefinedProperty) existingProperty );
        }
        return existingProperty;
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        Property existingProperty = graphGetProperty( state, propertyKeyId );
        if(existingProperty.isDefined())
        {
            state.txState().graphDoRemoveProperty( (DefinedProperty) existingProperty );
        }
        return existingProperty;
    }

    private void indexesUpdateProperty( KernelStatement state, long nodeId, int propertyKey,
                                        DefinedProperty before, DefinedProperty after ) throws EntityNotFoundException
    {
        for ( PrimitiveIntIterator labels = nodeGetLabels( state, nodeId ); labels.hasNext(); )
        {
            indexUpdateProperty( state, nodeId, labels.next(), propertyKey, before, after );
        }
    }

    private void indexUpdateProperty( KernelStatement state, long nodeId, int labelId, int propertyKey,
                                      DefinedProperty before, DefinedProperty after )
    {
        IndexDescriptor descriptor = indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
        if ( descriptor != null )
        {
            state.txState().indexDoUpdateProperty( descriptor, nodeId, before, after );
        }
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( nodeGetAllProperties( state, nodeId ) );
        }

        return storeLayer.nodeGetPropertyKeys( nodeId );
    }

    @Override
    public Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            Iterator<DefinedProperty> properties = nodeGetAllProperties( state, nodeId );
            while ( properties.hasNext() )
            {
                Property property = properties.next();
                if ( property.propertyKeyId() == propertyKeyId )
                {
                    return property;
                }
            }
            return Property.noNodeProperty( nodeId, propertyKeyId );
        }

        return storeLayer.nodeGetProperty( nodeId, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().nodeIsAddedInThisTx( nodeId ) )
            {
                return state.txState().addedAndChangedNodeProperties( nodeId );
            }
            if ( state.txState().nodeIsDeletedInThisTx( nodeId ) )
            {
                // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
                // EntityDeletedException instead and use it instead of returning empty values in similar places
                throw new IllegalStateException( "Node " + nodeId + " has been deleted" );
            }
            return state.txState().augmentNodeProperties( nodeId, storeLayer.nodeGetAllProperties( nodeId ) );
        }

        return storeLayer.nodeGetAllProperties( nodeId );
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( relationshipGetAllProperties( state, relationshipId ) );
        }

        return storeLayer.relationshipGetPropertyKeys( relationshipId );
    }

    @Override
    public Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            Iterator<DefinedProperty> properties = relationshipGetAllProperties( state, relationshipId );
            while ( properties.hasNext() )
            {
                Property property = properties.next();
                if ( property.propertyKeyId() == propertyKeyId )
                {
                    return property;
                }
            }
            return Property.noRelationshipProperty( relationshipId, propertyKeyId );
        }
        return storeLayer.relationshipGetProperty( relationshipId, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( KernelStatement state, long relationshipId )
            throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( state.txState().relationshipIsAddedInThisTx( relationshipId ) )
            {
                return state.txState().addedAndChangedRelationshipProperties( relationshipId );
            }
            if ( state.txState().relationshipIsDeletedInThisTx( relationshipId ) )
            {
                // TODO Throw IllegalStateException to conform with beans API. We may want to introduce
                // EntityDeletedException instead and use it instead of returning empty values in similar places
                throw new IllegalStateException( "Relationship " + relationshipId + " has been deleted" );
            }
            return state.txState().augmentRelationshipProperties( relationshipId,
                    storeLayer.relationshipGetAllProperties( relationshipId ) );
        }
        else
        {
            return storeLayer.relationshipGetAllProperties( relationshipId );
        }
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( graphGetAllProperties( state ) );
        }

        return storeLayer.graphGetPropertyKeys( state );
    }

    @Override
    public Property graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        Iterator<DefinedProperty> properties = graphGetAllProperties( state );
        while ( properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property;
            }
        }
        return Property.noGraphProperty( propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().augmentGraphProperties( storeLayer.graphGetAllProperties() );
        }

        return storeLayer.graphGetAllProperties();
    }

    @Override
    public long countsForNode( KernelStatement statement, int labelId )
    {
        return storeLayer.countsForNode( labelId );
    }

    @Override
    public long countsForRelationship( KernelStatement statement, int startLabelId, int typeId, int endLabelId )
    {
        return storeLayer.countsForRelationship( startLabelId, typeId, endLabelId );
    }

    @Override
    public double indexUniqueValuesPercentage( KernelStatement statement, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexUniqueValuesPercentage( descriptor );
    }

    @Override
    public PrimitiveLongIterator nodeGetRelationships( KernelStatement state, long nodeId, Direction direction,
                                                       int[] relTypes ) throws EntityNotFoundException
    {
        relTypes = deduplicate( relTypes );

        if ( state.hasTxStateWithChanges() )
        {
            ReadableTxState txState = state.txState();
            PrimitiveLongIterator stored;
            if( txState.nodeIsAddedInThisTx( nodeId ) )
            {
                stored = PrimitiveLongCollections.emptyIterator();
            }
            else
            {
                stored = storeLayer.nodeListRelationships( nodeId, direction, relTypes );
            }
            return txState.augmentRelationships( nodeId, direction, relTypes, stored );
        }
        return storeLayer.nodeListRelationships( nodeId, direction, relTypes );
    }

    @Override
    public Cursor nodeGetRelationships( KernelStatement state, long nodeId, Direction direction, int[] types,
                                        RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException
    {
        return new RelationshipCursor( state, nodeGetRelationships( state, nodeId, direction, types ), visitor );
    }

    @Override
    public PrimitiveLongIterator nodeGetRelationships( KernelStatement state, long nodeId, Direction direction ) throws EntityNotFoundException
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableTxState txState = state.txState();
            PrimitiveLongIterator stored;
            if( txState.nodeIsAddedInThisTx( nodeId ) )
            {
                stored = PrimitiveLongCollections.emptyIterator();
            }
            else
            {
                stored = storeLayer.nodeListRelationships( nodeId, direction );
            }
            return txState.augmentRelationships( nodeId, direction, stored );
        }
        return storeLayer.nodeListRelationships( nodeId, direction );
    }

    @Override
    public Cursor nodeGetRelationships( KernelStatement state, long nodeId, Direction direction,
                                        RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException
    {
        return new RelationshipCursor( state, nodeGetRelationships( state, nodeId, direction ), visitor );
    }

    private class RelationshipCursor implements Cursor
    {
        private final PrimitiveLongIterator relationships;
        private final KernelStatement state;
        private final RelationshipVisitor<? extends RuntimeException> visitor;

        public RelationshipCursor( KernelStatement state, PrimitiveLongIterator relationships,
                                   RelationshipVisitor<? extends RuntimeException> visitor )
        {
            this.relationships = relationships;
            this.state = state;
            this.visitor = visitor;
        }

        @Override
        public boolean next()
        {
            while ( relationships.hasNext() )
            {
                try
                {
                    relationshipVisit( state, relationships.next(), visitor );
                }
                catch ( EntityNotFoundException e )
                {
                    continue;
                }
                return true;
            }
            return false;
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }

    @Override
    public int nodeGetDegree( KernelStatement state, long nodeId, Direction direction, int relType ) throws EntityNotFoundException

    {
        if( state.hasTxStateWithChanges() )
        {
            int degree = 0;
            if(state.txState().nodeIsDeletedInThisTx( nodeId ))
            {
                return 0;
            }

            if( !state.txState().nodeIsAddedInThisTx( nodeId ))
            {
                degree = storeLayer.nodeGetDegree( nodeId, direction, relType );
            }

            return state.txState().augmentNodeDegree( nodeId, degree, direction, relType );
        }
        else
        {
            return storeLayer.nodeGetDegree( nodeId, direction, relType );
        }
    }

    @Override
    public int nodeGetDegree( KernelStatement state, long nodeId, Direction direction ) throws EntityNotFoundException
    {
        if( state.hasTxStateWithChanges() )
        {
            int degree = 0;
            if(state.txState().nodeIsDeletedInThisTx( nodeId ))
            {
                return 0;
            }

            if( !state.txState().nodeIsAddedInThisTx( nodeId ))
            {
                degree = storeLayer.nodeGetDegree( nodeId, direction );
            }
            return state.txState().augmentNodeDegree( nodeId, degree, direction );
        }
        else
        {
            return storeLayer.nodeGetDegree( nodeId, direction );
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        if(state.hasTxStateWithChanges() && state.txState().nodeModifiedInThisTx(nodeId))
        {
            ReadableTxState tx = state.txState();
            if(tx.nodeIsDeletedInThisTx( nodeId ))
            {
                return PrimitiveIntCollections.emptyIterator();
            }

            if(tx.nodeIsAddedInThisTx( nodeId ))
            {
                return tx.nodeRelationshipTypes(nodeId);
            }

            Set<Integer> types = new HashSet<>();

            // Add types in the current transaction
            PrimitiveIntIterator typesInTx = tx.nodeRelationshipTypes( nodeId );
            while(typesInTx.hasNext())
            {
                types.add( typesInTx.next() );
            }

            // Augment with types stored on disk, minus any types where all rels of that type are deleted
            // in current tx.
            PrimitiveIntIterator committedTypes = storeLayer.nodeGetRelationshipTypes( nodeId );
            while(committedTypes.hasNext())
            {
                int current = committedTypes.next();
                if(!types.contains( current ) && nodeGetDegree( state, nodeId, Direction.BOTH, current ) > 0)
                {
                    types.add( current );
                }
            }

            return PrimitiveIntCollections.toPrimitiveIterator( types.iterator() );
        }
        else
        {
            return storeLayer.nodeGetRelationshipTypes( nodeId );
        }
    }

    //
    // Methods that delegate directly to storage
    //

    @Override
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return storeLayer.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
            throws SchemaRuleNotFoundException
    {
        return storeLayer.indexGetCommittedId( index, kind );
    }

    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexGetFailure( descriptor );
    }

    @Override
    public int labelGetForName( Statement state, String labelName )
    {
        return storeLayer.labelGetForName( labelName );
    }

    @Override
    public String labelGetName( Statement state, int labelId ) throws LabelNotFoundKernelException
    {
        return storeLayer.labelGetName( labelId );
    }

    @Override
    public int propertyKeyGetForName( Statement state, String propertyKeyName )
    {
        return storeLayer.propertyKeyGetForName( propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( Statement state, int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        return storeLayer.propertyKeyGetName( propertyKeyId );
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens( Statement state )
    {
        return storeLayer.propertyKeyGetAllTokens();
    }

    @Override
    public Iterator<Token> labelsGetAllTokens( Statement state )
    {
        return storeLayer.labelsGetAllTokens();
    }

    @Override
    public int relationshipTypeGetForName( Statement state, String relationshipTypeName )
    {
        return storeLayer.relationshipTypeGetForName( relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( Statement state, int relationshipTypeId ) throws
            RelationshipTypeIdNotFoundKernelException
    {
        return storeLayer.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public int labelGetOrCreateForName( Statement state, String labelName ) throws IllegalTokenNameException,
            TooManyLabelsException
    {
        return storeLayer.labelGetOrCreateForName( labelName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( Statement state, String propertyKeyName ) throws IllegalTokenNameException
    {
        return storeLayer.propertyKeyGetOrCreateForName( propertyKeyName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( Statement state, String relationshipTypeName )
            throws IllegalTokenNameException
    {
        return storeLayer.relationshipTypeGetOrCreateForName( relationshipTypeName );
    }

    @Override
    public void labelCreateForName( KernelStatement state, String labelName,
                                    int id ) throws IllegalTokenNameException, TooManyLabelsException
    {
        state.txState().labelDoCreateForName( labelName, id );
    }

    @Override
    public void propertyKeyCreateForName( KernelStatement state, String propertyKeyName, int id ) throws IllegalTokenNameException
    {
        state.txState().propertyKeyDoCreateForName( propertyKeyName, id );

    }

    @Override
    public void relationshipTypeCreateForName( KernelStatement state, String relationshipTypeName, int id ) throws IllegalTokenNameException
    {
        state.txState().relationshipTypeDoCreateForName( relationshipTypeName, id );
    }

    private static int[] deduplicate( int[] types )
    {
        int unique = 0;
        for ( int i = 0; i < types.length; i++ )
        {
            int type = types[i];
            for ( int j = 0; j < unique; j++ )
            {
                if ( type == types[j] )
                {
                    type = -1; // signal that this relationship is not unique
                    break; // we will not find more than one conflict
                }
            }
            if ( type != -1 )
            { // this has to be done outside the inner loop, otherwise we'd never accept a single one...
                types[unique++] = types[i];
            }
        }
        if ( unique < types.length )
        {
            types = Arrays.copyOf( types, unique );
        }
        return types;
    }

    // <Legacy index>
    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement,
            long relId, RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION
    {
        if ( statement.hasTxStateWithChanges() )
        {
            if ( statement.txState().relationshipVisit( relId, visitor ) )
            {
                return;
            }
        }
        storeLayer.relationshipVisit( relId, visitor );
    }

    @Override
    public Cursor expand( KernelStatement statement, Cursor inputCursor,
                          NeoRegister.Node.In nodeId, Register.Object.In<int[]> types,
                          Register.Object.In<Direction> expandDirection, NeoRegister.Relationship.Out relId,
                          NeoRegister.RelType.Out relType, Register.Object.Out<Direction> direction,
                          NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId )
    {
        if( statement.hasTxStateWithChanges() )
        {
            return new AugmentWithLocalStateExpandCursor( storeLayer, statement.txState(),
                    inputCursor, nodeId, types, expandDirection, relId, relType, direction, startNodeId, neighborNodeId );
        }
        return storeLayer.expand( inputCursor, nodeId, types, expandDirection,
                                  relId, relType, direction, startNodeId, neighborNodeId );
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexGet( KernelStatement statement, String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        return statement.legacyIndexTxState().nodeChanges( indexName ).get( key, value );
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( KernelStatement statement, String indexName, String key,
            Object queryOrQueryObject ) throws LegacyIndexNotFoundKernelException
    {
        return statement.legacyIndexTxState().nodeChanges( indexName ).query( key, queryOrQueryObject );
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( KernelStatement statement, String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        return statement.legacyIndexTxState().nodeChanges( indexName ).query( queryOrQueryObject );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexGet( KernelStatement statement, String indexName, String key,
            Object value, long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        LegacyIndex index = statement.legacyIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.get( key, value, startNode, endNode );
        }
        return index.get( key, value );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( KernelStatement statement, String indexName, String key,
            Object queryOrQueryObject, long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        LegacyIndex index = statement.legacyIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.query( key, queryOrQueryObject, startNode, endNode );
        }
        return index.query( key, queryOrQueryObject );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( KernelStatement statement, String indexName,
            Object queryOrQueryObject, long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        LegacyIndex index = statement.legacyIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.query( queryOrQueryObject, startNode, endNode );
        }
        return index.query( queryOrQueryObject );
    }

    @Override
    public void nodeLegacyIndexCreateLazily( KernelStatement statement, String indexName,
            Map<String, String> customConfig )
    {
        legacyIndexStore.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    @Override
    public void nodeLegacyIndexCreate( KernelStatement statement, String indexName, Map<String, String> customConfig )
    {
        statement.txState().nodeLegacyIndexDoCreate( indexName, customConfig );
    }

    @Override
    public void relationshipLegacyIndexCreateLazily( KernelStatement statement, String indexName,
            Map<String, String> customConfig )
    {
        legacyIndexStore.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    @Override
    public void relationshipLegacyIndexCreate( KernelStatement statement, String indexName, Map<String, String> customConfig )
    {
        statement.txState().relationshipLegacyIndexDoCreate( indexName, customConfig );
    }

    @Override
    public void nodeAddToLegacyIndex( KernelStatement statement, String indexName, long node, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().nodeChanges( indexName ).addNode( node, key, value );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( KernelStatement statement, String indexName, long node, String key,
            Object value ) throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().nodeChanges( indexName ).remove( node, key, value );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( KernelStatement statement, String indexName, long node, String key )
            throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().nodeChanges( indexName ).remove( node, key );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( KernelStatement statement, String indexName, long node )
            throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().nodeChanges( indexName ).remove( node );
    }

    @Override
    public void relationshipAddToLegacyIndex( final KernelStatement statement, final String indexName,
            final long relationship, final String key, final Object value )
                    throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        relationshipVisit( statement, relationship, new RelationshipVisitor<LegacyIndexNotFoundKernelException>()
        {
            @Override
            public void visit( long relId, int type, long startNode, long endNode )
                    throws LegacyIndexNotFoundKernelException
            {
                statement.legacyIndexTxState().relationshipChanges( indexName ).addRelationship(
                        relationship, key, value, startNode, endNode );
            }
        } );
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement, final String indexName, long relationship,
            final String key, final Object value ) throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        try
        {
            relationshipVisit( statement, relationship, new RelationshipVisitor<LegacyIndexNotFoundKernelException>()
            {
                @Override
                public void visit( long relId, int type, long startNode, long endNode )
                        throws LegacyIndexNotFoundKernelException
                {
                    statement.legacyIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, key, value, startNode, endNode );
                }
            } );
        }
        catch ( EntityNotFoundException e )
        {   // Apparently this is OK
        }
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement, final String indexName, long relationship,
            final String key ) throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        try
        {
            relationshipVisit( statement, relationship, new RelationshipVisitor<LegacyIndexNotFoundKernelException>()
            {
                @Override
                public void visit( long relId, int type, long startNode, long endNode )
                        throws LegacyIndexNotFoundKernelException
                {
                    statement.legacyIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, key, startNode, endNode );
                }
            } );
        }
        catch ( EntityNotFoundException e )
        {   // Apparently this is OK
        }
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement, final String indexName, long relationship )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        try
        {
            relationshipVisit( statement, relationship, new RelationshipVisitor<LegacyIndexNotFoundKernelException>()
            {
                @Override
                public void visit( long relId, int type, long startNode, long endNode )
                        throws LegacyIndexNotFoundKernelException
                {
                    statement.legacyIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, startNode, endNode );
                }
            } );
        }
        catch ( EntityNotFoundException e )
        {
            // This is a special case which is still OK. This method is called lazily where deleted relationships
            // that still are referenced by a legacy index will be added for removal in this transaction.
            // Ideally we'd want to include start/end node too, but we can't since the relationship doesn't exist.
            // So we do the "normal" remove call on the legacy index transaction changes. The downside is that
            // Some queries on this transaction state that include start/end nodes might produce invalid results.
            statement.legacyIndexTxState().relationshipChanges( indexName ).remove( relationship );
        }
    }

    @Override
    public void nodeLegacyIndexDrop( KernelStatement statement, String indexName ) throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().nodeChanges( indexName ).drop();
        statement.legacyIndexTxState().deleteIndex( IndexEntityType.Node, indexName );
    }

    @Override
    public void relationshipLegacyIndexDrop( KernelStatement statement, String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        statement.legacyIndexTxState().relationshipChanges( indexName ).drop();
        statement.legacyIndexTxState().deleteIndex( IndexEntityType.Relationship, indexName );
    }

    @Override
    public String nodeLegacyIndexSetConfiguration( KernelStatement statement, String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.setNodeIndexConfiguration( indexName, key, value );
    }

    @Override
    public String relationshipLegacyIndexSetConfiguration( KernelStatement statement, String indexName, String key,
            String value ) throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.setRelationshipIndexConfiguration( indexName, key, value );
    }

    @Override
    public String nodeLegacyIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.removeNodeIndexConfiguration( indexName, key );
    }

    @Override
    public String relationshipLegacyIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.removeRelationshipIndexConfiguration( indexName, key );
    }

    @Override
    public Map<String, String> nodeLegacyIndexGetConfiguration( KernelStatement statement, String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.getNodeIndexConfiguration( indexName );
    }

    @Override
    public Map<String, String> relationshipLegacyIndexGetConfiguration( KernelStatement statement, String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        return legacyIndexStore.getRelationshipIndexConfiguration( indexName );
    }

    @Override
    public String[] nodeLegacyIndexesGetAll( KernelStatement statement )
    {
        return legacyIndexStore.getAllNodeIndexNames();
    }

    @Override
    public String[] relationshipLegacyIndexesGetAll( KernelStatement statement )
    {
        return legacyIndexStore.getAllRelationshipIndexNames();
    }
    // </Legacy index>
}
