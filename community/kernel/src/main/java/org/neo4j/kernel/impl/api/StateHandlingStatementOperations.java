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
import java.util.Iterator;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Predicate;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.MandatoryNodePropertyConstraint;
import org.neo4j.kernel.api.constraints.MandatoryRelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.EntityItem;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
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
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

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

    // <Cursors>
    @Override
    public Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId )
    {
        Cursor<NodeItem> cursor = statement.getStoreStatement().acquireSingleNodeCursor( nodeId );
        if ( statement.hasTxStateWithChanges() )
        {
            return statement.txState().augmentSingleNodeCursor( cursor, nodeId );
        }
        return cursor;
    }

    @Override
    public Cursor<NodeItem> nodeCursor( TxStateHolder txStateHolder, StoreStatement statement, long nodeId )
    {
        Cursor<NodeItem> cursor = statement.acquireSingleNodeCursor( nodeId );
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            return txStateHolder.txState().augmentSingleNodeCursor( cursor, nodeId );
        }
        return cursor;
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relationshipId )
    {
        Cursor<RelationshipItem> cursor = statement.getStoreStatement().acquireSingleRelationshipCursor(
                relationshipId );
        if ( statement.hasTxStateWithChanges() )
        {
            return statement.txState().augmentSingleRelationshipCursor( cursor, relationshipId );
        }
        return cursor;
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( TxStateHolder txStateHolder, StoreStatement statement,
            long relationshipId )
    {
        Cursor<RelationshipItem> cursor = statement.acquireSingleRelationshipCursor( relationshipId );
        if ( txStateHolder.hasTxStateWithChanges() )
        {
            return txStateHolder.txState().augmentSingleRelationshipCursor( cursor, relationshipId );
        }
        else
        {
            return cursor;
        }
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll( KernelStatement statement )
    {
        Cursor<NodeItem> cursor = statement.getStoreStatement().nodesGetAllCursor();
        if ( statement.hasTxStateWithChanges() )
        {
            return statement.txState().augmentNodesGetAllCursor( cursor );
        }
        return cursor;
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        Cursor<RelationshipItem> cursor = statement.getStoreStatement().relationshipsGetAllCursor();
        if ( statement.hasTxStateWithChanges() )
        {
            return statement.txState().augmentRelationshipsGetAllCursor( cursor );
        }
        return cursor;
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( KernelStatement statement, int labelId )
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodesGetForLabel( statement, labelId ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor( storeLayer.nodesGetFromIndexSeek( statement,
                index, value ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodesGetFromIndexScan( statement, index ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor( storeLayer.nodesGetFromIndexRangeSeekByPrefix(
                statement, index, prefix ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                        includeUpper ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( KernelStatement statement,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodesGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                        includeUpper ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement, IndexDescriptor index,
            String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodesGetFromIndexRangeSeekByPrefix( statement, index, prefix ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexBrokenKernelException, IndexNotFoundKernelException
    {
        // TODO Filter this properly
        return statement.getStoreStatement().acquireIteratorNodeCursor(
                storeLayer.nodeGetFromUniqueIndexSeek( statement, index, value ) );
    }

    // </Cursors>

    @Override
    public long nodeCreate( KernelStatement state )
    {
        long nodeId = storeLayer.reserveNode();
        state.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public void nodeDelete( KernelStatement state, NodeItem node )
    {
        legacyPropertyTrackers.nodeDelete( node.id() );
        state.txState().nodeDoDelete( node.id() );
    }

    @Override
    public long relationshipCreate( KernelStatement state,
            int relationshipTypeId,
            NodeItem startNode,
            NodeItem endNode )
            throws EntityNotFoundException
    {
        long id = storeLayer.reserveRelationship();
        state.txState().relationshipDoCreate( id, relationshipTypeId, startNode.id(), endNode.id() );
        return id;
    }

    @Override
    public void relationshipDelete( final KernelStatement state, RelationshipItem relationship )
    {
        // NOTE: We implicitly delegate to neoStoreTransaction via txState.legacyState here. This is because that
        // call returns modified properties, which node manager uses to update legacy tx state. This will be cleaned up
        // once we've removed legacy tx state.
        legacyPropertyTrackers.relationshipDelete( relationship.id() );
        final TransactionState txState = state.txState();
        if ( txState.relationshipIsAddedInThisTx( relationship.id() ) )
        {
            txState.relationshipDoDeleteAddedInThisTx( relationship.id() );
        }
        else
        {
            txState.relationshipDoDelete( relationship.id(), relationship.type(), relationship.startNode(),
                    relationship.endNode() );
        }
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        return state.txState().augmentNodesGetAll( storeLayer.nodesGetAll() );
    }

    @Override

    public RelationshipIterator relationshipsGetAll( KernelStatement state )
    {
        return state.txState().augmentRelationshipsGetAll( storeLayer.relationshipsGetAll() );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, NodeItem node, int labelId )
    {
        try ( Cursor<LabelItem> labels = node.label( labelId ) )
        {
            if ( labels.next() )
            {
                // Label is already in state or in store, no-op
                return false;
            }
        }

        state.txState().nodeDoAddLabel( labelId, node.id() );

        try ( Cursor<PropertyItem> properties = node.properties() )
        {
            while ( properties.next() )
            {
                PropertyItem propertyItem = properties.get();
                IndexDescriptor descriptor = indexesGetForLabelAndPropertyKey( state, labelId,
                        propertyItem.propertyKeyId() );
                if ( descriptor != null )
                {
                    DefinedProperty after = Property.property( propertyItem.propertyKeyId(),
                            propertyItem.value() );

                    state.txState().indexDoUpdateProperty( descriptor, node.id(), null, after );
                }
            }

            return true;
        }
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, NodeItem node, int labelId )
    {
        try ( Cursor<LabelItem> labels = node.label( labelId ) )
        {
            if ( !labels.next() )
            {
                // Label does not exist in state or in store, no-op
                return false;
            }
        }

        state.txState().nodeDoRemoveLabel( labelId, node.id() );

        try ( Cursor<PropertyItem> properties = node.properties() )
        {
            while ( properties.next() )
            {
                PropertyItem propItem = properties.get();
                DefinedProperty property = Property.property( propItem.propertyKeyId(), propItem.value() );
                indexUpdateProperty( state, node.id(), labelId, property.propertyKeyId(), property, null );
            }
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
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state, int labelId, int propertyKeyId )
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
                for ( Iterator<NodePropertyConstraint> it = storeLayer.constraintsGetForLabelAndPropertyKey(
                        labelId, propertyKeyId ); it.hasNext(); )
                {
                    if ( it.next().equals( constraint ) )
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
        catch ( ConstraintVerificationFailedKernelException | DropIndexFailureException | TransactionFailureException
                e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
    }

    @Override
    public MandatoryNodePropertyConstraint mandatoryNodePropertyConstraintCreate( KernelStatement state, int labelId,
            int propertyKeyId ) throws CreateConstraintFailureException
    {
        MandatoryNodePropertyConstraint constraint = new MandatoryNodePropertyConstraint( labelId, propertyKeyId );
        state.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public MandatoryRelationshipPropertyConstraint mandatoryRelationshipPropertyConstraintCreate( KernelStatement state,
            int relTypeId, int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        MandatoryRelationshipPropertyConstraint constraint =
                new MandatoryRelationshipPropertyConstraint( relTypeId, propertyKeyId );
        state.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state,
            int labelId, int propertyKeyId )
    {
        Iterator<NodePropertyConstraint> constraints =
                storeLayer.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForLabelAndProperty( labelId, propertyKeyId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        Iterator<NodePropertyConstraint> constraints = storeLayer.constraintsGetForLabel( labelId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForLabel( labelId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey(
            KernelStatement state, int relTypeId, int propertyKeyId )
    {
        Iterator<RelationshipPropertyConstraint> constraints =
                storeLayer.constraintsGetForRelationshipTypeAndPropertyKey( relTypeId, propertyKeyId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState()
                    .constraintsChangesForRelationshipTypeAndProperty( relTypeId, propertyKeyId )
                    .apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( KernelStatement state,
            int typeId )
    {
        Iterator<RelationshipPropertyConstraint> constraints = storeLayer.constraintsGetForRelationshipType( typeId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll( KernelStatement state )
    {
        Iterator<PropertyConstraint> constraints = storeLayer.constraintsGetAll();
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChanges().apply( constraints );
        }
        return constraints;
    }


    @Override
    public void constraintDrop( KernelStatement state, NodePropertyConstraint constraint )
    {
        state.txState().constraintDoDrop( constraint );
    }

    @Override
    public void constraintDrop( KernelStatement state, RelationshipPropertyConstraint constraint )
            throws DropConstraintFailureException
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
            public boolean test( IndexDescriptor item )
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
    public long nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        PrimitiveLongResourceIterator committed = storeLayer.nodeGetFromUniqueIndexSeek( state, index, value );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        PrimitiveLongIterator changesFiltered = filterIndexStateChangesForScanOrSeek( state, index, value,
                exactMatches );
        return single( resourceIterator( changesFiltered, committed ), NO_SUCH_NODE );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        PrimitiveLongIterator committed = storeLayer.nodesGetFromIndexSeek( state, index, value );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        return filterIndexStateChangesForScanOrSeek( state, index, value, exactMatches );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement state, IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper ) throws IndexNotFoundKernelException

    {
        PrimitiveLongIterator committed = storeLayer.nodesGetFromIndexRangeSeekByNumber( state, index, lower,
                includeLower, upper, includeUpper );
        return filterIndexStateChangesForRangeSeekByNumber( state, index, lower, includeLower, upper, includeUpper,
                committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement state, IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper ) throws IndexNotFoundKernelException

    {
        PrimitiveLongIterator committed = storeLayer.nodesGetFromIndexRangeSeekByString( state, index, lower,
                includeLower, upper, includeUpper );
        return filterIndexStateChangesForRangeSeekByString( state, index, lower, includeLower, upper, includeUpper,
                committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state, IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        PrimitiveLongIterator committed = storeLayer.nodesGetFromIndexRangeSeekByPrefix( state, index, prefix );
        return filterIndexStateChangesForRangeSeekByPrefix( state, index, prefix, committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        PrimitiveLongIterator committed = storeLayer.nodesGetFromIndexScan( state, index );
        return filterIndexStateChangesForScanOrSeek( state, index, null, committed );
    }

    private PrimitiveLongIterator filterExactIndexMatches( final KernelStatement state, IndexDescriptor index,
            Object value, PrimitiveLongIterator committed )
    {
        return LookupFilter.exactIndexMatches( this, state, committed, index.getPropertyKeyId(), value );
    }

    private PrimitiveLongIterator filterIndexStateChangesForScanOrSeek( KernelStatement state, IndexDescriptor index,
            Object value, PrimitiveLongIterator nodeIds )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges = state.txState().indexUpdatesForScanOrSeek( index, value );
            ReadableDiffSets<Long> nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChanges.augment( nodeIds ) );
        }
        return nodeIds;
    }

    private PrimitiveLongIterator filterIndexStateChangesForRangeSeekByNumber( KernelStatement state,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper,
            PrimitiveLongIterator nodeIds )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForNumber =
                    state.txState().indexUpdatesForRangeSeekByNumber( index, lower, includeLower, upper, includeUpper );
            ReadableDiffSets<Long> nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChangesForNumber.augment( nodeIds ) );
        }
        return nodeIds;

    }

    private PrimitiveLongIterator filterIndexStateChangesForRangeSeekByString( KernelStatement state,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper,
            PrimitiveLongIterator nodeIds )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForString =
                    state.txState().indexUpdatesForRangeSeekByString( index, lower, includeLower, upper, includeUpper );
            ReadableDiffSets<Long> nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChangesForString.augment( nodeIds ) );
        }
        return nodeIds;

    }

    private PrimitiveLongIterator filterIndexStateChangesForRangeSeekByPrefix( KernelStatement state,
            IndexDescriptor index,
            String prefix,
            PrimitiveLongIterator nodeIds )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChangesForPrefix =
                    state.txState().indexUpdatesForRangeSeekByPrefix( index, prefix );
            ReadableDiffSets<Long> nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChangesForPrefix.augment( nodeIds ) );
        }
        return nodeIds;
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, NodeItem node, DefinedProperty property )
    {
        Property existingProperty;
        try ( Cursor<PropertyItem> properties = node.property( property.propertyKeyId() ) )
        {
            if ( !properties.next() )
            {
                legacyPropertyTrackers.nodeAddStoreProperty( node.id(), property );
                existingProperty = Property.noProperty( property.propertyKeyId(), EntityType.NODE, node.id() );
            }
            else
            {
                existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );
                legacyPropertyTrackers.nodeChangeStoreProperty( node.id(), (DefinedProperty) existingProperty,
                        property );
            }
        }

        state.txState().nodeDoReplaceProperty( node.id(), existingProperty, property );

        PrimitiveIntCollection labelIds = getLabels( node );

        indexesUpdateProperty( state, node.id(), labelIds, property.propertyKeyId(),
                existingProperty instanceof DefinedProperty ? (DefinedProperty) existingProperty : null,
                property );

        return existingProperty;
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            RelationshipItem relationship,
            DefinedProperty property )
    {
        Property existingProperty;
        try ( Cursor<PropertyItem> properties = relationship.property( property.propertyKeyId() ) )
        {
            if ( !properties.next() )
            {
                legacyPropertyTrackers.relationshipAddStoreProperty( relationship.id(), property );
                existingProperty = Property.noProperty( property.propertyKeyId(), EntityType.RELATIONSHIP,
                        relationship.id() );
            }
            else
            {
                existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );
                legacyPropertyTrackers.relationshipChangeStoreProperty( relationship.id(),
                        (DefinedProperty) existingProperty, property );
            }
        }

        state.txState().relationshipDoReplaceProperty( relationship.id(), existingProperty, property );
        return existingProperty;
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        Object existingPropertyValue = graphGetProperty( state, property.propertyKeyId() );
        Property existingProperty = existingPropertyValue == null ?
                Property.noGraphProperty( property.propertyKeyId() ) :
                Property.property( property.propertyKeyId(), existingPropertyValue );
        state.txState().graphDoReplaceProperty( existingProperty, property );
        return existingProperty;
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, NodeItem node, int propertyKeyId )
    {
        PrimitiveIntCollection labelIds = getLabels( node );

        Property existingProperty;
        try ( Cursor<PropertyItem> properties = node.property( propertyKeyId ) )
        {
            if ( !properties.next() )
            {
                existingProperty = Property.noProperty( propertyKeyId, EntityType.NODE, node.id() );
            }
            else
            {
                existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );

                legacyPropertyTrackers.nodeRemoveStoreProperty( node.id(), (DefinedProperty) existingProperty );
                state.txState().nodeDoRemoveProperty( node.id(), (DefinedProperty) existingProperty );

                indexesUpdateProperty( state, node.id(), labelIds, propertyKeyId,
                        (DefinedProperty) existingProperty, null );
            }
        }
        return existingProperty;
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            RelationshipItem relationship,
            int propertyKeyId )
    {
        Property existingProperty;
        try ( Cursor<PropertyItem> properties = relationship.property( propertyKeyId ) )
        {
            if ( !properties.next() )
            {
                existingProperty = Property.noProperty( propertyKeyId, EntityType.RELATIONSHIP, relationship.id() );
            }
            else
            {
                existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );

                legacyPropertyTrackers.relationshipRemoveStoreProperty( relationship.id(),
                        (DefinedProperty) existingProperty );
                state.txState().relationshipDoRemoveProperty( relationship.id(),
                        (DefinedProperty) existingProperty );
            }
        }
        return existingProperty;
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        Object existingPropertyValue = graphGetProperty( state, propertyKeyId );
        if ( existingPropertyValue != null )
        {
            DefinedProperty existingProperty = Property.property( propertyKeyId, existingPropertyValue );
            state.txState().graphDoRemoveProperty( existingProperty );
            return existingProperty;
        }
        else
        {
            return Property.noGraphProperty( propertyKeyId );
        }
    }

    private void indexesUpdateProperty( KernelStatement state,
            long nodeId,
            PrimitiveIntCollection labels,
            int propertyKey,
            DefinedProperty before,
            DefinedProperty after )
    {
        PrimitiveIntIterator labelIterator = labels.iterator();
        while ( labelIterator.hasNext() )
        {
            indexUpdateProperty( state, nodeId, labelIterator.next(), propertyKey, before, after );
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
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return new PropertyKeyIdIterator( graphGetAllProperties( state ) );
        }

        return storeLayer.graphGetPropertyKeys( state );
    }

    @Override
    public boolean graphHasProperty( KernelStatement state, int propertyKeyId )
    {
        return graphGetProperty( state, propertyKeyId ) != null;
    }

    @Override
    public Object graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        Iterator<DefinedProperty> properties = graphGetAllProperties( state );
        while ( properties.hasNext() )
        {
            DefinedProperty property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property.value();
            }
        }
        return null;
    }

    private Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state )
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
    public long indexSize( KernelStatement statement, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexSize( descriptor );
    }

    @Override
    public double indexUniqueValuesPercentage( KernelStatement statement, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexUniqueValuesPercentage( descriptor );
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
    public void propertyKeyCreateForName( KernelStatement state,
            String propertyKeyName,
            int id ) throws IllegalTokenNameException
    {
        state.txState().propertyKeyDoCreateForName( propertyKeyName, id );

    }

    @Override
    public void relationshipTypeCreateForName( KernelStatement state,
            String relationshipTypeName,
            int id ) throws IllegalTokenNameException
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
    public LegacyIndexHits nodeLegacyIndexQuery( KernelStatement statement,
            String indexName,
            Object queryOrQueryObject )
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
    public void relationshipLegacyIndexCreate( KernelStatement statement,
            String indexName,
            Map<String, String> customConfig )
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
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement,
            final String indexName,
            long relationship,
            final String key,
            final Object value ) throws LegacyIndexNotFoundKernelException, EntityNotFoundException
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
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement,
            final String indexName,
            long relationship,
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
    public void relationshipRemoveFromLegacyIndex( final KernelStatement statement,
            final String indexName,
            long relationship )
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
    public void nodeLegacyIndexDrop( KernelStatement statement,
            String indexName ) throws LegacyIndexNotFoundKernelException
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
    public String nodeLegacyIndexSetConfiguration( KernelStatement statement,
            String indexName,
            String key,
            String value )
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

    private PrimitiveIntIterator getPropertyKeys( EntityItem entity )
    {
        PrimitiveIntStack keys = new PrimitiveIntStack();
        try ( Cursor<PropertyItem> properties = entity.properties() )
        {
            while ( properties.next() )
            {
                keys.push( properties.get().propertyKeyId() );
            }
        }

        return keys.iterator();
    }

    private boolean hasProperty( EntityItem entity, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = entity.property( propertyKeyId ) )
        {
            return cursor.next();
        }
    }

    private Object getProperty( EntityItem entity, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = entity.property( propertyKeyId ) )
        {
            if ( cursor.next() )
            {
                return cursor.get().value();
            }
        }

        return null;
    }

    private PrimitiveIntCollection getLabels( NodeItem node )
    {
        PrimitiveIntStack labelIds = new PrimitiveIntStack();
        try ( Cursor<LabelItem> labels = node.labels() )
        {
            while ( labels.next() )
            {
                labelIds.push( labels.get().getAsInt() );
            }
        }
        return labelIds;
    }
}
