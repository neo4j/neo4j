/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
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
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
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
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_NUMBERS;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

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
    private final AutoIndexing autoIndexing;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final LegacyIndexStore legacyIndexStore;

    public StateHandlingStatementOperations(
            StoreReadLayer storeLayer, AutoIndexing propertyTrackers,
            ConstraintIndexCreator constraintIndexCreator,
            LegacyIndexStore legacyIndexStore )
    {
        this.storeLayer = storeLayer;
        this.autoIndexing = propertyTrackers;
        this.constraintIndexCreator = constraintIndexCreator;
        this.legacyIndexStore = legacyIndexStore;
    }

    // <Cursors>

    @Override
    public Cursor<NodeItem> nodeCursorById( KernelStatement statement, long nodeId ) throws EntityNotFoundException
    {
        Cursor<NodeItem> node = nodeCursor( statement, nodeId );
        if ( !node.next() )
        {
            node.close();
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        return node;
    }

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
    public Cursor<RelationshipItem> relationshipCursorById( KernelStatement statement, long relationshipId )
            throws EntityNotFoundException
    {
        Cursor<RelationshipItem> relationship = relationshipCursor( statement, relationshipId );
        if ( !relationship.next() )
        {
            relationship.close();
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        return relationship;
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
        StorageStatement storeStatement = statement.getStoreStatement();
        return storeStatement.acquireIteratorNodeCursor(
                storeLayer.nodesGetForLabel( storeStatement, labelId ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return storeStatement.acquireIteratorNodeCursor( reader.seek( value ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return storeStatement.acquireIteratorNodeCursor( reader.scan() );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return storeStatement.acquireIteratorNodeCursor( reader.rangeSeekByPrefix( prefix ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return COMPARE_NUMBERS.isEmptyRange( lower, includeLower, upper, includeUpper ) ? Cursors.<NodeItem>empty() :
               storeStatement.acquireIteratorNodeCursor(
                       reader.rangeSeekByNumberInclusive( lower, upper ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( KernelStatement statement,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return storeStatement.acquireIteratorNodeCursor(
                reader.rangeSeekByString( lower, includeLower, upper, includeUpper ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement, IndexDescriptor index,
            String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        return storeStatement.acquireIteratorNodeCursor( reader.rangeSeekByPrefix( prefix ) );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexBrokenKernelException, IndexNotFoundKernelException
    {
        // TODO Filter this properly
        StorageStatement storeStatement = statement.getStoreStatement();
        IndexReader reader = storeStatement.getFreshIndexReader( index );
        PrimitiveLongIterator seekResult = PrimitiveLongCollections.resourceIterator( reader.seek( value ), reader );
        return storeStatement.acquireIteratorNodeCursor( seekResult );
    }

    // </Cursors>

    @Override
    public long nodeCreate( KernelStatement state )
    {
        long nodeId = storeLayer.reserveNode();
        state.txState().nodeDoCreate(nodeId);
        return nodeId;
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId )
            throws AutoIndexingKernelException, EntityNotFoundException, InvalidTransactionTypeKernelException
    {
        autoIndexing.nodes().entityRemoved( state.dataWriteOperations(), nodeId );
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            state.txState().nodeDoDelete( cursor.get().id() );
        }
    }

    @Override
    public int nodeDetachDelete( KernelStatement state, long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        nodeDelete( state, nodeId );
        return 0;
    }

    @Override
    public long relationshipCreate( KernelStatement state,
            int relationshipTypeId,
            long startNodeId,
            long endNodeId )
            throws EntityNotFoundException
    {
        try ( Cursor<NodeItem> startNode = nodeCursorById( state, startNodeId ) )
        {
            try ( Cursor<NodeItem> endNode = nodeCursorById( state, endNodeId ) )
            {
                long id = storeLayer.reserveRelationship();
                state.txState().relationshipDoCreate( id, relationshipTypeId, startNode.get().id(), endNode.get().id() );
                return id;
            }
        }
    }

    @Override
    public void relationshipDelete( final KernelStatement state, long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        try ( Cursor<RelationshipItem> cursor = relationshipCursorById( state, relationshipId ) )
        {
            RelationshipItem relationship = cursor.get();

            // NOTE: We implicitly delegate to neoStoreTransaction via txState.legacyState here. This is because that
            // call returns modified properties, which node manager uses to update legacy tx state. This will be cleaned up
            // once we've removed legacy tx state.
            autoIndexing.relationships().entityRemoved( state.dataWriteOperations(), relationshipId );
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
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        PrimitiveLongIterator iterator = storeLayer.nodesGetAll();
        return state.hasTxStateWithChanges() ? state.txState().augmentNodesGetAll( iterator ) : iterator;
    }

    @Override
    public RelationshipIterator relationshipsGetAll( KernelStatement state )
    {
        RelationshipIterator iterator = storeLayer.relationshipsGetAll();
        return state.hasTxStateWithChanges() ? state.txState().augmentRelationshipsGetAll( iterator ) : iterator;
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
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
                    IndexDescriptor descriptor = indexGetForLabelAndPropertyKey( state, labelId,
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
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
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
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        if ( state.hasTxStateWithChanges() )
        {
            PrimitiveLongIterator wLabelChanges =
                    state.txState().nodesWithLabelChanged( labelId ).augment(
                            storeLayer.nodesGetForLabel( state.getStoreStatement(), labelId ) );
            return state.txState().addedAndRemovedNodes().augmentWithRemovals( wLabelChanges );
        }

        return storeLayer.nodesGetForLabel( state.getStoreStatement(), labelId );
    }

    @Override
    public long nodesGetCount( KernelStatement state )
    {
        long base = storeLayer.nodesGetCount();
        return state.hasTxStateWithChanges() ? base + state.txState().addedAndRemovedNodes().delta() : base;
    }

    @Override
    public long relationshipsGetCount( KernelStatement state )
    {
        long base = storeLayer.relationshipsGetCount();
        return state.hasTxStateWithChanges() ? base + state.txState().addedAndRemovedRelationships().delta() : base;
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
            if ( state.hasTxStateWithChanges() &&
                 state.txState().constraintIndexDoUnRemove( index ) ) // ..., DROP, *CREATE*
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
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( KernelStatement state, int labelId,
            int propertyKeyId ) throws CreateConstraintFailureException
    {
        NodePropertyExistenceConstraint constraint = new NodePropertyExistenceConstraint( labelId, propertyKeyId );
        state.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            int relTypeId, int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        RelationshipPropertyExistenceConstraint constraint =
                new RelationshipPropertyExistenceConstraint( relTypeId, propertyKeyId );
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
    public IndexDescriptor indexGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
    {
        IndexDescriptor indexDescriptor = storeLayer.indexGetForLabelAndPropertyKey( labelId, propertyKey );

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
        Predicate<IndexDescriptor> predicate = item -> item.getPropertyKeyId() == propertyKey;
        return Iterators.filter( predicate, descriptorIterator );
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

    @Override
    public PopulationProgress indexGetPopulationProgress( KernelStatement state, IndexDescriptor descriptor ) throws
            IndexNotFoundKernelException
    {
        // If index is in our state, then return 0%
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor, state.txState().indexDiffSetsByLabel( descriptor.getLabelId() ) ) )
            {
                return PopulationProgress.NONE;
            }
            ReadableDiffSets<IndexDescriptor> changes =
                    state.txState().constraintIndexDiffSetsByLabel( descriptor.getLabelId() );
            if ( checkIndexState( descriptor, changes ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return storeLayer.indexGetPopulationProgress( descriptor );
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
                    .apply( storeLayer.uniquenessIndexesGetForLabel( labelId ) );
        }

        return storeLayer.uniquenessIndexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintIndexChanges()
                    .apply( storeLayer.uniquenessIndexesGetAll() );
        }

        return storeLayer.uniquenessIndexesGetAll();
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getFreshIndexReader( index );

        /* Here we have an intricate scenario where we need to return the PrimitiveLongIterator
         * since subsequent filtering will happen outside, but at the same time have the ability to
         * close the IndexReader when done iterating over the lookup result. This is because we get
         * a fresh reader that isn't associated with the current transaction and hence will not be
         * automatically closed. */
        PrimitiveLongResourceIterator committed = PrimitiveLongCollections.resourceIterator( reader.seek( value ), reader );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        PrimitiveLongIterator changesFiltered = filterIndexStateChangesForScanOrSeek( state, index, value,
                exactMatches );
        return single( PrimitiveLongCollections.resourceIterator( changesFiltered, committed ), NO_SUCH_NODE );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.seek( value );
        PrimitiveLongIterator exactMatches = filterExactIndexMatches( state, index, value, committed );
        return filterIndexStateChangesForScanOrSeek( state, index, value, exactMatches );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement state, IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper ) throws IndexNotFoundKernelException

    {
        StorageStatement storeStatement = state.getStoreStatement();
        PrimitiveLongIterator committed = COMPARE_NUMBERS.isEmptyRange( lower, includeLower, upper, includeUpper )
                ? PrimitiveLongCollections.emptyIterator()
                : storeStatement.getIndexReader( index ).rangeSeekByNumberInclusive( lower, upper );
        PrimitiveLongIterator exactMatches = filterExactRangeMatches( state, index, committed, lower, includeLower,
                upper, includeUpper );
        return filterIndexStateChangesForRangeSeekByNumber( state, index, lower, includeLower, upper, includeUpper,
                exactMatches );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement state, IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper ) throws IndexNotFoundKernelException

    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.rangeSeekByString( lower, includeLower, upper, includeUpper );
        return filterIndexStateChangesForRangeSeekByString( state, index, lower, includeLower, upper, includeUpper,
                committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state, IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.rangeSeekByPrefix( prefix );
        return filterIndexStateChangesForRangeSeekByPrefix( state, index, prefix, committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.scan();
        return filterIndexStateChangesForScanOrSeek( state, index, null, committed );
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        IndexReader reader = statement.getStoreStatement().getIndexReader( index );
        return reader.countIndexedNodes( nodeId, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan( KernelStatement state, IndexDescriptor index,
            String term )
            throws IndexNotFoundKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.containsString( term );
        return filterIndexStateChangesForScanOrSeek( state, index, null, committed );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan( KernelStatement state, IndexDescriptor index,
            String suffix ) throws IndexNotFoundKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongIterator committed = reader.endsWith( suffix );
        return filterIndexStateChangesForScanOrSeek( state, index, null, committed );
    }

    private PrimitiveLongIterator filterExactIndexMatches( final KernelStatement state, IndexDescriptor index,
            Object value, PrimitiveLongIterator committed )
    {
        return LookupFilter.exactIndexMatches( this, state, committed, index.getPropertyKeyId(), value );
    }

    private PrimitiveLongIterator filterExactRangeMatches( final KernelStatement state, IndexDescriptor index,
            PrimitiveLongIterator committed, Number lower, boolean includeLower, Number upper, boolean includeUpper )
    {
        return LookupFilter.exactRangeMatches( this, state, committed, index.getPropertyKeyId(), lower, includeLower,
                upper, includeUpper );
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
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            Property existingProperty;
            try ( Cursor<PropertyItem> properties = node.property( property.propertyKeyId() ) )
            {
                if ( !properties.next() )
                {
                    autoIndexing.nodes().propertyAdded( ops, nodeId, property );
                    existingProperty = Property.noProperty( property.propertyKeyId(), EntityType.NODE, node.id() );
                }
                else
                {
                    existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );
                    autoIndexing.nodes().propertyChanged( ops, nodeId, existingProperty, property );
                }
            }

            state.txState().nodeDoReplaceProperty( node.id(), existingProperty, property );

            DefinedProperty before = definedPropertyOrNull( existingProperty );
            indexesUpdateProperty( state, node, property.propertyKeyId(), before, property );

            return existingProperty;
        }
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            long relationshipId,
            DefinedProperty property )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<RelationshipItem> cursor = relationshipCursorById( state, relationshipId ) )
        {
            RelationshipItem relationship = cursor.get();
            Property existingProperty;
            try ( Cursor<PropertyItem> properties = relationship.property( property.propertyKeyId() ) )
            {
                if ( !properties.next() )
                {
                    autoIndexing.relationships().propertyAdded( ops, relationshipId, property );
                    existingProperty = Property.noProperty( property.propertyKeyId(), EntityType.RELATIONSHIP,
                            relationship.id() );
                }
                else
                {
                    existingProperty = Property.property( properties.get().propertyKeyId(), properties.get().value() );
                    autoIndexing.relationships().propertyChanged( ops, relationshipId, existingProperty, property );
                }
            }

            state.txState().relationshipDoReplaceProperty( relationship.id(), existingProperty, property );
            return existingProperty;
        }
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
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
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

                    autoIndexing.nodes().propertyRemoved( ops, nodeId, propertyKeyId );
                    state.txState().nodeDoRemoveProperty( node.id(), (DefinedProperty) existingProperty );

                    indexesUpdateProperty( state, node, propertyKeyId, (DefinedProperty) existingProperty, null );
                }
            }
            return existingProperty;
        }
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<RelationshipItem> cursor = relationshipCursorById( state, relationshipId ) )
        {
            RelationshipItem relationship = cursor.get();
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

                    autoIndexing.relationships().propertyRemoved( ops, relationshipId, propertyKeyId );
                    state.txState().relationshipDoRemoveProperty( relationship.id(),
                            (DefinedProperty) existingProperty );
                }
            }
            return existingProperty;
        }
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
        return Property.noGraphProperty( propertyKeyId );
    }

    private void indexesUpdateProperty( KernelStatement state, NodeItem node, int propertyKey, DefinedProperty before,
            DefinedProperty after )
    {
        try ( Cursor<LabelItem> labels = node.labels() )
        {
            while ( labels.next() )
            {
                LabelItem label = labels.get();
                indexUpdateProperty( state, node.id(), label.getAsInt(), propertyKey, before, after );
            }
        }
    }

    private void indexUpdateProperty( KernelStatement state, long nodeId, int labelId, int propertyKey,
            DefinedProperty before, DefinedProperty after )
    {
        IndexDescriptor descriptor = indexGetForLabelAndPropertyKey( state, labelId, propertyKey );
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

        return storeLayer.graphGetPropertyKeys();
    }

    @Override
    public boolean graphHasProperty( KernelStatement state, int propertyKeyId )
    {
        return graphGetProperty( state, propertyKeyId ) != null;
    }

    @Override
    public Object graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        Iterator<StorageProperty> properties = graphGetAllProperties( state );
        while ( properties.hasNext() )
        {
            DefinedProperty property = (DefinedProperty) properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property.value();
            }
        }
        return null;
    }

    private Iterator<StorageProperty> graphGetAllProperties( KernelStatement state )
    {
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().augmentGraphProperties( storeLayer.graphGetAllProperties() );
        }

        return storeLayer.graphGetAllProperties();
    }

    @Override
    public long countsForNode( final KernelStatement statement, int labelId )
    {
        long count = countsForNodeWithoutTxState( statement, labelId );
        if ( statement.hasTxStateWithChanges() )
        {
            CountsRecordState counts = new CountsRecordState();
            try
            {
                statement.txState().accept( new TransactionCountingStateVisitor( EMPTY, storeLayer,
                        statement.getStoreStatement(), statement.txState(), counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                }
            }
            catch ( ConstraintValidationKernelException | CreateConstraintFailureException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForNodeWithoutTxState( final KernelStatement statement, int labelId )
    {
        return storeLayer.countsForNode( labelId );
    }

    @Override
    public long countsForRelationship( KernelStatement statement, int startLabelId, int typeId, int endLabelId )
    {
        long count = countsForRelationshipWithoutTxState( statement, startLabelId, typeId, endLabelId );
        if ( statement.hasTxStateWithChanges() )
        {
            CountsRecordState counts = new CountsRecordState();
            try
            {
                statement.txState().accept( new TransactionCountingStateVisitor( EMPTY, storeLayer,
                        statement.getStoreStatement(), statement.txState(), counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.relationshipCount( startLabelId, typeId, endLabelId, newDoubleLongRegister() )
                            .readSecond();
                }
            }
            catch ( ConstraintValidationKernelException | CreateConstraintFailureException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForRelationshipWithoutTxState( KernelStatement statement, int startLabelId, int typeId,
            int endLabelId )
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

    @Override
    public DoubleLongRegister indexUpdatesAndSize( KernelStatement statement, IndexDescriptor index,
            DoubleLongRegister target )
    {
        return storeLayer.indexUpdatesAndSize( index, target );
    }

    @Override
    public DoubleLongRegister indexSample( KernelStatement statement, IndexDescriptor index, DoubleLongRegister target )
    {
        return storeLayer.indexSample( index, target );
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
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index, Predicate<SchemaRule.Kind> filter )
            throws SchemaRuleNotFoundException
    {
        return storeLayer.indexGetCommittedId( index, filter );
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
    public Iterator<Token> relationshipTypesGetAllTokens( Statement state )
    {
        return storeLayer.relationshipTypeGetAllTokens();
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

    @Override
    public int labelCount( KernelStatement statement )
    {
        return storeLayer.labelCount();
    }

    @Override
    public int propertyKeyCount( KernelStatement statement )
    {
        return storeLayer.propertyKeyCount();
    }

    @Override
    public int relationshipTypeCount( KernelStatement statement )
    {
        return storeLayer.relationshipTypeCount();
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
        statement.legacyIndexTxState().createIndex( IndexEntityType.Node, indexName, customConfig );
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
        statement.legacyIndexTxState().createIndex( IndexEntityType.Relationship, indexName, customConfig );
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

    @Override
    public boolean nodeExists( KernelStatement statement, long id )
    {
        if ( statement.hasTxStateWithChanges() )
        {
            TransactionState txState = statement.txState();
            if ( txState.nodeIsDeletedInThisTx( id ) )
            {
                return false;
            }
            else if ( txState.nodeIsAddedInThisTx( id ) )
            {
                return true;
            }
        }
        return storeLayer.nodeExists( id );
    }

    private static DefinedProperty definedPropertyOrNull( Property existingProperty )
    {
        return existingProperty instanceof DefinedProperty ? (DefinedProperty) existingProperty : null;
    }
}
