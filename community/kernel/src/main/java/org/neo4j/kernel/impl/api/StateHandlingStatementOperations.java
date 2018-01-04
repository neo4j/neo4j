/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.IndexTxStateUpdater;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.filter;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.function.Predicates.any;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.api.state.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.api.state.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;
import static org.neo4j.kernel.impl.util.Cursors.count;
import static org.neo4j.kernel.impl.util.Cursors.empty;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class StateHandlingStatementOperations implements
        KeyReadOperations,
        KeyWriteOperations,
        EntityOperations,
        SchemaReadOperations,
        SchemaWriteOperations,
        CountsOperations, ExplicitIndexReadOperations, ExplicitIndexWriteOperations
{
    private final StoreReadLayer storeLayer;
    private final AutoIndexing autoIndexing;
    private final ConstraintIndexCreator constraintIndexCreator;
    private final ExplicitIndexStore explicitIndexStore;
    private final IndexTxStateUpdater indexTxStateUpdater;

    public StateHandlingStatementOperations(
            StoreReadLayer storeLayer, AutoIndexing propertyTrackers,
            ConstraintIndexCreator constraintIndexCreator,
            ExplicitIndexStore explicitIndexStore )
    {
        this.storeLayer = storeLayer;
        this.autoIndexing = propertyTrackers;
        this.constraintIndexCreator = constraintIndexCreator;
        this.explicitIndexStore = explicitIndexStore;
        this.indexTxStateUpdater = new IndexTxStateUpdater( storeLayer, this );
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

    private Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId )
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

    private Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relationshipId )
    {
        Cursor<RelationshipItem> cursor =
                statement.getStoreStatement().acquireSingleRelationshipCursor( relationshipId );
        if ( statement.hasTxStateWithChanges() )
        {
            return statement.txState().augmentSingleRelationshipCursor( cursor, relationshipId );
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
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node,
            Direction direction )
    {
        Cursor<RelationshipItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer.nodeGetRelationships( statement.getStoreStatement(), node, direction );
        }
        if ( !statement.hasTxStateWithChanges() )
        {
            return cursor;
        }
        NodeState nodeState = statement.txState().getNodeState( node.id() );
        return statement.txState().augmentNodeRelationshipCursor( cursor, nodeState, direction );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes )
    {
        Cursor<RelationshipItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer.nodeGetRelationships( statement.getStoreStatement(), node, direction, any( relTypes ) );
        }
        if ( !statement.hasTxStateWithChanges() )
        {
            return cursor;
        }
        NodeState nodeState = statement.txState().getNodeState( node.id() );
        return statement.txState().augmentNodeRelationshipCursor( cursor, nodeState, direction, relTypes );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( KernelStatement statement, NodeItem node )
    {
        Cursor<PropertyItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer.nodeGetProperties( statement.getStoreStatement(), node, statement );
        }

        return statement.hasTxStateWithChanges()
               ? statement.txState().augmentPropertyCursor( cursor, statement.txState().getNodeState( node.id() ) )
               : cursor;
    }

    @Override
    public PrimitiveIntCollection nodeGetPropertyKeys( KernelStatement statement, NodeItem node )
    {
        PrimitiveIntStack keys = new PrimitiveIntStack();
        try ( Cursor<PropertyItem> properties = nodeGetProperties( statement, node ) )
        {
            while ( properties.next() )
            {
                keys.push( properties.get().propertyKeyId() );
            }
        }

        return keys;
    }

    @Override
    public Value nodeGetProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = nodeGetPropertyCursor( statement, node, propertyKeyId ) )
        {
            if ( cursor.next() )
            {
                return cursor.get().value();
            }
        }
        catch ( NotFoundException e )
        {
            return Values.NO_VALUE;
        }

        return Values.NO_VALUE;
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = nodeGetPropertyCursor( statement, node, propertyKeyId ) )
        {
            return cursor.next();
        }
    }

    private Cursor<PropertyItem> nodeGetPropertyCursor( KernelStatement statement, NodeItem node, int propertyKeyId )
    {

        Cursor<PropertyItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer.nodeGetProperty( statement.getStoreStatement(), node, propertyKeyId, statement );
        }

        return statement.hasTxStateWithChanges()
               ? statement.txState().augmentSinglePropertyCursor(
                       cursor, statement.txState().getNodeState( node.id() ), propertyKeyId )
               : cursor;
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( KernelStatement statement, RelationshipItem relationship )
    {
        Cursor<PropertyItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().relationshipIsAddedInThisTx( relationship.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer.relationshipGetProperties( statement.getStoreStatement(), relationship, statement );
        }

        return statement.hasTxStateWithChanges()
               ? statement.txState()
                       .augmentPropertyCursor( cursor, statement.txState().getRelationshipState( relationship.id() ) )
               : cursor;
    }

    @Override
    public PrimitiveIntCollection relationshipGetPropertyKeys( KernelStatement statement,
            RelationshipItem relationship )
    {
        PrimitiveIntStack keys = new PrimitiveIntStack();
        try ( Cursor<PropertyItem> properties = relationshipGetProperties( statement, relationship ) )
        {
            while ( properties.next() )
            {
                keys.push( properties.get().propertyKeyId() );
            }
        }

        return keys;
    }

    @Override
    public Value relationshipGetProperty( KernelStatement statement, RelationshipItem relationship, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = relationshipGetPropertyCursor( statement, relationship, propertyKeyId ) )
        {
            if ( cursor.next() )
            {
                return cursor.get().value();
            }
        }
        catch ( NotFoundException e )
        {
            return Values.NO_VALUE;
        }

        return Values.NO_VALUE;
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement statement, RelationshipItem relationship, int propertyKeyId )
    {
        try ( Cursor<PropertyItem> cursor = relationshipGetPropertyCursor( statement, relationship, propertyKeyId ) )
        {
            return cursor.next();
        }
    }

    private Cursor<PropertyItem> relationshipGetPropertyCursor( KernelStatement statement,
            RelationshipItem relationship, int propertyKeyId )
    {
        Cursor<PropertyItem> cursor;
        if ( statement.hasTxStateWithChanges() && statement.txState().relationshipIsAddedInThisTx( relationship.id() ) )
        {
            cursor = empty();
        }
        else
        {
            cursor = storeLayer
                    .relationshipGetProperty( statement.getStoreStatement(), relationship, propertyKeyId, statement );
        }

        return statement.hasTxStateWithChanges()
               ? statement.txState().augmentSinglePropertyCursor( cursor, statement.txState()
                        .getRelationshipState( relationship.id() ), propertyKeyId )
               : cursor;
    }

    // </Cursors>

    @Override
    public long nodeCreate( KernelStatement state )
    {
        long nodeId = state.getStoreStatement().reserveNode();
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
                long id = state.getStoreStatement().reserveRelationship();
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
            if ( node.hasLabel( labelId ) )
            {
                // Label is already in state or in store, no-op
                return false;
            }

            state.txState().nodeDoAddLabel( labelId, node.id() );

            indexTxStateUpdater.onLabelChange( state, labelId, node, ADDED_LABEL );

            return true;
        }
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            if ( !node.hasLabel( labelId ) )
            {
                // Label does not exist in state or in store, no-op
                return false;
            }

            state.txState().nodeDoRemoveLabel( labelId, node.id() );

            indexTxStateUpdater.onLabelChange( state, labelId, node, REMOVED_LABEL );

            return true;
        }
    }

    @Override
    public PrimitiveLongResourceIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        PrimitiveLongResourceIterator committed = storeLayer.nodesGetForLabel( state.getStoreStatement(), labelId );
        if ( state.hasTxStateWithChanges() )
        {
            PrimitiveLongIterator wLabelChanges = state.txState().nodesWithLabelChanged( labelId ).augment( committed );
            return PrimitiveLongCollections
                    .resourceIterator( state.txState().addedAndRemovedNodes().augmentWithRemovals( wLabelChanges ), committed );
        }

        return committed;
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
    public IndexDescriptor indexCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
    {
        IndexDescriptor indexDescriptor = IndexDescriptorFactory.forSchema( descriptor );
        state.txState().indexRuleDoAdd( indexDescriptor );
        return indexDescriptor;
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().indexDoDrop( descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.txState().indexDoDrop( descriptor );
    }

    private void indexBackedConstraintCreate( KernelStatement state, IndexBackedConstraintDescriptor constraint )
            throws CreateConstraintFailureException
    {
        LabelSchemaDescriptor descriptor = constraint.schema();
        try
        {
            if ( state.hasTxStateWithChanges() &&
                 state.txState().indexDoUnRemove( constraint.ownedIndexDescriptor() ) ) // ..., DROP, *CREATE*
            { // creation is undoing a drop
                if ( !state.txState().constraintDoUnRemove( constraint ) ) // CREATE, ..., DROP, *CREATE*
                { // ... the drop we are undoing did itself undo a prior create...
                    state.txState().constraintDoAdd(
                            constraint, state.txState().indexCreatedForConstraint( constraint ) );
                }
            }
            else // *CREATE*
            { // create from scratch
                Iterator<ConstraintDescriptor> it = storeLayer.constraintsGetForSchema( descriptor );
                while ( it.hasNext() )
                {
                    if ( it.next().equals( constraint ) )
                    {
                        return;
                    }
                }
                long indexId = constraintIndexCreator.createUniquenessConstraintIndex( state, this, descriptor );
                if ( !constraintExists( state, constraint ) )
                {
                    // This looks weird, but since we release the label lock while awaiting population of the index
                    // backing this constraint there can be someone else getting ahead of us, creating this exact constraint
                    // before we do, so now getting out here under the lock we must check again and if it exists
                    // we must at this point consider this an idempotent operation because we verified earlier
                    // that it didn't exist and went on to create it.
                    state.txState().constraintDoAdd( constraint, indexId );
                }
            }
            return;
        }
        catch ( UniquePropertyValueValidationException |
                DropIndexFailureException |
                TransactionFailureException |
                AlreadyConstrainedException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
    }

    @Override
    public NodeKeyConstraintDescriptor nodeKeyConstraintCreate( KernelStatement state,
            LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        NodeKeyConstraintDescriptor constraint = ConstraintDescriptorFactory.nodeKeyForSchema( descriptor );
        indexBackedConstraintCreate( state, constraint );
        return constraint;
    }

    @Override
    public UniquenessConstraintDescriptor uniquePropertyConstraintCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        indexBackedConstraintCreate( state, constraint );
        return constraint;
    }

    @Override
    public NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate(
            KernelStatement state,
            LabelSchemaDescriptor descriptor
    ) throws CreateConstraintFailureException
    {
        NodeExistenceConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor );
        state.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            KernelStatement state,
            RelationTypeSchemaDescriptor descriptor
    ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        RelExistenceConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor );
        state.txState().constraintDoAdd( constraint );
        return constraint;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( KernelStatement state, SchemaDescriptor descriptor )
    {
        Iterator<ConstraintDescriptor> constraints = storeLayer.constraintsGetForSchema( descriptor );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForSchema( descriptor ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public boolean constraintExists( KernelStatement state, ConstraintDescriptor descriptor )
    {
        boolean inStore = storeLayer.constraintExists( descriptor );
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<ConstraintDescriptor> diffSet =
                    state.txState().constraintsChangesForSchema( descriptor.schema() );
            return diffSet.isAdded( descriptor ) || (inStore && !diffSet.isRemoved( descriptor ));
        }
        return inStore;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        Iterator<ConstraintDescriptor> constraints = storeLayer.constraintsGetForLabel( labelId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForLabel( labelId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( KernelStatement state,
            int typeId )
    {
        Iterator<ConstraintDescriptor> constraints = storeLayer.constraintsGetForRelationshipType( typeId );
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll( KernelStatement state )
    {
        Iterator<ConstraintDescriptor> constraints = storeLayer.constraintsGetAll();
        if ( state.hasTxStateWithChanges() )
        {
            return state.txState().constraintsChanges().apply( constraints );
        }
        return constraints;
    }

    @Override
    public void constraintDrop( KernelStatement state, ConstraintDescriptor constraint )
    {
        state.txState().constraintDoDrop( constraint );
    }

    @Override
    public IndexDescriptor indexGetForSchema( KernelStatement state, LabelSchemaDescriptor descriptor )
    {
        IndexDescriptor indexDescriptor = storeLayer.indexGetForSchema( descriptor );
        Iterator<IndexDescriptor> rules = iterator( indexDescriptor );
        if ( state.hasTxStateWithChanges() )
        {
            rules = filter(
                    SchemaDescriptor.equalTo( descriptor ),
                    state.txState().indexDiffSetsByLabel( descriptor.getLabelId() ).apply( rules ) );
        }
        return singleOrNull( rules );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    state.txState().indexDiffSetsByLabel( descriptor.schema().getLabelId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return storeLayer.indexGetState( descriptor );
    }

    @Override
    public SchemaIndexProvider.Descriptor indexGetProviderDescriptor( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    state.txState().indexDiffSetsByLabel( descriptor.schema().getLabelId() ) ) )
            {
                return SchemaIndexProvider.UNDECIDED;
            }
        }
        return storeLayer.indexGetProviderDescriptor( descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( KernelStatement state, IndexDescriptor descriptor ) throws
            IndexNotFoundKernelException
    {
        // If index is in our state, then return 0%
        if ( state.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    state.txState().indexDiffSetsByLabel( descriptor.schema().getLabelId() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return storeLayer.indexGetPopulationProgress( descriptor.schema() );
    }

    private boolean checkIndexState( IndexDescriptor index, ReadableDiffSets<IndexDescriptor> diffSet )
            throws IndexNotFoundKernelException
    {
        if ( diffSet.isAdded( index ) )
        {
            return true;
        }
        if ( diffSet.isRemoved( index ) )
        {
            throw new IndexNotFoundKernelException( format( "Index on %s has been dropped in this transaction.",
                    index.userDescription( SchemaUtil.idTokenNameLookup ) ) );
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
    public long nodeGetFromUniqueIndexSeek(
            KernelStatement state, IndexDescriptor index, IndexQuery.ExactPredicate... query )
            throws IndexNotFoundKernelException, IndexBrokenKernelException, IndexNotApplicableKernelException
    {
        IndexReader reader = state.getStoreStatement().getFreshIndexReader( index );

        /* Here we have an intricate scenario where we need to return the PrimitiveLongIterator
         * since subsequent filtering will happen outside, but at the same time have the ability to
         * close the IndexReader when done iterating over the lookup result. This is because we get
         * a fresh reader that isn't associated with the current transaction and hence will not be
         * automatically closed. */
        PrimitiveLongResourceIterator committed = resourceIterator( reader.query( query ), reader );
        PrimitiveLongIterator exactMatches = reader.hasFullNumberPrecision( query )
                ? committed : LookupFilter.exactIndexMatches( this, state, committed, query );
        PrimitiveLongIterator changesFiltered =
                filterIndexStateChangesForSeek( state, exactMatches, index, IndexQuery.asValueTuple( query ) );
        return single( resourceIterator( changesFiltered, committed ), NO_SUCH_NODE );
    }

    @Override
    public PrimitiveLongResourceIterator indexQuery( KernelStatement state, IndexDescriptor index, IndexQuery... predicates )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        StorageStatement storeStatement = state.getStoreStatement();
        IndexReader reader = storeStatement.getIndexReader( index );
        PrimitiveLongResourceIterator committed = reader.query( predicates );
        PrimitiveLongIterator exactMatches = reader.hasFullNumberPrecision( predicates )
                ? committed : LookupFilter.exactIndexMatches( this, state, committed, predicates );

        PrimitiveLongIterator result;
        IndexQuery firstPredicate = predicates[0];
        switch ( firstPredicate.type() )
        {
        case exact:
            IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( predicates );
            result = filterIndexStateChangesForSeek( state, exactMatches, index, IndexQuery.asValueTuple( exactPreds ) );
            break;

        case stringSuffix:
        case stringContains:
        case exists:
            result = filterIndexStateChangesForScan( state, exactMatches, index );
            break;

        case rangeNumeric:
            assertSinglePredicate( predicates );
            IndexQuery.NumberRangePredicate numPred = (IndexQuery.NumberRangePredicate) firstPredicate;
            result = filterIndexStateChangesForRangeSeekByNumber( state, index, numPred.from(),
                    numPred.fromInclusive(), numPred.to(), numPred.toInclusive(), exactMatches );
            break;

        case rangeString:
        {
            assertSinglePredicate( predicates );
            IndexQuery.StringRangePredicate strPred = (IndexQuery.StringRangePredicate) firstPredicate;
            result = filterIndexStateChangesForRangeSeekByString(
                    state, index, strPred.from(), strPred.fromInclusive(), strPred.to(),
                    strPred.toInclusive(), committed );
            break;
        }
        case stringPrefix:
        {
            assertSinglePredicate( predicates );
            IndexQuery.StringPrefixPredicate strPred = (IndexQuery.StringPrefixPredicate) firstPredicate;
            result = filterIndexStateChangesForRangeSeekByPrefix( state, index, strPred.prefix(), committed );
            break;
        }
        default:
            throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( predicates ) );
        }
        return PrimitiveLongCollections.resourceIterator( result, committed );
    }

    public static IndexQuery.ExactPredicate[] assertOnlyExactPredicates( IndexQuery[] predicates )
    {
        IndexQuery.ExactPredicate[] exactPredicates;
        if ( predicates.getClass() == IndexQuery.ExactPredicate[].class )
        {
            exactPredicates = (IndexQuery.ExactPredicate[]) predicates;
        }
        else
        {
            exactPredicates = new IndexQuery.ExactPredicate[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                if ( predicates[i] instanceof IndexQuery.ExactPredicate )
                {
                    exactPredicates[i] = (IndexQuery.ExactPredicate) predicates[i];
                }
                else
                {
                    // TODO: what to throw?
                    throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( predicates ) );
                }
            }
        }
        return exactPredicates;
    }

    private void assertSinglePredicate( IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            // TODO: what to throw?
            throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( predicates ) );
        }
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Value value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        IndexReader reader = statement.getStoreStatement().getIndexReader( index );
        return reader.countIndexedNodes( nodeId, value );
    }

    private PrimitiveLongIterator filterIndexStateChangesForScan(
            KernelStatement state, PrimitiveLongIterator nodeIds, IndexDescriptor index )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges =
                    state.txState().indexUpdatesForScan( index );
            PrimitiveLongDiffSets nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChanges.augment( nodeIds ) );
        }
        return nodeIds;
    }

    private PrimitiveLongIterator filterIndexStateChangesForSeek(
            KernelStatement state, PrimitiveLongIterator nodeIds, IndexDescriptor index,
            ValueTuple propertyValues )
    {
        if ( state.hasTxStateWithChanges() )
        {
            ReadableDiffSets<Long> labelPropertyChanges =
                    state.txState().indexUpdatesForSeek( index, propertyValues );
            PrimitiveLongDiffSets nodes = state.txState().addedAndRemovedNodes();

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
                    state.txState().indexUpdatesForRangeSeekByNumber(
                            index, lower, includeLower, upper, includeUpper );
            PrimitiveLongDiffSets nodes = state.txState().addedAndRemovedNodes();

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
                    state.txState().indexUpdatesForRangeSeekByString(
                            index, lower, includeLower, upper, includeUpper );
            PrimitiveLongDiffSets nodes = state.txState().addedAndRemovedNodes();

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
            PrimitiveLongDiffSets nodes = state.txState().addedAndRemovedNodes();

            // Apply to actual index lookup
            return nodes.augmentWithRemovals( labelPropertyChangesForPrefix.augment( nodeIds ) );
        }
        return nodeIds;
    }

    @Override
    public Value nodeSetProperty( KernelStatement state, long nodeId, int propertyKeyId, Value value )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            Value existingValue = Values.NO_VALUE;
            try ( Cursor<PropertyItem> properties = nodeGetPropertyCursor( state, node, propertyKeyId ) )
            {
                if ( !properties.next() )
                {
                    autoIndexing.nodes().propertyAdded( ops, nodeId, propertyKeyId, value );
                }
                else
                {
                    existingValue = properties.get().value();
                    autoIndexing.nodes().propertyChanged( ops, nodeId, propertyKeyId, existingValue, value );
                }
            }

            if ( existingValue == Values.NO_VALUE )
            {
                state.txState().nodeDoAddProperty( node.id(), propertyKeyId, value );
                indexTxStateUpdater.onPropertyAdd( state, node, propertyKeyId, value );
                return Values.NO_VALUE;
            }
            else
            {
                if ( !value.equals( existingValue ) )
                {
                    state.txState().nodeDoChangeProperty( node.id(), propertyKeyId, existingValue, value );
                    indexTxStateUpdater.onPropertyChange( state, node, propertyKeyId, existingValue, value );
                }
                return existingValue;
            }
        }
    }

    @Override
    public Value relationshipSetProperty( KernelStatement state, long relationshipId, int propertyKeyId, Value value )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<RelationshipItem> cursor = relationshipCursorById( state, relationshipId ) )
        {
            RelationshipItem relationship = cursor.get();
            Value existingValue = Values.NO_VALUE;
            try ( Cursor<PropertyItem> properties = relationshipGetPropertyCursor( state, relationship, propertyKeyId ) )
            {
                if ( !properties.next() )
                {
                    autoIndexing.relationships().propertyAdded( ops, relationshipId, propertyKeyId, value );
                }
                else
                {
                    existingValue = properties.get().value();
                    autoIndexing.relationships().propertyChanged(
                            ops, relationshipId, propertyKeyId, existingValue, value );
                }
            }
            if ( !value.equals( existingValue ) )
            {
                state.txState().relationshipDoReplaceProperty(
                        relationship.id(), propertyKeyId, existingValue, value );
            }
            return existingValue;
        }
    }

    @Override
    public Value graphSetProperty( KernelStatement state, int propertyKeyId, Value value )
    {
        Value existingValue = graphGetProperty( state, propertyKeyId );

        if ( !value.equals( existingValue ) )
        {
            state.txState().graphDoReplaceProperty( propertyKeyId, existingValue, value );
        }

        return existingValue;
    }

    @Override
    public Value nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            Value existingValue = Values.NO_VALUE;
            try ( Cursor<PropertyItem> properties = nodeGetPropertyCursor( state, node, propertyKeyId ) )
            {
                if ( properties.next() )
                {
                    existingValue = properties.get().value();

                    autoIndexing.nodes().propertyRemoved( ops, nodeId, propertyKeyId );
                    state.txState().nodeDoRemoveProperty( node.id(), propertyKeyId, existingValue );

                    indexTxStateUpdater.onPropertyRemove( state, node, propertyKeyId, existingValue );
                }
            }
            return existingValue;
        }
    }

    @Override
    public Value relationshipRemoveProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        DataWriteOperations ops = state.dataWriteOperations();
        try ( Cursor<RelationshipItem> cursor = relationshipCursorById( state, relationshipId ) )
        {
            RelationshipItem relationship = cursor.get();
            Value existingValue = Values.NO_VALUE;
            try ( Cursor<PropertyItem> properties = relationshipGetPropertyCursor( state, relationship,
                    propertyKeyId ) )
            {
                if ( properties.next() )
                {
                    existingValue = properties.get().value();

                    autoIndexing.relationships().propertyRemoved( ops, relationshipId, propertyKeyId );
                    state.txState()
                            .relationshipDoRemoveProperty( relationship.id(), propertyKeyId, existingValue );
                }
            }
            return existingValue;
        }
    }

    @Override
    public Value graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        Value existingValue = graphGetProperty( state, propertyKeyId );
        if ( existingValue != Values.NO_VALUE )
        {
            state.txState().graphDoRemoveProperty( propertyKeyId, existingValue );
        }
        return existingValue;
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
        return graphGetProperty( state, propertyKeyId ) != Values.NO_VALUE;
    }

    @Override
    public Value graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        Iterator<StorageProperty> properties = graphGetAllProperties( state );
        while ( properties.hasNext() )
        {
            StorageProperty property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property.value();
            }
        }
        return Values.NO_VALUE;
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
            catch ( ConstraintValidationException | CreateConstraintFailureException e )
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
            catch ( ConstraintValidationException | CreateConstraintFailureException e )
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
        return storeLayer.indexSize( descriptor.schema() );
    }

    @Override
    public double indexUniqueValuesPercentage( KernelStatement statement, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexUniqueValuesPercentage( descriptor.schema() );
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( KernelStatement statement, IndexDescriptor index,
            DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return storeLayer.indexUpdatesAndSize( index.schema(), target );
    }

    @Override
    public DoubleLongRegister indexSample( KernelStatement statement, IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexSample( index.schema(), target );
    }

    //
    // Methods that delegate directly to storage
    //

    @Override
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index )
    {
        return storeLayer.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        return storeLayer.indexGetCommittedId( index );
    }

    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return storeLayer.indexGetFailure( descriptor.schema() );
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

    // <Explicit index>
    @Override
    public boolean nodeExplicitIndexExists( KernelStatement statement, String indexName, Map<String,String> customConfiguration )
    {
        return statement.explicitIndexTxState().checkIndexExistence( IndexEntityType.Node, indexName, customConfiguration );
    }

    @Override
    public boolean relationshipExplicitIndexExists( KernelStatement statement, String indexName, Map<String,String> customConfiguration )
    {
        return statement.explicitIndexTxState().checkIndexExistence( IndexEntityType.Relationship, indexName, customConfiguration );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexGet( KernelStatement statement, String indexName, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        return statement.explicitIndexTxState().nodeChanges( indexName ).get( key, value );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexQuery( KernelStatement statement, String indexName, String key,
            Object queryOrQueryObject ) throws ExplicitIndexNotFoundKernelException
    {
        return statement.explicitIndexTxState().nodeChanges( indexName ).query( key, queryOrQueryObject );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexQuery( KernelStatement statement,
            String indexName,
            Object queryOrQueryObject )
            throws ExplicitIndexNotFoundKernelException
    {
        return statement.explicitIndexTxState().nodeChanges( indexName ).query( queryOrQueryObject );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexGet( KernelStatement statement, String indexName, String key,
            Object value, long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        ExplicitIndex index = statement.explicitIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.get( key, value, startNode, endNode );
        }
        return index.get( key, value );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexQuery( KernelStatement statement, String indexName, String key,
            Object queryOrQueryObject, long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        ExplicitIndex index = statement.explicitIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.query( key, queryOrQueryObject, startNode, endNode );
        }
        return index.query( key, queryOrQueryObject );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexQuery( KernelStatement statement, String indexName,
            Object queryOrQueryObject, long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        ExplicitIndex index = statement.explicitIndexTxState().relationshipChanges( indexName );
        if ( startNode != -1 || endNode != -1 )
        {
            return index.query( queryOrQueryObject, startNode, endNode );
        }
        return index.query( queryOrQueryObject );
    }

    @Override
    public void nodeExplicitIndexCreateLazily( KernelStatement statement, String indexName,
            Map<String, String> customConfig )
    {
        explicitIndexStore.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    @Override
    public void nodeExplicitIndexCreate( KernelStatement statement, String indexName, Map<String, String> customConfig )
    {
        statement.explicitIndexTxState().createIndex( IndexEntityType.Node, indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreateLazily( KernelStatement statement, String indexName,
            Map<String, String> customConfig )
    {
        explicitIndexStore.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreate( KernelStatement statement,
            String indexName,
            Map<String, String> customConfig )
    {
        statement.explicitIndexTxState().createIndex( IndexEntityType.Relationship, indexName, customConfig );
    }

    @Override
    public void nodeAddToExplicitIndex( KernelStatement statement, String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().nodeChanges( indexName ).addNode( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node, String key,
            Object value ) throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().nodeChanges( indexName ).remove( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().nodeChanges( indexName ).remove( node, key );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().nodeChanges( indexName ).remove( node );
    }

    @Override
    public void relationshipAddToExplicitIndex( final KernelStatement statement, final String indexName,
            final long relationship, final String key, final Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        relationshipVisit( statement, relationship,
                ( relId, type, startNode, endNode ) -> statement.explicitIndexTxState().relationshipChanges( indexName ).addRelationship(
                        relationship, key, value, startNode, endNode ) );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( final KernelStatement statement,
            final String indexName,
            long relationship,
            final String key,
            final Object value ) throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        try
        {
            relationshipVisit( statement, relationship, ( relId, type, startNode, endNode ) ->
                    statement.explicitIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, key, value, startNode, endNode ) );
        }
        catch ( EntityNotFoundException e )
        {   // Apparently this is OK
        }
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( final KernelStatement statement,
            final String indexName,
            long relationship,
            final String key ) throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        try
        {
            relationshipVisit( statement, relationship, ( relId, type, startNode, endNode ) ->
                    statement.explicitIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, key, startNode, endNode ) );
        }
        catch ( EntityNotFoundException e )
        {   // Apparently this is OK
        }
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( final KernelStatement statement,
            final String indexName,
            long relationship )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        try
        {
            relationshipVisit( statement, relationship, ( relId, type, startNode, endNode ) ->
                    statement.explicitIndexTxState().relationshipChanges( indexName ).removeRelationship(
                            relId, startNode, endNode ) );
        }
        catch ( EntityNotFoundException e )
        {
            // This is a special case which is still OK. This method is called lazily where deleted relationships
            // that still are referenced by an explicit index will be added for removal in this transaction.
            // Ideally we'd want to include start/end node too, but we can't since the relationship doesn't exist.
            // So we do the "normal" remove call on the explicit index transaction changes. The downside is that
            // Some queries on this transaction state that include start/end nodes might produce invalid results.
            statement.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship );
        }
    }

    @Override
    public void nodeExplicitIndexDrop( KernelStatement statement,
            String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().nodeChanges( indexName ).drop();
        statement.explicitIndexTxState().deleteIndex( IndexEntityType.Node, indexName );
    }

    @Override
    public void relationshipExplicitIndexDrop( KernelStatement statement, String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.explicitIndexTxState().relationshipChanges( indexName ).drop();
        statement.explicitIndexTxState().deleteIndex( IndexEntityType.Relationship, indexName );
    }

    @Override
    public String nodeExplicitIndexSetConfiguration( KernelStatement statement,
            String indexName,
            String key,
            String value )
            throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.setNodeIndexConfiguration( indexName, key, value );
    }

    @Override
    public String relationshipExplicitIndexSetConfiguration( KernelStatement statement, String indexName, String key,
            String value ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.setRelationshipIndexConfiguration( indexName, key, value );
    }

    @Override
    public String nodeExplicitIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.removeNodeIndexConfiguration( indexName, key );
    }

    @Override
    public String relationshipExplicitIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.removeRelationshipIndexConfiguration( indexName, key );
    }

    @Override
    public Map<String, String> nodeExplicitIndexGetConfiguration( KernelStatement statement, String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.getNodeIndexConfiguration( indexName );
    }

    @Override
    public Map<String, String> relationshipExplicitIndexGetConfiguration( KernelStatement statement, String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.getRelationshipIndexConfiguration( indexName );
    }

    @Override
    public String[] nodeExplicitIndexesGetAll( KernelStatement statement )
    {
        return explicitIndexStore.getAllNodeIndexNames();
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll( KernelStatement statement )
    {
        return explicitIndexStore.getAllRelationshipIndexNames();
    }
    // </Explicit index>

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

    @Override
    public PrimitiveIntSet relationshipTypes( KernelStatement statement, NodeItem node )
    {
        if ( statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() ) )
        {
            return statement.txState().getNodeState( node.id() ).relationshipTypes();
        }

        // Read types in the current transaction
        PrimitiveIntSet types = statement.hasTxStateWithChanges()
                                ? statement.txState().getNodeState( node.id() ).relationshipTypes()
                                : Primitive.intSet();

        // Augment with types stored on disk, minus any types where all rels of that type are deleted
        // in current tx.
        types.addAll( filter( storeLayer.relationshipTypes( statement.getStoreStatement(), node ).iterator(),
                current -> !types.contains( current ) && degree( statement, node, Direction.BOTH, current ) > 0 ) );

        return types;
    }

    @Override
    public int degree( KernelStatement statement, NodeItem node, Direction direction )
    {
        int degree = statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() )
                     ? 0
                     : computeDegree( statement, node, direction, null );

        return statement.hasTxStateWithChanges()
                ? statement.txState().getNodeState( node.id() ).augmentDegree( direction, degree )
                : degree;
    }

    @Override
    public int degree( KernelStatement statement, NodeItem node, Direction direction, int relType )
    {
        int degree = statement.hasTxStateWithChanges() && statement.txState().nodeIsAddedInThisTx( node.id() )
                     ? 0
                     : computeDegree( statement, node, direction, relType );

        return statement.hasTxStateWithChanges()
               ? statement.txState().getNodeState( node.id() ).augmentDegree( direction, degree, relType )
               : degree;
    }

    private int computeDegree( KernelStatement statement, NodeItem node,  Direction direction, Integer relType )
    {
        StorageStatement storeStatement = statement.getStoreStatement();
        if ( node.isDense() )
        {
            return storeLayer.degreeRelationshipsInGroup( storeStatement, node.id(), node.nextGroupId(),
                    direction, relType );
        }
        else
        {
            return count( relType == null
                          ? storeLayer.nodeGetRelationships( storeStatement, node, direction )
                          : storeLayer.nodeGetRelationships( storeStatement, node, direction, t -> t == relType ) );
        }
    }
}
