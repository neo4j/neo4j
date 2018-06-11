/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleRelationshipCursor;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.txstate.DiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * This class contains transaction-local changes to the graph. These changes can then be used to augment reads from the
 * committed state of the database (to make the local changes appear in local transaction read operations). At commit
 * time a visitor is sent into this class to convert the end result of the tx changes into a physical changeset.
 * <p>
 * See {@link org.neo4j.kernel.impl.api.KernelTransactionImplementation} for how this happens.
 * <p>
 * This class is very large, as it has been used as a gathering point to consolidate all transaction state knowledge
 * into one component. Now that that work is done, this class should be refactored to increase transparency in how it
 * works.
 */
public class TxState implements TransactionState, RelationshipVisitor.Home
{
    /**
     * This factory must be used only for creating collections representing internal state that doesn't leak outside this class.
     */
    private final CollectionsFactory collectionsFactory;

    private MutableLongObjectMap<MutableLongDiffSets> labelStatesMap;
    private MutableLongObjectMap<NodeStateImpl> nodeStatesMap;
    private MutableLongObjectMap<RelationshipStateImpl> relationshipStatesMap;

    private MutableLongObjectMap<String> createdLabelTokens;
    private MutableLongObjectMap<String> createdPropertyKeyTokens;
    private MutableLongObjectMap<String> createdRelationshipTypeTokens;

    private GraphState graphState;
    private DiffSets<IndexDescriptor> indexChanges;
    private DiffSets<ConstraintDescriptor> constraintsChanges;

    private RemovalsCountingDiffSets nodes;
    private RemovalsCountingDiffSets relationships;

    private MutableObjectLongMap<IndexBackedConstraintDescriptor> createdConstraintIndexesByConstraint;

    private Map<SchemaDescriptor, Map<ValueTuple, MutableLongDiffSets>> indexUpdates;

    private InstanceCache<TxSingleNodeCursor> singleNodeCursor;
    private InstanceCache<TxSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<TxAllPropertyCursor> propertyCursor;

    private boolean hasChanges;
    private boolean hasDataChanges;

    public TxState()
    {
        this( OnHeapCollectionsFactory.INSTANCE );
    }

    public TxState( CollectionsFactory collectionsFactory )
    {
        this.collectionsFactory = collectionsFactory;
        singleNodeCursor = new InstanceCache<TxSingleNodeCursor>()
        {
            @Override
            protected TxSingleNodeCursor create()
            {
                return new TxSingleNodeCursor( TxState.this, this );
            }
        };
        propertyCursor = new InstanceCache<TxAllPropertyCursor>()
        {
            @Override
            protected TxAllPropertyCursor create()
            {
                return new TxAllPropertyCursor( (Consumer) this );
            }
        };
        singleRelationshipCursor = new InstanceCache<TxSingleRelationshipCursor>()
        {
            @Override
            protected TxSingleRelationshipCursor create()
            {
                return new TxSingleRelationshipCursor( TxState.this, this );
            }
        };
    }

    @Override
    public void accept( final TxStateVisitor visitor )
            throws ConstraintValidationException, CreateConstraintFailureException
    {
        if ( nodes != null )
        {
            nodes.getAdded().each( visitor::visitCreatedNode );
        }

        if ( relationships != null )
        {
            final LongIterator added = relationships.getAdded().longIterator();
            while ( added.hasNext() )
            {
                final long relId = added.next();
                if ( !relationshipVisit( relId, visitor::visitCreatedRelationship ) )
                {
                    throw new IllegalStateException( "No RelationshipState for added relationship!" );
                }
            }
            relationships.getRemoved().forEach( visitor::visitDeletedRelationship );
        }

        if ( nodes != null )
        {
            nodes.getRemoved().each( visitor::visitDeletedNode );
        }

        for ( NodeState node : modifiedNodes() )
        {
            if ( node.hasPropertyChanges() )
            {
                visitor.visitNodePropertyChanges( node.getId(), node.addedProperties(), node.changedProperties(), node.removedProperties() );
            }

            final LongDiffSets labelDiffSets = node.labelDiffSets();
            if ( !labelDiffSets.isEmpty() )
            {
                visitor.visitNodeLabelChanges( node.getId(), labelDiffSets.getAdded(), labelDiffSets.getRemoved() );
            }
        }

        for ( RelationshipState rel : modifiedRelationships() )
        {
            visitor.visitRelPropertyChanges( rel.getId(), rel.addedProperties(), rel.changedProperties(), rel.removedProperties() );
        }

        if ( graphState != null )
        {
            visitor.visitGraphPropertyChanges( graphState.addedProperties(), graphState.changedProperties(), graphState.removedProperties() );
        }

        if ( indexChanges != null )
        {
            indexChanges.getAdded().forEach( visitor::visitAddedIndex );
            indexChanges.getRemoved().forEach( visitor::visitRemovedIndex );
        }

        if ( constraintsChanges != null )
        {
            constraintsChanges.accept( new ConstraintDiffSetsVisitor( visitor ) );
        }

        if ( createdLabelTokens != null )
        {
            createdLabelTokens.forEachKeyValue( visitor::visitCreatedLabelToken );
        }

        if ( createdPropertyKeyTokens != null )
        {
            createdPropertyKeyTokens.forEachKeyValue( visitor::visitCreatedPropertyKeyToken );
        }

        if ( createdRelationshipTypeTokens != null )
        {
            createdRelationshipTypeTokens.forEachKeyValue( visitor::visitCreatedRelationshipTypeToken );
        }
    }

    @Override
    public boolean hasChanges()
    {
        return hasChanges;
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
    {
        return nodeStatesMap == null ? Iterables.empty() : Iterables.cast( nodeStatesMap.values() );
    }

    private MutableLongDiffSets getOrCreateLabelStateNodeDiffSets( long labelId )
    {
        if ( labelStatesMap == null )
        {
            labelStatesMap = collectionsFactory.newLongObjectMap();
        }
        return labelStatesMap.getIfAbsentPut( labelId, MutableLongDiffSetsImpl::new );
    }

    private LongDiffSets getLabelStateNodeDiffSets( long labelId )
    {
        if ( labelStatesMap == null )
        {
            return LongDiffSets.EMPTY;
        }
        final LongDiffSets nodeDiffSets = labelStatesMap.get( labelId );
        return nodeDiffSets == null ? LongDiffSets.EMPTY : nodeDiffSets;
    }

    @Override
    public LongDiffSets nodeStateLabelDiffSets( long nodeId )
    {
        return getNodeState( nodeId ).labelDiffSets();
    }

    private MutableLongDiffSets getOrCreateNodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).getOrCreateLabelDiffSets();
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return nodes != null && nodes.isAdded( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return relationships != null && relationships.isAdded( relationshipId );
    }

    private void changed()
    {
        hasChanges = true;
    }

    private void dataChanged()
    {
        changed();
        hasDataChanges = true;
    }

    @Override
    public void nodeDoCreate( long id )
    {
        nodes().add( id );
        dataChanged();
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        nodes().remove( nodeId );

        if ( nodeStatesMap != null )
        {
            NodeStateImpl nodeState = nodeStatesMap.remove( nodeId );
            if ( nodeState != null )
            {
                final LongDiffSets diff = nodeState.labelDiffSets();
                diff.getAdded().each( label -> getOrCreateLabelStateNodeDiffSets( label ).remove( nodeId ) );
                nodeState.clearIndexDiffs( nodeId );
                nodeState.clear();
            }
        }
        dataChanged();
    }

    @Override
    public void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId )
    {
        relationships().add( id );

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).addRelationship( id, relationshipTypeId, Direction.INCOMING );
        }

        getOrCreateRelationshipState( id ).setMetaData( startNodeId, endNodeId, relationshipTypeId );

        dataChanged();
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return nodes != null && nodes.wasRemoved( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, int type, long startNodeId, long endNodeId )
    {
        relationships().remove( id );

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).removeRelationship( id, type, Direction.INCOMING );
        }

        if ( relationshipStatesMap != null )
        {
            RelationshipStateImpl removed = relationshipStatesMap.remove( id );
            if ( removed != null )
            {
                removed.clear();
            }
        }

        dataChanged();
    }

    @Override
    public void relationshipDoDeleteAddedInThisTx( long relationshipId )
    {
        getRelationshipState( relationshipId ).accept( this::relationshipDoDelete );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return relationships != null && relationships.wasRemoved( relationshipId );
    }

    @Override
    public void nodeDoAddProperty( long nodeId, int newPropertyKeyId, Value value )
    {
        NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
        nodeState.addProperty( newPropertyKeyId, value );
        dataChanged();
    }

    @Override
    public void nodeDoChangeProperty( long nodeId, int propertyKeyId, Value newValue )
    {
        getOrCreateNodeState( nodeId ).changeProperty( propertyKeyId, newValue );
        dataChanged();
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, int propertyKeyId, Value replacedValue,
            Value newValue )
    {
        if ( replacedValue != NO_VALUE )
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( propertyKeyId, newValue );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( propertyKeyId, newValue );
        }
        dataChanged();
    }

    @Override
    public void graphDoReplaceProperty( int propertyKeyId, Value replacedValue, Value newValue )
    {
        if ( replacedValue != NO_VALUE )
        {
            getOrCreateGraphState().changeProperty( propertyKeyId, newValue );
        }
        else
        {
            getOrCreateGraphState().addProperty( propertyKeyId, newValue );
        }
        dataChanged();
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, int propertyKeyId )
    {
        getOrCreateNodeState( nodeId ).removeProperty( propertyKeyId );
        dataChanged();
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, int propertyKeyId )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( propertyKeyId );
        dataChanged();
    }

    @Override
    public void graphDoRemoveProperty( int propertyKeyId )
    {
        getOrCreateGraphState().removeProperty( propertyKeyId );
        dataChanged();
    }

    @Override
    public void nodeDoAddLabel( long labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).add( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).add( labelId );
        dataChanged();
    }

    @Override
    public void nodeDoRemoveLabel( long labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).remove( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).remove( labelId );
        dataChanged();
    }

    @Override
    public void labelDoCreateForName( String labelName, long id )
    {
        if ( createdLabelTokens == null )
        {
            createdLabelTokens = collectionsFactory.newLongObjectMap();
        }
        createdLabelTokens.put( id, labelName );
        changed();
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        if ( createdPropertyKeyTokens == null )
        {
            createdPropertyKeyTokens = collectionsFactory.newLongObjectMap();
        }
        createdPropertyKeyTokens.put( id, propertyKeyName );
        changed();
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        if ( createdRelationshipTypeTokens == null )
        {
            createdRelationshipTypeTokens = collectionsFactory.newLongObjectMap();
        }
        createdRelationshipTypeTokens.put( id, labelName );
        changed();
    }

    @Override
    public NodeState getNodeState( long id )
    {
        if ( nodeStatesMap == null )
        {
            return NodeStateImpl.EMPTY;
        }
        final NodeState nodeState = nodeStatesMap.get( id );
        return nodeState == null ? NodeStateImpl.EMPTY : nodeState;
    }

    @Override
    public RelationshipState getRelationshipState( long id )
    {
        if ( relationshipStatesMap == null )
        {
            return RelationshipStateImpl.EMPTY;
        }
        final RelationshipStateImpl relationshipState = relationshipStatesMap.get( id );
        return relationshipState == null ? RelationshipStateImpl.EMPTY : relationshipState;
    }

    @Override
    public GraphState getGraphState( )
    {
        return graphState;
    }

    @Override
    public Cursor<NodeItem> augmentSingleNodeCursor( Cursor<NodeItem> cursor, long nodeId )
    {
        return hasChanges ? singleNodeCursor.get().init( cursor, nodeId ) : cursor;
    }

    @Override
    public Cursor<PropertyItem> augmentPropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState )
    {
        return propertyContainerState.hasPropertyChanges() ?
                propertyCursor.get().init( cursor, propertyContainerState ) : cursor;
    }

    @Override
    public MutableLongSet augmentLabels( MutableLongSet labels, NodeState nodeState )
    {
        final LongDiffSets labelDiffSets = nodeState.labelDiffSets();
        if ( !labelDiffSets.isEmpty() )
        {
            labelDiffSets.getRemoved().forEach( labels::remove );
            labelDiffSets.getAdded().forEach( labels::add );
        }
        return labels;
    }

    @Override
    public Cursor<RelationshipItem> augmentSingleRelationshipCursor( Cursor<RelationshipItem> cursor,
            long relationshipId )
    {
        return hasChanges ? singleRelationshipCursor.get().init( cursor, relationshipId ) : cursor;
    }

    @Override
    public LongDiffSets nodesWithLabelChanged( long label )
    {
        return getLabelStateNodeDiffSets( label );
    }

    @Override
    public void indexDoAdd( IndexDescriptor descriptor )
    {
        DiffSets<IndexDescriptor> diff = indexChangesDiffSets();
        if ( !diff.unRemove( descriptor ) )
        {
            diff.add( descriptor );
        }
        changed();
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChangesDiffSets().remove( descriptor );
        changed();
    }

    @Override
    public boolean indexDoUnRemove( IndexDescriptor descriptor )
    {
        return indexChangesDiffSets().unRemove( descriptor );
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        return indexChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexChanges()
    {
        return ReadableDiffSets.Empty.ifNull( indexChanges );
    }

    private DiffSets<IndexDescriptor> indexChangesDiffSets()
    {
        if ( indexChanges == null )
        {
            indexChanges = new DiffSets<>();
        }
        return indexChanges;
    }

    @Override
    public LongDiffSets addedAndRemovedNodes()
    {
        return nodes == null ? LongDiffSets.EMPTY : nodes;
    }

    private RemovalsCountingDiffSets nodes()
    {
        if ( nodes == null )
        {
            nodes = new RemovalsCountingDiffSets();
        }
        return nodes;
    }

    @Override
    public LongDiffSets addedAndRemovedRelationships()
    {
        return relationships == null ? LongDiffSets.EMPTY : relationships;
    }

    private RemovalsCountingDiffSets relationships()
    {
        if ( relationships == null )
        {
            relationships = new RemovalsCountingDiffSets();
        }
        return relationships;
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return relationshipStatesMap == null ? Iterables.empty() : Iterables.cast( relationshipStatesMap.values() );
    }

    private NodeStateImpl getOrCreateNodeState( long nodeId )
    {
        if ( nodeStatesMap == null )
        {
            nodeStatesMap = collectionsFactory.newLongObjectMap();
        }
        return nodeStatesMap.getIfAbsentPut( nodeId, () -> new NodeStateImpl( nodeId ) );
    }

    private RelationshipStateImpl getOrCreateRelationshipState( long relationshipId )
    {
        if ( relationshipStatesMap == null )
        {
            relationshipStatesMap = collectionsFactory.newLongObjectMap();
        }
        return relationshipStatesMap.getIfAbsentPut( relationshipId, () -> new RelationshipStateImpl( relationshipId ) );
    }

    private GraphState getOrCreateGraphState()
    {
        if ( graphState == null )
        {
            graphState = new GraphState();
        }
        return graphState;
    }

    @Override
    public void constraintDoAdd( IndexBackedConstraintDescriptor constraint, long indexId )
    {
        constraintsChangesDiffSets().add( constraint );
        createdConstraintIndexesByConstraint().put( constraint, indexId );
        changed();
    }

    @Override
    public void constraintDoAdd( ConstraintDescriptor constraint )
    {
        constraintsChangesDiffSets().add( constraint );
        changed();
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForLabel( int labelId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForSchema( SchemaDescriptor descriptor )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptor.equalTo( descriptor ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType( int relTypeId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasRelType( relTypeId ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChanges()
    {
        return ReadableDiffSets.Empty.ifNull( constraintsChanges );
    }

    private DiffSets<ConstraintDescriptor> constraintsChangesDiffSets()
    {
        if ( constraintsChanges == null )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    @Override
    public void constraintDoDrop( ConstraintDescriptor constraint )
    {
        constraintsChangesDiffSets().remove( constraint );
        if ( constraint.enforcesUniqueness() )
        {
            indexDoDrop( getIndexForIndexBackedConstraint( (IndexBackedConstraintDescriptor) constraint ) );
        }
        changed();
    }

    @Override
    public boolean constraintDoUnRemove( ConstraintDescriptor constraint )
    {
        return constraintsChangesDiffSets().unRemove( constraint );
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
        if ( createdConstraintIndexesByConstraint != null && !createdConstraintIndexesByConstraint.isEmpty() )
        {
            return map( this::getIndexForIndexBackedConstraint, createdConstraintIndexesByConstraint.keySet() );
        }
        return Iterables.empty();
    }

    @Override
    public Long indexCreatedForConstraint( ConstraintDescriptor constraint )
    {
        return createdConstraintIndexesByConstraint == null ? null :
                createdConstraintIndexesByConstraint.get( constraint );
    }

    @Override
    public LongDiffSets indexUpdatesForScan( IndexDescriptor descriptor )
    {
        if ( indexUpdates == null )
        {
            return LongDiffSets.EMPTY;
        }
        Map<ValueTuple, MutableLongDiffSets> updates = indexUpdates.get( descriptor.schema() );
        if ( updates == null )
        {
            return LongDiffSets.EMPTY;
        }
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( MutableLongDiffSets diffSet : updates.values() )
        {
            diffs.addAll( diffSet.getAdded() );
            diffs.removeAll( diffSet.getRemoved() );
        }
        return diffs;
    }

    @Override
    public LongDiffSets indexUpdatesForSuffixOrContains( IndexDescriptor descriptor, IndexQuery query )
    {
        assert descriptor.schema().getPropertyIds().length == 1 :
                "Suffix and contains queries are only supported for single property queries";

        if ( indexUpdates == null )
        {
            return LongDiffSets.EMPTY;
        }
        Map<ValueTuple, MutableLongDiffSets> updates = indexUpdates.get( descriptor.schema() );
        if ( updates == null )
        {
            return LongDiffSets.EMPTY;
        }
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( Map.Entry<ValueTuple, MutableLongDiffSets> entry : updates.entrySet() )
        {
            if ( query.acceptsValue( entry.getKey().getOnlyValue() ) )
            {
                MutableLongDiffSets diffsets = entry.getValue();
                diffs.addAll( diffsets.getAdded() );
                diffs.removeAll( diffsets.getRemoved() );
            }
        }
        return diffs;
    }

    @Override
    public LongDiffSets indexUpdatesForSeek( IndexDescriptor descriptor, ValueTuple values )
    {
        MutableLongDiffSets indexUpdatesForSeek = getIndexUpdatesForSeek( descriptor.schema(), values, /*create=*/false );
        return indexUpdatesForSeek == null ? LongDiffSets.EMPTY : indexUpdatesForSeek;
    }

    @Override
    public LongDiffSets indexUpdatesForRangeSeek( IndexDescriptor descriptor, ValueGroup valueGroup,
                                                                   Value lower, boolean includeLower,
                                                                   Value upper, boolean includeUpper )
    {
        assert lower != null && upper != null : "Use Values.NO_VALUE to encode the lack of a bound";

        TreeMap<ValueTuple, MutableLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return LongDiffSets.EMPTY;
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == NO_VALUE )
        {
            selectedLower = ValueTuple.of( Values.minValue( valueGroup, upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == NO_VALUE )
        {
            selectedUpper = ValueTuple.of( Values.maxValue( valueGroup, lower ) );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    private LongDiffSets indexUpdatesForRangeSeek( TreeMap<ValueTuple, MutableLongDiffSets> sortedUpdates, ValueTuple lower,
            boolean includeLower, ValueTuple upper, boolean includeUpper )
    {
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();

        Collection<MutableLongDiffSets> inRange = sortedUpdates.subMap( lower, includeLower, upper, includeUpper ).values();
        for ( MutableLongDiffSets diffForSpecificValue : inRange )
        {
            diffs.addAll( diffForSpecificValue.getAdded() );
            diffs.removeAll( diffForSpecificValue.getRemoved() );
        }
        return diffs;
    }

    @Override
    public LongDiffSets indexUpdatesForRangeSeekByPrefix( IndexDescriptor descriptor, String prefix )
    {
        TreeMap<ValueTuple, MutableLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return LongDiffSets.EMPTY;
        }
        ValueTuple floor = ValueTuple.of( Values.stringValue( prefix ) );
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( Map.Entry<ValueTuple, MutableLongDiffSets> entry : sortedUpdates.tailMap( floor ).entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( ((TextValue) key.getOnlyValue()).stringValue().startsWith( prefix ) )
            {
                MutableLongDiffSets diffSets = entry.getValue();
                diffs.addAll( diffSets.getAdded() );
                diffs.removeAll( diffSets.getRemoved() );
            }
            else
            {
                break;
            }
        }
        return diffs;
    }

    // Ensure sorted index updates for a given index. This is needed for range query support and
    // may involve converting the existing hash map first
    //
    private TreeMap<ValueTuple, MutableLongDiffSets> getSortedIndexUpdates( SchemaDescriptor descriptor )
    {
        if ( indexUpdates == null )
        {
            return null;
        }
        Map<ValueTuple, MutableLongDiffSets> updates = indexUpdates.get( descriptor );
        if ( updates == null )
        {
            return null;
        }
        TreeMap<ValueTuple, MutableLongDiffSets> sortedUpdates;
        if ( updates instanceof TreeMap )
        {
            sortedUpdates = (TreeMap<ValueTuple, MutableLongDiffSets>) updates;
        }
        else
        {
            sortedUpdates = new TreeMap<>( ValueTuple.COMPARATOR );
            sortedUpdates.putAll( updates );
            indexUpdates.put( descriptor, sortedUpdates );
        }
        return sortedUpdates;
    }

    @Override
    public void indexDoUpdateEntry( SchemaDescriptor descriptor, long nodeId,
            ValueTuple propertiesBefore, ValueTuple propertiesAfter )
    {
        NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
        Map<ValueTuple, MutableLongDiffSets> updates = getIndexUpdatesByDescriptor( descriptor, true );
        if ( propertiesBefore != null )
        {
            MutableLongDiffSets before = getIndexUpdatesForSeek( updates, propertiesBefore, true );
            //noinspection ConstantConditions
            before.remove( nodeId );
            if ( before.getRemoved().contains( nodeId ) )
            {
                nodeState.addIndexDiff( before );
            }
            else
            {
                nodeState.removeIndexDiff( before );
            }
        }
        if ( propertiesAfter != null )
        {
            MutableLongDiffSets after = getIndexUpdatesForSeek( updates, propertiesAfter, true );
            //noinspection ConstantConditions
            after.add( nodeId );
            if ( after.getAdded().contains( nodeId ) )
            {
                nodeState.addIndexDiff( after );
            }
            else
            {
                nodeState.removeIndexDiff( after );
            }
        }
    }

    private MutableLongDiffSets getIndexUpdatesForSeek(
            SchemaDescriptor schema, ValueTuple values, boolean create )
    {
        Map<ValueTuple, MutableLongDiffSets> updates = getIndexUpdatesByDescriptor( schema, create );
        if ( updates != null )
        {
            return getIndexUpdatesForSeek( updates, values, create );
        }
        return null;
    }

    private MutableLongDiffSets getIndexUpdatesForSeek( Map<ValueTuple, MutableLongDiffSets> updates,
            ValueTuple values, boolean create )
    {
        return create ? updates.computeIfAbsent( values, value -> new MutableLongDiffSetsImpl() ) : updates.get( values );
    }

    private Map<ValueTuple, MutableLongDiffSets> getIndexUpdatesByDescriptor( SchemaDescriptor schema,
            boolean create )
    {
        if ( indexUpdates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates = new HashMap<>();
        }
        Map<ValueTuple, MutableLongDiffSets> updates = indexUpdates.get( schema );
        if ( updates == null )
        {
            if ( !create )
            {
                return null;
            }
            updates = new HashMap<>();
            indexUpdates.put( schema, updates );
        }
        return updates;
    }

    private MutableObjectLongMap<IndexBackedConstraintDescriptor> createdConstraintIndexesByConstraint()
    {
        if ( createdConstraintIndexesByConstraint == null )
        {
            createdConstraintIndexesByConstraint = new ObjectLongHashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private IndexDescriptor getIndexForIndexBackedConstraint( IndexBackedConstraintDescriptor constraint )
    {
        return constraint.ownedIndexDescriptor();
    }

    @Override
    public <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX
    {
        return getRelationshipState( relId ).accept( visitor );
    }

    @Override
    public boolean hasDataChanges()
    {
        return hasDataChanges;
    }

    /**
     * Release all underlying resources. The instance must not be used after calling this method .
     */
    public void release()
    {
        // nop
    }

    private static class ConstraintDiffSetsVisitor implements DiffSetsVisitor<ConstraintDescriptor>
    {
        private final TxStateVisitor visitor;

        ConstraintDiffSetsVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public void visitAdded( ConstraintDescriptor constraint ) throws CreateConstraintFailureException
        {
            visitor.visitAddedConstraint( constraint );
        }

        @Override
        public void visitRemoved( ConstraintDescriptor constraint )
        {
            visitor.visitRemovedConstraint( constraint );
        }
    }

    /**
     * This class works around the fact that create-delete in the same transaction is a no-op in {@link DiffSets},
     * whereas we need to know total number of explicit removals.
     */
    private class RemovalsCountingDiffSets extends MutableLongDiffSetsImpl
    {
        private MutableLongSet removedFromAdded;

        @Override
        public boolean remove( long elem )
        {
            if ( isAdded( elem ) && super.remove( elem ) )
            {
                if ( removedFromAdded == null )
                {
                    removedFromAdded = collectionsFactory.newLongSet();
                }
                removedFromAdded.add( elem );
                return true;
            }
            return super.remove( elem );
        }

        private boolean wasRemoved( long id )
        {
            return (removedFromAdded != null && removedFromAdded.contains( id )) || super.isRemoved( id );
        }
    }
}
