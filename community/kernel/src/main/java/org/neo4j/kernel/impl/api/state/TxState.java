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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.MutableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableDiffSetsImpl;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.GraphState;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

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

    private GraphStateImpl graphState;
    private MutableDiffSets<IndexDescriptor> indexChanges;
    private MutableDiffSets<ConstraintDescriptor> constraintsChanges;

    private RemovalsCountingDiffSets nodes;
    private RemovalsCountingDiffSets relationships;

    private MutableObjectLongMap<IndexBackedConstraintDescriptor> createdConstraintIndexesByConstraint;

    private Map<SchemaDescriptor, Map<ValueTuple, MutableLongDiffSets>> indexUpdates;

    private long revision;
    private long dataRevision;

    public TxState()
    {
        this( OnHeapCollectionsFactory.INSTANCE );
    }

    public TxState( CollectionsFactory collectionsFactory )
    {
        this.collectionsFactory = collectionsFactory;
    }

    @Override
    public void accept( final TxStateVisitor visitor ) throws ConstraintValidationException, CreateConstraintFailureException
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
            for ( ConstraintDescriptor added : constraintsChanges.getAdded() )
            {
                visitor.visitAddedConstraint( added );
            }
            constraintsChanges.getRemoved().forEach( visitor::visitRemovedConstraint );
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
        return revision != 0;
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
    {
        return nodeStatesMap == null ? Iterables.empty() : Iterables.cast( nodeStatesMap.values() );
    }

    @VisibleForTesting
    MutableLongDiffSets getOrCreateLabelStateNodeDiffSets( long labelId )
    {
        if ( labelStatesMap == null )
        {
            labelStatesMap = new LongObjectHashMap<>();
        }
        return labelStatesMap.getIfAbsentPut( labelId, () -> new MutableLongDiffSetsImpl( collectionsFactory ) );
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
        revision++;
    }

    private void dataChanged()
    {
        changed();
        dataRevision = revision;
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
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, RelationshipDirection.LOOP );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, RelationshipDirection.OUTGOING );
            getOrCreateNodeState( endNodeId ).addRelationship( id, relationshipTypeId, RelationshipDirection.INCOMING );
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
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, RelationshipDirection.LOOP );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, RelationshipDirection.OUTGOING );
            getOrCreateNodeState( endNodeId ).removeRelationship( id, type, RelationshipDirection.INCOMING );
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
            createdLabelTokens = new LongObjectHashMap<>();
        }
        createdLabelTokens.put( id, labelName );
        changed();
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        if ( createdPropertyKeyTokens == null )
        {
            createdPropertyKeyTokens = new LongObjectHashMap<>();
        }
        createdPropertyKeyTokens.put( id, propertyKeyName );
        changed();
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        if ( createdRelationshipTypeTokens == null )
        {
            createdRelationshipTypeTokens = new LongObjectHashMap<>();
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
    public GraphState getGraphState()
    {
        return graphState;
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
    public LongDiffSets nodesWithLabelChanged( long label )
    {
        return getLabelStateNodeDiffSets( label );
    }

    @Override
    public void indexDoAdd( IndexDescriptor descriptor )
    {
        MutableDiffSets<IndexDescriptor> diff = indexChangesDiffSets();
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
    public DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        return indexChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
    }

    @Override
    public DiffSets<IndexDescriptor> indexDiffSetsByRelationshipType( int relationshipType )
    {
        return indexChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasRelType( relationshipType ) );
    }

    @Override
    public DiffSets<IndexDescriptor> indexDiffSetsBySchema( SchemaDescriptor schema )
    {
        return indexChangesDiffSets().filterAdded( indexDescriptor -> indexDescriptor.schema().equals( schema ) );
    }

    @Override
    public DiffSets<IndexDescriptor> indexChanges()
    {
        return DiffSets.Empty.ifNull( indexChanges );
    }

    private MutableDiffSets<IndexDescriptor> indexChangesDiffSets()
    {
        if ( indexChanges == null )
        {
            indexChanges = new MutableDiffSetsImpl<>();
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

    @VisibleForTesting
    NodeStateImpl getOrCreateNodeState( long nodeId )
    {
        if ( nodeStatesMap == null )
        {
            nodeStatesMap = new LongObjectHashMap<>();
        }
        return nodeStatesMap.getIfAbsentPut( nodeId, () -> new NodeStateImpl( nodeId, collectionsFactory ) );
    }

    private RelationshipStateImpl getOrCreateRelationshipState( long relationshipId )
    {
        if ( relationshipStatesMap == null )
        {
            relationshipStatesMap = new LongObjectHashMap<>();
        }
        return relationshipStatesMap.getIfAbsentPut( relationshipId, () -> new RelationshipStateImpl( relationshipId, collectionsFactory ) );
    }

    @VisibleForTesting
    GraphStateImpl getOrCreateGraphState()
    {
        if ( graphState == null )
        {
            graphState = new GraphStateImpl( collectionsFactory );
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
    public DiffSets<ConstraintDescriptor> constraintsChangesForLabel( int labelId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
    }

    @Override
    public DiffSets<ConstraintDescriptor> constraintsChangesForSchema( SchemaDescriptor descriptor )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptor.equalTo( descriptor ) );
    }

    @Override
    public DiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType( int relTypeId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasRelType( relTypeId ) );
    }

    @Override
    public DiffSets<ConstraintDescriptor> constraintsChanges()
    {
        return DiffSets.Empty.ifNull( constraintsChanges );
    }

    private MutableDiffSets<ConstraintDescriptor> constraintsChangesDiffSets()
    {
        if ( constraintsChanges == null )
        {
            constraintsChanges = new MutableDiffSetsImpl<>();
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
            return map( TxState::getIndexForIndexBackedConstraint, createdConstraintIndexesByConstraint.keySet() );
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
    @Nullable
    public UnmodifiableMap<ValueTuple, ? extends LongDiffSets> getIndexUpdates( SchemaDescriptor schema )
    {
        if ( indexUpdates == null )
        {
            return null;
        }
        Map<ValueTuple, MutableLongDiffSets> updates = indexUpdates.get( schema );
        if ( updates == null )
        {
            return null;
        }

        return new UnmodifiableMap<>( updates );
    }

    @Override
    @Nullable
    public NavigableMap<ValueTuple, ? extends LongDiffSets> getSortedIndexUpdates( SchemaDescriptor descriptor )
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
        return Collections.unmodifiableNavigableMap( sortedUpdates );
    }

    @Override
    public void indexDoUpdateEntry( SchemaDescriptor descriptor, long nodeId,
            ValueTuple propertiesBefore, ValueTuple propertiesAfter )
    {
        NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
        Map<ValueTuple, MutableLongDiffSets> updates = getOrCreateIndexUpdatesByDescriptor( descriptor );
        if ( propertiesBefore != null )
        {
            MutableLongDiffSets before = getOrCreateIndexUpdatesForSeek( updates, propertiesBefore );
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
            MutableLongDiffSets after = getOrCreateIndexUpdatesForSeek( updates, propertiesAfter );
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

    @VisibleForTesting
    MutableLongDiffSets getOrCreateIndexUpdatesForSeek( Map<ValueTuple, MutableLongDiffSets> updates, ValueTuple values )
    {
        return updates.computeIfAbsent( values, value -> new MutableLongDiffSetsImpl( collectionsFactory ) );
    }

    private Map<ValueTuple, MutableLongDiffSets> getOrCreateIndexUpdatesByDescriptor( SchemaDescriptor schema )
    {
        if ( indexUpdates == null )
        {
            indexUpdates = new HashMap<>();
        }
        return indexUpdates.computeIfAbsent( schema, k -> new HashMap<>() );
    }

    private MutableObjectLongMap<IndexBackedConstraintDescriptor> createdConstraintIndexesByConstraint()
    {
        if ( createdConstraintIndexesByConstraint == null )
        {
            createdConstraintIndexesByConstraint = new ObjectLongHashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private static IndexDescriptor getIndexForIndexBackedConstraint( IndexBackedConstraintDescriptor constraint )
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
        return dataRevision != 0;
    }

    public long getDataRevision()
    {
        return dataRevision;
    }

    /**
     * This class works around the fact that create-delete in the same transaction is a no-op in {@link MutableDiffSetsImpl},
     * whereas we need to know total number of explicit removals.
     */
    private class RemovalsCountingDiffSets extends MutableLongDiffSetsImpl
    {
        private MutableLongSet removedFromAdded;

        RemovalsCountingDiffSets()
        {
            super( collectionsFactory );
        }

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
