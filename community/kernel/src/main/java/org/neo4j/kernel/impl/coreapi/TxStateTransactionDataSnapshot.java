/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.util.DiffSets;

/**
 * Transform for {@link org.neo4j.kernel.api.TxState} to make it accessible as {@link TransactionData}.
 */
public class TxStateTransactionDataSnapshot implements TransactionData
{
    private final TxState state;
    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relLookup;

    private final Collection<PropertyEntry<Node>> assignedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> assignedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> assignedLabels = new ArrayList<>();

    private final Collection<PropertyEntry<Node>> removedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> removedLabels = new ArrayList<>();
    private final ThreadToStatementContextBridge bridge;

    public TxStateTransactionDataSnapshot( TxState state, NodeProxy.NodeLookup nodeLookup,
            RelationshipProxy.RelationshipLookups relLookup, ThreadToStatementContextBridge bridge,
            StoreReadLayer storeReadLayer )
    {
        this.state = state;
        this.nodeLookup = nodeLookup;
        this.relLookup = relLookup;
        this.bridge = bridge;

        // Load all changes eagerly, because we won't have access to the after state after the tx has been committed.
        takeSnapshot( state, storeReadLayer );
    }

    @Override
    public Iterable<Node> createdNodes()
    {
        return map2Nodes( state.addedAndRemovedNodes().getAdded() );
    }

    @Override
    public Iterable<Node> deletedNodes()
    {
        return map2Nodes( state.addedAndRemovedNodes().getRemoved() );
    }

    @Override
    public Iterable<Relationship> createdRelationships()
    {
        return map2Rels( state.addedAndRemovedRels().getAdded() );
    }

    @Override
    public Iterable<Relationship> deletedRelationships()
    {
        return map2Rels( state.addedAndRemovedRels().getRemoved() );
    }

    @Override
    public boolean isDeleted( Node node )
    {
        return state.nodeIsDeletedInThisTx( node.getId() );
    }

    @Override
    public boolean isDeleted( Relationship relationship )
    {
        return state.relationshipIsDeletedInThisTx( relationship.getId() );
    }

    @Override
    public Iterable<PropertyEntry<Node>> assignedNodeProperties()
    {
        return assignedNodeProperties;
    }

    @Override
    public Iterable<PropertyEntry<Node>> removedNodeProperties()
    {
        return removedNodeProperties;
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties()
    {
        return assignedRelationshipProperties;
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties()
    {
        return removedRelationshipProperties;
    }

    @Override
    public Iterable<LabelEntry> removedLabels()
    {
        return removedLabels;
    }

    @Override
    public Iterable<LabelEntry> assignedLabels()
    {
        return assignedLabels;
    }

    private void takeSnapshot( TxState state, StoreReadLayer storeReadLayer )
    {
        try
        {
            for ( Long nodeId : state.addedAndRemovedNodes().getRemoved() )
            {
                Iterator<DefinedProperty> props = storeReadLayer.nodeGetAllProperties( nodeId );
                while(props.hasNext())
                {
                    DefinedProperty prop = props.next();
                    removedNodeProperties.add( new NodePropertyEntryView( nodeId,
                            storeReadLayer.propertyKeyGetName( prop.propertyKeyId() ), null, prop.value() ) );
                }

                PrimitiveIntIterator labels = storeReadLayer.nodeGetLabels( nodeId );
                while(labels.hasNext())
                {
                    removedLabels.add( new LabelEntryView( nodeId, storeReadLayer.labelGetName( labels.next() ) ) );
                }

            }
            for ( Long relId : state.addedAndRemovedRels().getRemoved() )
            {
                Iterator<DefinedProperty> props = storeReadLayer.relationshipGetAllProperties( relId );
                while(props.hasNext())
                {
                    DefinedProperty prop = props.next();
                    removedRelationshipProperties.add( new RelationshipPropertyEntryView( relId,
                            storeReadLayer.propertyKeyGetName( prop.propertyKeyId() ), null, prop.value() ) );
                }
            }
            for ( NodeState nodeState : state.modifiedNodes() )
            {
                Iterator<DefinedProperty> added = nodeState.addedAndChangedProperties();
                while ( added.hasNext()  )
                {
                    DefinedProperty property = added.next();
                    assignedNodeProperties.add( new NodePropertyEntryView( nodeState.getId(),
                            storeReadLayer.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                            committedValue( storeReadLayer, nodeState, property.propertyKeyId() ) ) );
                }
                Iterator<Integer> removed = nodeState.removedProperties();
                while ( removed.hasNext()  )
                {
                    Integer property = removed.next();
                    removedNodeProperties.add( new NodePropertyEntryView( nodeState.getId(),
                            storeReadLayer.propertyKeyGetName( property ), null,
                            committedValue( storeReadLayer, nodeState, property ) ) );
                }
                DiffSets<Integer> labels = nodeState.labelDiffSets();
                for ( Integer label : labels.getAdded() )
                {
                    assignedLabels.add( new LabelEntryView( nodeState.getId(), storeReadLayer.labelGetName( label ) ) );
                }
                for ( Integer label : labels.getRemoved() )
                {
                    removedLabels.add( new LabelEntryView( nodeState.getId(), storeReadLayer.labelGetName( label ) ) );
                }
            }
            for ( RelationshipState relState : state.modifiedRelationships() )
            {
                Iterator<DefinedProperty> added = relState.addedAndChangedProperties();
                while ( added.hasNext()  )
                {
                    DefinedProperty property = added.next();
                    assignedRelationshipProperties.add( new RelationshipPropertyEntryView( relState.getId(),
                            storeReadLayer.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                            committedValue( storeReadLayer, relState, property.propertyKeyId() ) ) );
                }
                Iterator<Integer> removed = relState.removedProperties();
                while ( removed.hasNext()  )
                {
                    Integer property = removed.next();
                    removedRelationshipProperties.add( new RelationshipPropertyEntryView( relState.getId(),
                            storeReadLayer.propertyKeyGetName( property ), null,
                            committedValue( storeReadLayer, relState, property ) ) );
                }
            }
        }
        catch ( EntityNotFoundException | PropertyKeyIdNotFoundKernelException | LabelNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake", "An entity that does not exist was modified.", e );
        }
    }

    private Iterable<Node> map2Nodes( Iterable<Long> added )
    {
        return Iterables.map(new Function<Long, Node>(){
            @Override
            public Node apply( Long id )
            {
                return new NodeProxy( id, nodeLookup, relLookup, bridge );
            }
        }, added);
    }

    private Iterable<Relationship> map2Rels( Iterable<Long> added )
    {
        return Iterables.map(new Function<Long, Relationship>(){
            @Override
            public Relationship apply( Long id )
            {
                return new RelationshipProxy( id, relLookup, bridge );
            }
        }, added);
    }

    private Object committedValue( StoreReadLayer storeReadLayer, NodeState nodeState, int property )
    {
        try
        {
            if ( state.nodeIsAddedInThisTx( nodeState.getId() ) )
            {
                return null;
            }
            return storeReadLayer.nodeGetProperty( nodeState.getId(), property ).value();
        }
        catch ( EntityNotFoundException | PropertyNotFoundException e )
        {
            return null;
        }
    }

    private Object committedValue( StoreReadLayer storeReadLayer, RelationshipState relState, int property )
    {
        try
        {
            if ( state.relationshipIsAddedInThisTx( relState.getId() ) )
            {
                return null;
            }
            return storeReadLayer.relationshipGetProperty( relState.getId(), property ).value();
        }
        catch ( EntityNotFoundException | PropertyNotFoundException e )
        {
            return null;
        }
    }

    private class NodePropertyEntryView implements PropertyEntry<Node>
    {
        private final long nodeId;
        private final String key;
        private final Object newValue;
        private final Object oldValue;

        public NodePropertyEntryView( long nodeId, String key, Object newValue, Object oldValue )
        {
            this.nodeId = nodeId;
            this.key = key;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        @Override
        public Node entity()
        {
            return new NodeProxy( nodeId, nodeLookup, relLookup, bridge );
        }
        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommitedValue()
        {
            return oldValue;
        }

        @Override
        public Object value()
        {
            if(newValue == null)
            {
                throw new IllegalStateException( "This property has been removed, it has no value anymore." );
            }
            return newValue;
        }

        @Override
        public String toString()
        {
            return "NodePropertyEntryView{" +
                    "nodeId=" + nodeId +
                    ", key='" + key + '\'' +
                    ", newValue=" + newValue +
                    ", oldValue=" + oldValue +
                    '}';
        }
    }

    private class RelationshipPropertyEntryView implements PropertyEntry<Relationship>
    {
        private final long relId;
        private final String key;
        private final Object newValue;
        private final Object oldValue;

        public RelationshipPropertyEntryView( long relId, String key, Object newValue, Object oldValue )
        {
            this.relId = relId;
            this.key = key;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        @Override
        public Relationship entity()
        {
            return new RelationshipProxy( relId, relLookup, bridge );
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommitedValue()
        {
            return oldValue;
        }

        @Override
        public Object value()
        {
            if(newValue == null)
            {
                throw new IllegalStateException( "This property has been removed, it has no value anymore." );
            }
            return newValue;
        }

        @Override
        public String toString()
        {
            return "RelationshipPropertyEntryView{" +
                    "relId=" + relId +
                    ", key='" + key + '\'' +
                    ", newValue=" + newValue +
                    ", oldValue=" + oldValue +
                    '}';
        }
    }

    private class LabelEntryView implements LabelEntry
    {
        private final long nodeId;
        private final Label label;

        public LabelEntryView( long nodeId, String labelName )
        {
            this.nodeId = nodeId;
            this.label = DynamicLabel.label( labelName );
        }

        @Override
        public Label label()
        {
            return label;
        }

        @Override
        public Node node()
        {
            return new NodeProxy( nodeId, nodeLookup, relLookup, bridge );
        }

        @Override
        public String toString()
        {
            return "LabelEntryView{" +
                    "nodeId=" + nodeId +
                    ", label=" + label +
                    '}';
        }
    }
}
