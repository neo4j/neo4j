/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipActions;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.storageengine.api.txstate.ReadableTransactionState.EMPTY;

/**
 * Transform for {@link org.neo4j.storageengine.api.txstate.ReadableTransactionState} to make it accessible as {@link TransactionData}.
 */
public class TxStateTransactionDataSnapshot implements TransactionData
{
    private final ReadableTransactionState state;
    private final NodeProxy.NodeActions nodeActions;
    private final RelationshipActions relationshipActions;
    private final StoreReadLayer store;
    private KernelTransaction transaction;

    private final Collection<PropertyEntry<Node>> assignedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> assignedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> assignedLabels = new ArrayList<>();

    private final Collection<PropertyEntry<Node>> removedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> removedLabels = new ArrayList<>();
    private final PrimitiveLongObjectMap<RelationshipProxy> relationshipsReadFromStore = Primitive.longObjectMap( 16 );

    public TxStateTransactionDataSnapshot( ReadableTransactionState state, NodeProxy.NodeActions nodeActions,
            RelationshipActions relationshipActions, StoreReadLayer storeReadLayer, KernelTransaction transaction )
    {
        this.state = state;
        this.nodeActions = nodeActions;
        this.relationshipActions = relationshipActions;
        this.store = storeReadLayer;
        this.transaction = transaction;

        // Load changes that require store access eagerly, because we won't have access to the after-state
        // after the tx has been committed.
        takeSnapshot();
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
        return map2Rels( state.addedAndRemovedRelationships().getAdded() );
    }

    @Override
    public Iterable<Relationship> deletedRelationships()
    {
        return map2Rels( state.addedAndRemovedRelationships().getRemoved() );
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
    public String username()
    {
        return transaction.securityContext().subject().username();
    }

    @Override
    public Map<String,Object> metaData()
    {
        return transaction.getUserMetaData();
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

    @Override
    public long getTransactionId()
    {
        return transaction.getTransactionId();
    }

    @Override
    public long getCommitTime()
    {
        return transaction.getCommitTime();
    }

    private void takeSnapshot()
    {
        try
        {
            for ( long nodeId : state.addedAndRemovedNodes().getRemoved() )
            {
                try ( Cursor<NodeItem> node = store.nodeGetSingleCursor( nodeId, EMPTY ) )
                {
                    if ( node.next() )
                    {
                        try ( Cursor<PropertyItem> properties = store.nodeGetProperties( node.get(), NodeState.EMPTY ) )
                        {
                            while ( properties.next() )
                            {
                                removedNodeProperties.add( new NodePropertyEntryView( nodeId,
                                        store.propertyKeyGetName( properties.get().propertyKeyId() ), null,
                                        properties.get().value() ) );
                            }
                        }

                        node.get().labels().visitKeys( labelId ->
                        {
                            removedLabels.add( new LabelEntryView( nodeId, store.labelGetName( labelId ) ) );
                            return false;
                        } );
                    }
                }
            }
            for ( long relId : state.addedAndRemovedRelationships().getRemoved() )
            {
                Relationship relationshipProxy = relationship( relId );
                try ( Cursor<RelationshipItem> relationship = store.relationshipGetSingleCursor( relId, EMPTY ) )
                {
                    if ( relationship.next() )
                    {
                        try ( Cursor<PropertyItem> properties = store
                                .relationshipGetProperties( relationship.get(), RelationshipState.EMPTY ) )
                        {
                            while ( properties.next() )
                            {
                                removedRelationshipProperties.add( new RelationshipPropertyEntryView( relationshipProxy,
                                        store.propertyKeyGetName( properties.get().propertyKeyId() ), null,
                                        properties.get().value() ) );
                            }
                        }

                    }
                }
            }
            for ( NodeState nodeState : state.modifiedNodes() )
            {
                Iterator<StorageProperty> added = nodeState.addedAndChangedProperties();
                while ( added.hasNext() )
                {
                    DefinedProperty property = (DefinedProperty) added.next();
                    assignedNodeProperties.add( new NodePropertyEntryView( nodeState.getId(),
                            store.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                            committedValue( nodeState, property.propertyKeyId() ) ) );
                }
                Iterator<Integer> removed = nodeState.removedProperties();
                while ( removed.hasNext() )
                {
                    Integer property = removed.next();
                    removedNodeProperties
                            .add( new NodePropertyEntryView( nodeState.getId(), store.propertyKeyGetName( property ),
                                    null, committedValue( nodeState, property ) ) );
                }
                ReadableDiffSets<Integer> labels = nodeState.labelDiffSets();
                for ( Integer label : labels.getAdded() )
                {
                    assignedLabels.add( new LabelEntryView( nodeState.getId(), store.labelGetName( label ) ) );
                }
                for ( Integer label : labels.getRemoved() )
                {
                    removedLabels.add( new LabelEntryView( nodeState.getId(), store.labelGetName( label ) ) );
                }
            }
            for ( RelationshipState relState : state.modifiedRelationships() )
            {
                Relationship relationship = relationship( relState.getId() );
                Iterator<StorageProperty> added = relState.addedAndChangedProperties();
                while ( added.hasNext() )
                {
                    DefinedProperty property = (DefinedProperty) added.next();
                    assignedRelationshipProperties.add( new RelationshipPropertyEntryView( relationship,
                            store.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                            committedValue( relState, property.propertyKeyId() ) ) );
                }
                Iterator<Integer> removed = relState.removedProperties();
                while ( removed.hasNext() )
                {
                    Integer property = removed.next();
                    removedRelationshipProperties
                            .add( new RelationshipPropertyEntryView( relationship, store.propertyKeyGetName( property ),
                                    null, committedValue( relState, property ) ) );
                }
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException | LabelNotFoundKernelException e )
        {
            throw new IllegalStateException( "An entity that does not exist was modified.", e );
        }
    }

    private Relationship relationship( long relId )
    {
        if ( state.relationshipIsAddedInThisTx( relId ) )
        {
            return new RelationshipProxy( relationshipActions, relId );
        }

        // This relationship has been created or changed in this transaction
        RelationshipProxy cached = relationshipsReadFromStore.get( relId );
        if ( cached != null )
        {
            return cached;
        }

        // Get this relationship data from the store
        try ( Cursor<RelationshipItem> cursor = store.relationshipGetSingleCursor( relId, EMPTY ) )
        {
            if ( !cursor.next() )
            {
                throw new IllegalStateException(
                        "Getting deleted relationship data should have been covered by the tx state" );
            }
            RelationshipItem rel = cursor.get();
            RelationshipProxy relationship =
                    new RelationshipProxy( relationshipActions, relId, rel.startNode(), rel.type(), rel.endNode() );
            relationshipsReadFromStore.put( relId, relationship );
            return relationship;
        }
    }

    private Iterable<Node> map2Nodes( Iterable<Long> added )
    {
        return new IterableWrapper<Node, Long>( added )
        {
            @Override
            protected Node underlyingObjectToObject( Long id )
            {
                return new NodeProxy( nodeActions, id );
            }
        };
    }

    private Iterable<Relationship> map2Rels( Iterable<Long> ids )
    {
        return new IterableWrapper<Relationship, Long>( ids )
        {
            @Override
            protected Relationship underlyingObjectToObject( Long id )
            {
                return relationship( id );
            }
        };
    }

    private Object committedValue( NodeState nodeState, int property )
    {
        if ( state.nodeIsAddedInThisTx( nodeState.getId() ) )
        {
            return null;
        }

        try ( Cursor<NodeItem> node = store.nodeGetSingleCursor( nodeState.getId(), EMPTY ) )
        {
            if ( !node.next() )
            {
                return null;
            }

            try ( Cursor<PropertyItem> properties = store.nodeGetProperty( node.get(), property, NodeState.EMPTY ) )
            {
                if ( properties.next() )
                {
                    return properties.get().value();
                }
            }
        }

        return null;
    }

    private Object committedValue( RelationshipState relState, int property )
    {
        if ( state.relationshipIsAddedInThisTx( relState.getId() ) )
        {
            return null;
        }

        try ( Cursor<RelationshipItem> relationship = store.relationshipGetSingleCursor( relState.getId(), EMPTY ) )
        {
            if ( !relationship.next() )
            {
                return null;
            }

            try ( Cursor<PropertyItem> properties = store
                    .relationshipGetProperty( relationship.get(), property, RelationshipState.EMPTY ) )
            {
                if ( properties.next() )
                {
                    return properties.get().value();
                }
            }
        }

        return null;
    }

    private class NodePropertyEntryView implements PropertyEntry<Node>
    {
        private final long nodeId;
        private final String key;
        private final Object newValue;
        private final Object oldValue;

        NodePropertyEntryView( long nodeId, String key, Object newValue, Object oldValue )
        {
            this.nodeId = nodeId;
            this.key = key;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        @Override
        public Node entity()
        {
            return new NodeProxy( nodeActions, nodeId );
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommittedValue()
        {
            return oldValue;
        }

        @Override
        public Object value()
        {
            if ( newValue == null )
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
        private final Relationship relationship;
        private final String key;
        private final Object newValue;
        private final Object oldValue;

        RelationshipPropertyEntryView( Relationship relationship, String key, Object newValue, Object oldValue )
        {
            this.relationship = relationship;
            this.key = key;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        @Override
        public Relationship entity()
        {
            return relationship;
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommittedValue()
        {
            return oldValue;
        }

        @Override
        public Object value()
        {
            if ( newValue == null )
            {
                throw new IllegalStateException( "This property has been removed, it has no value anymore." );
            }
            return newValue;
        }

        @Override
        public String toString()
        {
            return "RelationshipPropertyEntryView{" +
                    "relId=" + relationship.getId() +
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

        LabelEntryView( long nodeId, String labelName )
        {
            this.nodeId = nodeId;
            this.label = Label.label( labelName );
        }

        @Override
        public Label label()
        {
            return label;
        }

        @Override
        public Node node()
        {
            return new NodeProxy( nodeActions, nodeId );
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
