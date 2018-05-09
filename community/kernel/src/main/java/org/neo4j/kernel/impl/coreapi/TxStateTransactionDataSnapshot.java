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
package org.neo4j.kernel.impl.coreapi;

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;

/**
 * Transform for {@link org.neo4j.storageengine.api.txstate.ReadableTransactionState} to make it accessible as {@link TransactionData}.
 */
public class TxStateTransactionDataSnapshot implements TransactionData
{
    private final ReadableTransactionState state;
    private final EmbeddedProxySPI proxySpi;
    private final StorageReader store;
    private final KernelTransaction transaction;

    private final Collection<PropertyEntry<Node>> assignedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> assignedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> assignedLabels = new ArrayList<>();

    private final Collection<PropertyEntry<Node>> removedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> removedLabels = new ArrayList<>();
    private final MutableLongObjectMap<RelationshipProxy> relationshipsReadFromStore = new LongObjectHashMap<>( 16 );

    public TxStateTransactionDataSnapshot(
            ReadableTransactionState state, EmbeddedProxySPI proxySpi,
            StorageReader storageReader, KernelTransaction transaction )
    {
        this.state = state;
        this.proxySpi = proxySpi;
        this.store = storageReader;
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
        if ( transaction instanceof KernelTransactionImplementation )
        {
            return ((KernelTransactionImplementation) transaction).getMetaData();
        }
        else
        {
            return Collections.emptyMap();
        }
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
        try ( NodeCursor nodeCursor = store.allocateNodeCursorCommitted();
              PropertyCursor propertyCursor = store.allocatePropertyCursorCommitted();
              RelationshipScanCursor relationshipCursor = store.allocateRelationshipScanCursorCommitted() )
        {
            state.addedAndRemovedNodes().getRemoved().each( nodeId ->
            {
                store.singleNode( nodeId, nodeCursor );
                if ( nodeCursor.next() )
                {
                    nodeCursor.properties( propertyCursor );
                    while ( propertyCursor.next() )
                    {
                        try
                        {
                            NodePropertyEntryView entryView = new NodePropertyEntryView( nodeId, store.propertyKeyGetName( propertyCursor.propertyKey() ), null,
                                    propertyCursor.propertyValue() );
                            removedNodeProperties.add( entryView );
                        }
                        catch ( PropertyKeyIdNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Non-existent property was modified for node " + nodeId, e );
                        }
                    }
                    Nodes.visitLabels( nodeCursor.labels(), labelId ->
                    {
                        try
                        {
                            LabelEntryView entryView = new LabelEntryView( nodeId, store.labelGetName( labelId ) );
                            removedLabels.add( entryView );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Non-existent label was modified for node " + nodeId, e );
                        }
                    } );
                }
            } );
            state.addedAndRemovedRelationships().getRemoved().each( relId ->
            {
                Relationship relationshipProxy = relationship( relId );
                store.singleRelationship( relId, relationshipCursor );
                if ( relationshipCursor.next() )
                {
                    relationshipCursor.properties( propertyCursor );
                    while ( propertyCursor.next() )
                    {
                        try
                        {
                            RelationshipPropertyEntryView entryView =
                                    new RelationshipPropertyEntryView( relationshipProxy, store.propertyKeyGetName( propertyCursor.propertyKey() ), null,
                                            propertyCursor.propertyValue() );
                            removedRelationshipProperties.add( entryView );
                        }
                        catch ( PropertyKeyIdNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Non-existent property was modified for relationship " + relId, e );
                        }
                    }
                }
            } );
            for ( NodeState nodeState : state.modifiedNodes() )
            {
                Iterator<StorageProperty> added = nodeState.addedAndChangedProperties();
                long nodeId = nodeState.getId();
                while ( added.hasNext() )
                {
                    StorageProperty property = added.next();
                    try
                    {
                        NodePropertyEntryView entryView = new NodePropertyEntryView( nodeId, store.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                                committedValue( nodeState.getId(), property.propertyKeyId(), nodeCursor, propertyCursor ) );
                        assignedNodeProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Non-existent property was modified for node " + nodeId, e );
                    }
                }
                nodeState.removedProperties().each( id ->
                {
                    try
                    {
                        NodePropertyEntryView entryView = new NodePropertyEntryView( nodeState.getId(), store.propertyKeyGetName( id ), null,
                                committedValue( nodeState.getId(), id, nodeCursor, propertyCursor ) );
                        removedNodeProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Non-existent property was modified for node " + nodeId, e );
                    }
                } );

                final LongDiffSets labels = nodeState.labelDiffSets();
                addLabelEntriesTo( nodeId, labels.getAdded(), assignedLabels );
                addLabelEntriesTo( nodeId, labels.getRemoved(), removedLabels );
            }
            for ( RelationshipState relState : state.modifiedRelationships() )
            {
                Relationship relationship = relationship( relState.getId() );
                Iterator<StorageProperty> added = relState.addedAndChangedProperties();
                while ( added.hasNext() )
                {
                    StorageProperty property = added.next();
                    try
                    {
                        RelationshipPropertyEntryView entryView =
                                new RelationshipPropertyEntryView( relationship, store.propertyKeyGetName( property.propertyKeyId() ), property.value(),
                                        committedValue( relState.getId(), property.propertyKeyId(), relationshipCursor, propertyCursor ) );
                        assignedRelationshipProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Non-existent property was modified for relationship " + relState.getId(), e );
                    }
                }
                relState.removedProperties().each( id ->
                {
                    try
                    {
                        RelationshipPropertyEntryView entryView = new RelationshipPropertyEntryView( relationship, store.propertyKeyGetName( id ),
                                null, committedValue( relState.getId(), id, relationshipCursor, propertyCursor ) );
                        removedRelationshipProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Non-existent property was modified for relationship " + relState.getId(), e );
                    }
                } );
            }
        }
    }

    private void addLabelEntriesTo( long nodeId, LongSet labelIds, Collection<LabelEntry> target )
    {
        labelIds.each( labelId ->
        {
            try
            {
                final LabelEntry labelEntryView = new LabelEntryView( nodeId, store.labelGetName( toIntExact( labelId ) ) );
                target.add( labelEntryView );
            }
            catch ( LabelNotFoundKernelException e )
            {
                throw new IllegalStateException( "Non-existent label was modified for node " + nodeId, e );
            }
        } );
    }

    private Relationship relationship( long relId )
    {
        RelationshipProxy relationship = proxySpi.newRelationshipProxy( relId );
        if ( !state.relationshipVisit( relId, relationship ) )
        {   // This relationship has been created or changed in this transaction
            RelationshipProxy cached = relationshipsReadFromStore.get( relId );
            if ( cached != null )
            {
                return cached;
            }

            try
            {   // Get this relationship data from the store
                store.relationshipVisit( relId, relationship );
                relationshipsReadFromStore.put( relId, relationship );
            }
            catch ( EntityNotFoundException e )
            {
                throw new IllegalStateException(
                        "Getting deleted relationship data should have been covered by the tx state", e );
            }
        }
        return relationship;
    }

    private Iterable<Node> map2Nodes( LongIterable ids )
    {
        return ids.asLazy().collect( id -> new NodeProxy( proxySpi, id ) );
    }

    private Iterable<Relationship> map2Rels( LongIterable ids )
    {
        return ids.asLazy().collect( this::relationship );
    }

    private Value committedValue( long nodeId, int property, NodeCursor nodeCursor, PropertyCursor propertyCursor )
    {
        if ( state.nodeIsAddedInThisTx( nodeId ) )
        {
            return Values.NO_VALUE;
        }

        store.singleNode( nodeId, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return Values.NO_VALUE;
        }

        nodeCursor.properties( propertyCursor );
        return findProperty( propertyCursor, property );
    }

    private Value findProperty( PropertyCursor propertyCursor, int propertyKeyId )
    {
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKeyId )
            {
                return propertyCursor.propertyValue();
            }
        }
        return Values.NO_VALUE;
    }

    private Value committedValue( long relationshipId, int property, RelationshipScanCursor relationshipCursor, PropertyCursor propertyCursor )
    {
        if ( state.relationshipIsAddedInThisTx( relationshipId ) )
        {
            return Values.NO_VALUE;
        }

        store.singleRelationship( relationshipId, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            return Values.NO_VALUE;
        }

        relationshipCursor.properties( propertyCursor );
        return findProperty( propertyCursor, property );
    }

    private class NodePropertyEntryView implements PropertyEntry<Node>
    {
        private final long nodeId;
        private final String key;
        private final Value newValue;
        private final Value oldValue;

        NodePropertyEntryView( long nodeId, String key, Value newValue, Value oldValue )
        {
            this.nodeId = nodeId;
            this.key = key;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        @Override
        public Node entity()
        {
            return new NodeProxy( proxySpi, nodeId );
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommitedValue()
        {
            return oldValue.asObjectCopy();
        }

        @Override
        public Object value()
        {
            if ( newValue == null || newValue == Values.NO_VALUE )
            {
                throw new IllegalStateException( "This property has been removed, it has no value anymore: " + this );
            }
            return newValue.asObjectCopy();
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

    private static class RelationshipPropertyEntryView implements PropertyEntry<Relationship>
    {
        private final Relationship relationship;
        private final String key;
        private final Value newValue;
        private final Value oldValue;

        RelationshipPropertyEntryView( Relationship relationship, String key, Value newValue, Value oldValue )
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
        public Object previouslyCommitedValue()
        {
            return oldValue.asObjectCopy();
        }

        @Override
        public Object value()
        {
            if ( newValue == null || newValue == Values.NO_VALUE )
            {
                throw new IllegalStateException( "This property has been removed, it has no value anymore: " + this );
            }
            return newValue.asObjectCopy();
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
            return new NodeProxy( proxySpi, nodeId );
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
