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
package org.neo4j.kernel.internal.event;

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
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
public class TxStateTransactionDataSnapshot implements TransactionData, AutoCloseable
{
    private final ReadableTransactionState state;
    private final StorageReader store;
    private final KernelTransaction transaction;

    private final Collection<PropertyEntry<Node>> assignedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> assignedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> assignedLabels = new ArrayList<>();

    private final Collection<PropertyEntry<Node>> removedNodeProperties = new ArrayList<>();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties = new ArrayList<>();
    private final Collection<LabelEntry> removedLabels = new ArrayList<>();
    private final MutableLongObjectMap<RelationshipEntity> relationshipsReadFromStore = new LongObjectHashMap<>( 16 );
    private final StorageRelationshipScanCursor relationship;
    private final InternalTransaction internalTransaction;

    TxStateTransactionDataSnapshot( ReadableTransactionState state, StorageReader storageReader,
            KernelTransaction transaction )
    {
        this.state = state;
        this.store = storageReader;
        this.transaction = transaction;
        this.internalTransaction = ((KernelTransactionImplementation) transaction).internalTransaction();
        this.relationship = storageReader.allocateRelationshipScanCursor();

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
        return transaction.getMetaData();
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
        try ( StorageNodeCursor node = store.allocateNodeCursor();
              StoragePropertyCursor properties = store.allocatePropertyCursor() )
        {
            TokenRead tokenRead = transaction.tokenRead();
            state.addedAndRemovedNodes().getRemoved().each( nodeId ->
            {
                node.single( nodeId );
                if ( node.next() )
                {
                    node.properties( properties );
                    while ( properties.next() )
                    {
                        try
                        {
                            removedNodeProperties.add( new NodePropertyEntryView( nodeId, tokenRead.propertyKeyName( properties.propertyKey() ),
                                    null, properties.propertyValue() ) );
                        }
                        catch ( PropertyKeyIdNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Not existing properties was modified for node " + nodeId, e );
                        }
                    }

                    for ( long labelId : node.labels() )
                    {
                        try
                        {
                            removedLabels.add( new LabelEntryView( nodeId, tokenRead.nodeLabelName( toIntExact( labelId ) ) ) );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Not existing label was modified for node " + nodeId, e );
                        }
                    }
                }
            } );
            state.addedAndRemovedRelationships().getRemoved().each( relId ->
            {
                Relationship relationship = relationship( relId );
                this.relationship.single( relId );
                if ( this.relationship.next() )
                {
                    this.relationship.properties( properties );
                    while ( properties.next() )
                    {
                        try
                        {
                            removedRelationshipProperties.add(
                                    new RelationshipPropertyEntryView( relationship, tokenRead.propertyKeyName( properties.propertyKey() ), null,
                                            properties.propertyValue() ) );
                        }
                        catch ( PropertyKeyIdNotFoundKernelException e )
                        {
                            throw new IllegalStateException( "Not existing node properties was modified for relationship " + relId, e );
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
                    assignedNodeProperties.add( new NodePropertyEntryView( nodeId,
                            tokenRead.propertyKeyName( property.propertyKeyId() ), property.value(),
                            committedValue( nodeState, property.propertyKeyId(), node, properties ) ) );
                }
                nodeState.removedProperties().each( id ->
                {
                    try
                    {
                        final NodePropertyEntryView entryView = new NodePropertyEntryView( nodeId, tokenRead.propertyKeyName( id ), null,
                                committedValue( nodeState, id, node, properties ) );
                        removedNodeProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Not existing node properties was modified for node " + nodeId, e );
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
                    assignedRelationshipProperties.add( new RelationshipPropertyEntryView( relationship,
                            tokenRead.propertyKeyName( property.propertyKeyId() ), property.value(),
                            committedValue( relState, property.propertyKeyId(), this.relationship, properties ) ) );
                }
                relState.removedProperties().each( id ->
                {
                    try
                    {
                        final RelationshipPropertyEntryView entryView = new RelationshipPropertyEntryView( relationship, tokenRead.propertyKeyName( id ),
                                null, committedValue( relState, id, this.relationship, properties ) );
                        removedRelationshipProperties.add( entryView );
                    }
                    catch ( PropertyKeyIdNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "Not existing properties was modified for relationship " + relState.getId(), e );
                    }
                } );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "An entity that does not exist was modified.", e );
        }
    }

    @Override
    public void close()
    {
        relationship.close();
    }

    private void addLabelEntriesTo( long nodeId, LongSet labelIds, Collection<LabelEntry> target )
    {
        labelIds.each( labelId ->
        {
            try
            {
                final LabelEntry labelEntryView = new LabelEntryView( nodeId, transaction.tokenRead().nodeLabelName( toIntExact( labelId ) ) );
                target.add( labelEntryView );
            }
            catch ( LabelNotFoundKernelException e )
            {
                throw new IllegalStateException( "Not existing label was modified for node " + nodeId, e );
            }
        } );
    }

    private Relationship relationship( long relId )
    {
        RelationshipEntity relationship = (RelationshipEntity) internalTransaction.newRelationshipEntity( relId );
        if ( !state.relationshipVisit( relId, relationship ) )
        {   // This relationship has been created or changed in this transaction
            RelationshipEntity cached = relationshipsReadFromStore.get( relId );
            if ( cached != null )
            {
                return cached;
            }

            // Get this relationship data from the store
            this.relationship.single( relId );
            if ( !this.relationship.next() )
            {
                throw new IllegalStateException( "Getting deleted relationship data should have been covered by the tx state" );
            }
            relationship.visit( relId, this.relationship.type(), this.relationship.sourceNodeReference(), this.relationship.targetNodeReference() );
            relationshipsReadFromStore.put( relId, relationship );
        }
        return relationship;
    }

    private Iterable<Node> map2Nodes( LongIterable ids )
    {
        return ids.asLazy().collect( id -> new NodeEntity( internalTransaction, id ) );
    }

    private Iterable<Relationship> map2Rels( LongIterable ids )
    {
        return ids.asLazy().collect( this::relationship );
    }

    private Value committedValue( NodeState nodeState, int property, StorageNodeCursor node, StoragePropertyCursor properties )
    {
        if ( state.nodeIsAddedInThisTx( nodeState.getId() ) )
        {
            return Values.NO_VALUE;
        }

        node.single( nodeState.getId() );
        if ( !node.next() )
        {
            return Values.NO_VALUE;
        }

        return committedValue( properties, node, property );
    }

    private static Value committedValue( StoragePropertyCursor properties, StorageEntityCursor entity, int propertyKey )
    {
        entity.properties( properties );
        while ( properties.next() )
        {
            if ( properties.propertyKey() == propertyKey )
            {
                return properties.propertyValue();
            }
        }

        return Values.NO_VALUE;
    }

    private Value committedValue( RelationshipState relState, int property, StorageRelationshipScanCursor relationship, StoragePropertyCursor properties )
    {
        if ( state.relationshipIsAddedInThisTx( relState.getId() ) )
        {
            return Values.NO_VALUE;
        }

        relationship.single( relState.getId() );
        if ( !relationship.next() )
        {
            return Values.NO_VALUE;
        }

        return committedValue( properties, relationship, property );
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
            return new NodeEntity( internalTransaction, nodeId );
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public Object previouslyCommittedValue()
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
        public Object previouslyCommittedValue()
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
            return new NodeEntity( internalTransaction, nodeId );
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
