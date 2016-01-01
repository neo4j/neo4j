/**
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
package org.neo4j.kernel.impl.coreapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.state.OldTxStateBridge;
import org.neo4j.kernel.impl.api.state.TxStateImpl;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class TxStateTransactionDataViewTest
{
    private final ThreadToStatementContextBridge bridge = mock(ThreadToStatementContextBridge.class);
    private final Statement stmt = mock(Statement.class);
    private final ReadOperations ops = mock(ReadOperations.class);
    private final OldTxStateBridge oldTxState = mock( OldTxStateBridge.class );
    private final TxState state = new TxStateImpl( oldTxState, mock( PersistenceManager.class),
                                             mock( TxState.IdGeneration.class) );


    @Before
    public void setup()
    {
        when(bridge.instance()).thenReturn( stmt );
        when(stmt.readOperations()).thenReturn( ops );
        when(oldTxState.nodeCreate()).thenReturn( 1l,2l,3l,4l,5l,6l,7l,8l,9l );
        when(oldTxState.relationshipCreate( anyInt(), anyLong(), anyLong() )).thenReturn( 1l,2l,3l,4l,5l,6l,7l,8l,9l );
    }

    @Test
    public void showsCreatedNodes() throws Exception
    {
        // Given
        state.nodeDoCreate();
        state.nodeDoCreate();

        // When & Then
        assertThat( idList( snapshot().createdNodes() ), equalTo( asList( 1l, 2l ) ) );
    }

    @Test
    public void showsDeletedNodes() throws Exception
    {
        // Given
        state.nodeDoDelete( 1l );
        state.nodeDoDelete( 2l );
        when(ops.nodeGetAllCommittedProperties( 1l )).thenReturn( IteratorUtil.<DefinedProperty>emptyIterator() );
        when(ops.nodeGetAllCommittedProperties( 2l )).thenReturn( asList( Property.stringProperty( 1, "p" ) ).iterator() );
        when(ops.nodeGetCommittedLabels( 1l )).thenReturn( PrimitiveIntCollections.emptyIterator() );
        when(ops.nodeGetCommittedLabels( 2l )).thenReturn( PrimitiveIntCollections.iterator( 15 ) );
        when(ops.propertyKeyGetName( 1 )).thenReturn( "key" );
        when(ops.labelGetName( 15 )).thenReturn( "label" );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedNodes()), equalTo( asList( 1l, 2l ) ) );
        assertThat( single( snapshot.removedLabels()).label().name(), equalTo("label"));
        assertThat( single( snapshot.removedNodeProperties()).key(), equalTo("key"));
    }

    @Test
    public void showsAddedRelationships() throws Exception
    {
        // Given
        state.relationshipDoCreate( 1, 1l, 2l );
        state.relationshipDoCreate( 1, 1l, 1l );

        // When & Then
        assertThat( idList( snapshot().createdRelationships() ), equalTo( asList( 1l, 2l ) ) );
    }

    @Test
    public void showsRemovedRelationships() throws Exception
    {
        // Given
        state.relationshipDoDelete( 1l, 1l, 2l, 1 );
        state.relationshipDoDelete( 2l, 1l, 1l, 1 );

        when(ops.relationshipGetAllCommittedProperties( 1l )).thenReturn( IteratorUtil.<DefinedProperty>emptyIterator() );
        when(ops.relationshipGetAllCommittedProperties( 2l )).thenReturn( asList( Property.stringProperty( 1, "p" ) ).iterator() );
        when(ops.propertyKeyGetName( 1 )).thenReturn( "key" );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedRelationships() ), equalTo( asList( 1l, 2l ) ) );
        assertThat( single( snapshot.removedRelationshipProperties()).key(), equalTo("key"));
    }

    @Test
    public void correctlySaysNodeIsDeleted() throws Exception
    {
        // Given
        state.nodeDoDelete( 1l );
        Node node = mock( Node.class );
        when(node.getId()).thenReturn( 1l );
        when(ops.nodeGetAllCommittedProperties( 1l )).thenReturn( IteratorUtil.<DefinedProperty>emptyIterator() );
        when(ops.nodeGetCommittedLabels( 1l )).thenReturn( PrimitiveIntCollections.emptyIterator() );

        // When & Then
        assertThat( snapshot().isDeleted( node ), equalTo( true ) );
    }

    @Test
    public void correctlySaysRelIsDeleted() throws Exception
    {
        // Given
        state.relationshipDoDelete( 1l, 1l, 2l, 1 );

        Relationship rel = mock( Relationship.class );
        when(rel.getId()).thenReturn( 1l );
        when(ops.relationshipGetAllCommittedProperties( 1l )).thenReturn( Collections.<DefinedProperty>emptyIterator());

        // When & Then
        assertThat( snapshot().isDeleted( rel ), equalTo( true ) );
    }

    @Test
    public void shouldListAddedNodePropertiesProperties() throws Exception
    {
        // Given
        DefinedProperty prevProp = stringProperty( 1, "prevValue" );
        state.nodeDoReplaceProperty( 1l, prevProp, stringProperty(1, "newValue") );
        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "theKey" );
        when(ops.nodeGetCommittedProperty( 1, 1 )).thenReturn( prevProp );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().assignedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single(propertyEntries);
        assertThat(entry.key(), equalTo("theKey"));
        assertThat(entry.value(), equalTo((Object)"newValue"));
        assertThat(entry.previouslyCommitedValue(), equalTo((Object)"prevValue"));
        assertThat(entry.entity().getId(), equalTo(1l));
    }

    @Test
    public void shouldListRemovedNodeProperties() throws Exception
    {
        // Given
        DefinedProperty prevProp = stringProperty( 1, "prevValue" );
        state.nodeDoRemoveProperty( 1l, prevProp );
        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "theKey" );
        when(ops.nodeGetCommittedProperty( 1, 1 )).thenReturn( prevProp );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().removedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single(propertyEntries);
        assertThat(entry.key(), equalTo( "theKey" ));
        assertThat(entry.previouslyCommitedValue(), equalTo((Object)"prevValue"));
        assertThat(entry.entity().getId(), equalTo(1l));
    }

    @Test
    public void shouldListRemovedRelationshipProperties() throws Exception
    {
        // Given
        DefinedProperty prevValue = stringProperty( 1, "prevValue" );
        state.relationshipDoRemoveProperty( 1l, prevValue );
        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "theKey" );
        when( ops.relationshipGetCommittedProperty( 1, 1 ) ).thenReturn( prevValue );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().removedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single(propertyEntries);
        assertThat(entry.key(), equalTo( "theKey" ));
        assertThat(entry.previouslyCommitedValue(), equalTo((Object)"prevValue"));
        assertThat(entry.entity().getId(), equalTo(1l));
    }

    @Test
    public void shouldListAddedRelationshipProperties() throws Exception
    {
        // Given
        DefinedProperty prevProp = stringProperty( 1, "prevValue" );
        state.relationshipDoReplaceProperty( 1l, prevProp, stringProperty(1, "newValue") );

        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "theKey" );
        when(ops.relationshipGetCommittedProperty( 1, 1 )).thenReturn( prevProp );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().assignedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single(propertyEntries);
        assertThat(entry.key(), equalTo("theKey"));
        assertThat(entry.value(), equalTo((Object)"newValue"));
        assertThat(entry.previouslyCommitedValue(), equalTo((Object)"prevValue"));
        assertThat(entry.entity().getId(), equalTo(1l));
    }

    @Test
    public void shouldListAddedLabels() throws Exception
    {
        // Given
        state.nodeDoAddLabel( 2, 1l );
        when(ops.labelGetName( 2 )).thenReturn( "theLabel" );
        when(ops.nodeGetCommittedLabels( 1l )).thenReturn( PrimitiveIntCollections.emptyIterator() );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().assignedLabels();

        // Then
        LabelEntry entry = single(labelEntries);
        assertThat( entry.label().name(), equalTo("theLabel") );
        assertThat( entry.node().getId(), equalTo(1l));
    }

    @Test
    public void shouldListRemovedLabels() throws Exception
    {
        // Given
        state.nodeDoRemoveLabel( 2, 1l );
        when(ops.labelGetName( 2 )).thenReturn( "theLabel" );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().removedLabels();

        // Then
        LabelEntry entry = single(labelEntries);
        assertThat( entry.label().name(), equalTo("theLabel") );
        assertThat( entry.node().getId(), equalTo(1l));
    }

    private List<Long> idList( Iterable<? extends PropertyContainer> entities )
    {
        List<Long> out = new ArrayList<>();
        for ( PropertyContainer entity : entities )
        {
            out.add( entity instanceof Node ? ((Node)entity).getId() : ((Relationship)entity).getId() );
        }
        return out;
    }

    private TxStateTransactionDataSnapshot snapshot()
    {
        return new TxStateTransactionDataSnapshot( state, mock( NodeProxy.NodeLookup.class ),
                mock( RelationshipProxy.RelationshipLookups.class ), bridge );
    }
}
