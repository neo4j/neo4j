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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.NodeProgression;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.kernel.api.properties.Property.stringProperty;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNode;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asRelationship;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.labels;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

public class TxStateTransactionDataViewTest
{
    private final ThreadToStatementContextBridge bridge = mock( ThreadToStatementContextBridge.class );
    private final Statement stmt = mock( Statement.class );
    private final StoreReadLayer ops = mock( StoreReadLayer.class );
    private final StoreStatement storeStatement = mock( StoreStatement.class );
    private final KernelTransaction transaction = mock( KernelTransaction.class );

    private final TransactionState state = new TxState();

    @Before
    public void setup()
    {
        when( bridge.get() ).thenReturn( stmt );
        when( ops.newStatement() ).thenReturn( storeStatement );
    }

    @Test
    public void showsCreatedNodes() throws Exception
    {
        // Given
        state.nodeDoCreate( 1 );
        state.nodeDoCreate( 2 );

        // When & Then
        assertThat( idList( snapshot().createdNodes() ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    public void showsDeletedNodes() throws Exception
    {
        // Given
        state.nodeDoDelete( 1L );
        state.nodeDoDelete( 2L );

        NodeItem node1 = asNode( 2L, 20L, labels( 15 ) );
        when( ops.nodeCursor( storeStatement, 2L, null ) ).thenReturn( cursor( node1 ) );

        when( ops.nodeGetProperties( storeStatement, node1, null ) )
                .thenReturn( asPropertyCursor( stringProperty( 1, "p" ) ) );

        NodeItem node2 = asNode( 1L, 21L, labels() );
        when( ops.nodeCursor( storeStatement, 1L, null ) ).thenReturn( cursor( node2 ) );
        when( ops.nodeGetProperties( storeStatement, node2, null ) ).thenReturn( asPropertyCursor() );

        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "key" );
        when( ops.labelGetName( 15 ) ).thenReturn( "label" );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedNodes() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedLabels() ).label().name(), equalTo( "label" ) );
        assertThat( single( snapshot.removedNodeProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    public void showsAddedRelationships() throws Exception
    {
        // Given
        state.relationshipDoCreate( 1, 1, 1L, 2L );
        state.relationshipDoCreate( 2, 1, 1L, 1L );

        // When & Then
        assertThat( idList( snapshot().createdRelationships() ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    public void showsRemovedRelationships() throws Exception
    {
        // Given
        state.relationshipDoDelete( 1L, 1, 1L, 2L );
        state.relationshipDoDelete( 2L, 1, 1L, 1L );

        RelationshipItem rel1 = asRelationship( 1L, 1, 1L, 2L, -1L );
        when( ops.relationshipCursor( storeStatement, 1L, null ) ).thenReturn( cursor( rel1 ) );
        when( ops.relationshipGetProperties( storeStatement, rel1, null ) ).thenReturn( asPropertyCursor() );

        RelationshipItem rel2 = asRelationship( 2L, 1, 1L, 1L, 40L );
        when( ops.relationshipCursor( storeStatement, 2L, null ) ).thenReturn( cursor( rel2 ) );
        when( ops.relationshipGetProperties( storeStatement, rel2, null ) )
                .thenReturn( asPropertyCursor( stringProperty( 1, "p" ) ) );

        when( ops.propertyKeyGetName( 1 ) ).thenReturn( "key" );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedRelationships() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedRelationshipProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    public void correctlySaysNodeIsDeleted() throws Exception
    {
        // Given
        state.nodeDoDelete( 1L );
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( 1L );
        NodeItem nodeItem = asNode( 1 );
        when( ops.nodeCursor( storeStatement, 1, null ) ).thenReturn( cursor( nodeItem ) );
        when( ops.nodeGetProperties( storeStatement, nodeItem, null ) ).thenReturn( asPropertyCursor() );

        // When & Then
        assertThat( snapshot().isDeleted( node ), equalTo( true ) );
    }

    @Test
    public void correctlySaysRelIsDeleted() throws Exception
    {
        // Given
        state.relationshipDoDelete( 1L, 1, 1L, 2L );

        Relationship rel = mock( Relationship.class );
        when( rel.getId() ).thenReturn( 1L );
        RelationshipItem relationship = asRelationship( 1L, 1, 1L, 2L, -1L );
        when( ops.relationshipCursor( storeStatement, 1L, null ) ).thenReturn( cursor( relationship ) );
        when( ops.relationshipGetProperties( storeStatement, relationship, null ) ).thenReturn( asPropertyCursor() );

        // When & Then
        assertThat( snapshot().isDeleted( rel ), equalTo( true ) );
    }

    @Test
    public void shouldListAddedNodePropertiesProperties() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        DefinedProperty prevProp = stringProperty( propertyKeyId, "prevValue" );
        state.nodeDoChangeProperty( 1L, prevProp, stringProperty( propertyKeyId, "newValue" ) );
        when( ops.propertyKeyGetName( propertyKeyId ) ).thenReturn( "theKey" );
        long propertyId = 20L;
        NodeItem node = asNode( 1L, propertyId, labels() );
        when( ops.nodeCursor( storeStatement, 1L, null ) ).thenReturn( cursor( node ) );
        when( ops.nodeGetProperty( storeStatement, node, propertyKeyId, null ) )
                .thenReturn( asPropertyCursor( prevProp ) );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().assignedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListRemovedNodeProperties() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        DefinedProperty prevProp = stringProperty( propertyKeyId, "prevValue" );
        state.nodeDoRemoveProperty( 1L, prevProp );
        when( ops.propertyKeyGetName( propertyKeyId ) ).thenReturn( "theKey" );
        long propertyId = 20L;
        NodeItem node = asNode( 1L, propertyId, labels() );
        when( ops.nodeCursor( storeStatement, 1L, null ) ).thenReturn( cursor( node ) );
        when( ops.nodeGetProperty( storeStatement, node, propertyKeyId, null ) )
                .thenReturn( asPropertyCursor( prevProp ) );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().removedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListRemovedRelationshipProperties() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        DefinedProperty prevValue = stringProperty( propertyKeyId, "prevValue" );
        state.relationshipDoRemoveProperty( 1L, prevValue );
        when( ops.propertyKeyGetName( propertyKeyId ) ).thenReturn( "theKey" );
        long propertyId = 40L;
        RelationshipItem relationshipItem = asRelationship( 1, 0, 0, 0, propertyId );
        when( ops.relationshipCursor( storeStatement, 1, null ) )
                .thenReturn( cursor( relationshipItem ) );
        when( ops.relationshipGetProperty(  storeStatement, relationshipItem, propertyKeyId, null ) )
                .thenReturn( asPropertyCursor( prevValue ) );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().removedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListAddedRelationshipProperties() throws Exception
    {
        // Given
        int propertyKeyId = 1;
        DefinedProperty prevProp = stringProperty( propertyKeyId, "prevValue" );
        state.relationshipDoReplaceProperty( 1L, prevProp, stringProperty( propertyKeyId, "newValue" ) );

        when( ops.propertyKeyGetName( propertyKeyId ) ).thenReturn( "theKey" );
        long propertyId = 40L;
        RelationshipItem relationshipItem = asRelationship( 1, 0, 0, 0, propertyId );
        when( ops.relationshipCursor( storeStatement, 1, null ) )
                .thenReturn( cursor( relationshipItem ) );
        when( ops.relationshipGetProperty( storeStatement, relationshipItem, propertyKeyId, null ) )
                .thenReturn( asPropertyCursor( prevProp ) );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().assignedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListAddedLabels() throws Exception
    {
        // Given
        state.nodeDoAddLabel( 2, 1L );
        when( ops.labelGetName( 2 ) ).thenReturn( "theLabel" );
        when( storeStatement.acquireNodeCursor( any( NodeProgression.class ) ) ).thenReturn( asNodeCursor( 1 ) );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().assignedLabels();

        // Then
        LabelEntry entry = single( labelEntries );
        assertThat( entry.label().name(), equalTo( "theLabel" ) );
        assertThat( entry.node().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListRemovedLabels() throws Exception
    {
        // Given
        state.nodeDoRemoveLabel( 2, 1L );
        when( ops.labelGetName( 2 ) ).thenReturn( "theLabel" );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().removedLabels();

        // Then
        LabelEntry entry = single( labelEntries );
        assertThat( entry.label().name(), equalTo( "theLabel" ) );
        assertThat( entry.node().getId(), equalTo( 1L ) );
    }

    @Test
    public void accessTransactionIdAndCommitTime()
    {
        long committedTransactionId = 7L;
        long commitTime = 10L;
        when( transaction.getTransactionId() ).thenReturn( committedTransactionId );
        when( transaction.getCommitTime() ).thenReturn( commitTime );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( committedTransactionId, transactionDataSnapshot.getTransactionId() );
        assertEquals( commitTime, transactionDataSnapshot.getCommitTime() );
    }

    @Test
    public void shouldGetEmptyUsernameForAnonymousContext()
    {
        when( transaction.securityContext() ).thenReturn( AnonymousContext.read() );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( "", transactionDataSnapshot.username() );
    }

    @Test
    public void shouldAccessUsernameFromAuthSubject()
    {
        AuthSubject authSubject = mock( AuthSubject.class );
        when( authSubject.username() ).thenReturn( "Christof" );
        when( transaction.securityContext() )
                .thenReturn( new SecurityContext.Frozen( authSubject, AccessMode.Static.FULL ) );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( "Christof", transactionDataSnapshot.username() );
    }

    @Test
    public void shouldAccessEmptyMetaData()
    {
        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( 0, transactionDataSnapshot.metaData().size() );
    }

    @Test
    public void shouldAccessExampleMetaData()
    {
        NodeProxy.NodeActions nodeActions = mock( NodeProxy.NodeActions.class );
        final RelationshipProxy.RelationshipActions relActions = mock( RelationshipProxy.RelationshipActions.class );
        final KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.getMetaData() ).thenReturn( genericMap( "username", "Igor" ) );
        TxStateTransactionDataSnapshot transactionDataSnapshot =
                new TxStateTransactionDataSnapshot( state, nodeActions, relActions, ops, storeStatement, transaction );
        assertEquals( 1, transactionDataSnapshot.metaData().size() );
        assertThat( "Expected metadata map to contain defined username", transactionDataSnapshot.metaData(),
                equalTo( genericMap( "username", "Igor" ) ) );
    }

    private List<Long> idList( Iterable<? extends PropertyContainer> entities )
    {
        List<Long> out = new ArrayList<>();
        for ( PropertyContainer entity : entities )
        {
            out.add( entity instanceof Node ? ((Node) entity).getId() : ((Relationship) entity).getId() );
        }
        return out;
    }

    private TxStateTransactionDataSnapshot snapshot()
    {
        NodeProxy.NodeActions nodeActions = mock( NodeProxy.NodeActions.class );
        final RelationshipProxy.RelationshipActions relActions = mock( RelationshipProxy.RelationshipActions.class );
        return new TxStateTransactionDataSnapshot( state, nodeActions, relActions, ops, storeStatement, transaction );
    }
}
