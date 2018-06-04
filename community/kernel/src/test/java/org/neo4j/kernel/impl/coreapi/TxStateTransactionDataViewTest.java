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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class TxStateTransactionDataViewTest
{
    private static final long[] NO_LABELS = new long[0];
    private final ThreadToStatementContextBridge bridge = mock( ThreadToStatementContextBridge.class );
    private final Statement stmt = mock( Statement.class );
    private final StubStorageCursors ops = new StubStorageCursors();
    private final KernelTransaction transaction = mock( KernelTransaction.class );

    private final TransactionState state = new TxState();

    @Before
    public void setup()
    {
        when( bridge.get() ).thenReturn( stmt );
    }

    @Test
    public void showsCreatedNodes()
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

        int labelId = ops.labelGetOrCreateForName( "label" );
        ops.withNode( 1, new long[]{labelId}, genericMap( "key", Values.of( "p" ) ) );
        ops.withNode( 2, NO_LABELS );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedNodes() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedLabels() ).label().name(), equalTo( "label" ) );
        assertThat( single( snapshot.removedNodeProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    public void showsAddedRelationships()
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

        ops.withRelationship( 1, 1, 1, 2 );
        ops.withRelationship( 2, 1, 1, 1, genericMap( "key", Values.of( "p") ) );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedRelationships() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedRelationshipProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    public void correctlySaysNodeIsDeleted()
    {
        // Given
        state.nodeDoDelete( 1L );
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( 1L );
        ops.withNode( 1 );

        // When & Then
        assertThat( snapshot().isDeleted( node ), equalTo( true ) );
    }

    @Test
    public void correctlySaysRelIsDeleted()
    {
        // Given
        state.relationshipDoDelete( 1L, 1, 1L, 2L );

        Relationship rel = mock( Relationship.class );
        when( rel.getId() ).thenReturn( 1L );
        ops.withRelationship( 1L, 1L, 1, 2L );

        // When & Then
        assertThat( snapshot().isDeleted( rel ), equalTo( true ) );
    }

    @Test
    public void shouldListAddedNodePropertiesProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyGetOrCreateForName( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.nodeDoChangeProperty( 1L, propertyKeyId, Values.of( "newValue" ) );
        ops.withNode( 1, NO_LABELS, genericMap( "theKey", prevValue ) );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().assignedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommitedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListRemovedNodeProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyGetOrCreateForName( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.nodeDoRemoveProperty( 1L, propertyKeyId );
        ops.withNode( 1, NO_LABELS, genericMap( "theKey", prevValue ) );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().removedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommitedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListRemovedRelationshipProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyGetOrCreateForName( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.relationshipDoRemoveProperty( 1L, propertyKeyId );
        ops.withRelationship( 1, 0, 0, 0, genericMap( "theKey", prevValue ) );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().removedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommitedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListAddedRelationshipProperties() throws Exception
    {
        // Given
        Value prevValue = Values.of( "prevValue" );
        int propertyKeyId = ops.propertyKeyGetOrCreateForName( "theKey" );
        state.relationshipDoReplaceProperty( 1L, propertyKeyId, prevValue, Values.of( "newValue" ) );
        ops.withRelationship( 1, 0, 0, 0, genericMap( "theKey", prevValue ) );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().assignedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommitedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldListAddedLabels() throws Exception
    {
        // Given
        int labelId = ops.labelGetOrCreateForName( "theLabel" );
        state.nodeDoAddLabel( labelId, 1L );

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
        int labelId = ops.labelGetOrCreateForName( "theLabel" );
        state.nodeDoRemoveLabel( labelId, 1L );

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
        when( transaction.securityContext() ).thenReturn( AnonymousContext.read().authorize( s -> -1 ) );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( "", transactionDataSnapshot.username() );
    }

    @Test
    public void shouldAccessUsernameFromAuthSubject()
    {
        AuthSubject authSubject = mock( AuthSubject.class );
        when( authSubject.username() ).thenReturn( "Christof" );
        when( transaction.securityContext() )
                .thenReturn( new SecurityContext( authSubject, AccessMode.Static.FULL ) );

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
        EmbeddedProxySPI spi = mock( EmbeddedProxySPI.class );
        final KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.getMetaData() ).thenReturn( genericMap( "username", "Igor" ) );
        TxStateTransactionDataSnapshot transactionDataSnapshot =
                new TxStateTransactionDataSnapshot( state, spi, ops, transaction );
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
        EmbeddedProxySPI spi = mock( EmbeddedProxySPI.class );
        when( spi.newNodeProxy( anyLong() ) ).thenAnswer( invocation -> new NodeProxy( spi, invocation.getArgument( 0 ) ) );
        when( spi.newRelationshipProxy( anyLong() ) ).thenAnswer( invocation -> new RelationshipProxy( spi, invocation.getArgument( 0 ) ) );
        when( spi.newRelationshipProxy( anyLong(), anyLong(), anyInt(), anyLong() ) ).thenAnswer(
                invocation -> new RelationshipProxy( spi, invocation.getArgument( 0 ), invocation.getArgument( 1 ),
                        invocation.getArgument( 2 ), invocation.getArgument( 3 ) ) );
        return new TxStateTransactionDataSnapshot( state, spi, ops, transaction );
    }
}
