/*
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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsIteratorFrom;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsPrimitiveLongIteratorFrom;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.properties.Property.noNodeProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;
import static org.neo4j.kernel.impl.api.state.StubCursors.asLabelCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;

public class IndexQueryTransactionStateTest
{
    int labelId = 2;
    int propertyKeyId = 3;
    String value = "My Value";
    IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );

    private StoreReadLayer store;
    private StoreStatement statement;
    private EntityOperations txContext;
    private KernelStatement state;
    private IndexReader indexReader;

    @Before
    public void before() throws Exception
    {
        TransactionState txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );

        int labelId1 = 10, labelId2 = 12;
        store = mock( StoreReadLayer.class );
        when( store.indexGetState( indexDescriptor ) ).thenReturn( InternalIndexState.ONLINE );
        when( store.indexesGetForLabel( labelId1 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( answerAsIteratorFrom( Collections.<IndexDescriptor>emptyList() ) );
        when( store.constraintsGetForLabel( labelId ) ).thenReturn( Collections.<NodePropertyConstraint>emptyIterator() );
        when( store.indexGetForLabelAndPropertyKey( labelId, propertyKeyId ) )
                .thenReturn( new IndexDescriptor( labelId, propertyKeyId ) );

        statement = mock( StoreStatement.class );
        when( state.getStoreStatement() ).thenReturn( statement );
        indexReader = mock( IndexReader.class );
        when( statement.getIndexReader( indexDescriptor ) ).thenReturn( indexReader );
        when( statement.getFreshIndexReader( indexDescriptor ) ).thenReturn( indexReader );

        StateHandlingStatementOperations stateHandlingOperations = new StateHandlingStatementOperations(
                store,
                new InternalAutoIndexing( Config.empty(), null ),
                mock( ConstraintIndexCreator.class ),
                mock( LegacyIndexStore.class ) );
        txContext = new ConstraintEnforcingEntityOperations(
                new StandardConstraintSemantics(), stateHandlingOperations, stateHandlingOperations, stateHandlingOperations, stateHandlingOperations );
    }

    @Test
    public void shouldExcludeRemovedNodesFromIndexQuery() throws Exception
    {
        // Given
        long nodeId = 2l;
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 1l, nodeId, 3l ) ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId ) );

        txContext.nodeDelete( state, nodeId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 1l, 3l ) ) );
    }

    @Test
    public void shouldExcludeRemovedNodeFromUniqueIndexQuery() throws Exception
    {
        // Given
        long nodeId = 1l;
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId ) );

        txContext.nodeDelete( state, nodeId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeChangedNodesWithMissingLabelFromIndexQuery() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        state.txState().nodeDoReplaceProperty( 1l, Property.noNodeProperty( 1l, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeChangedNodeWithMissingLabelFromUniqueIndexQuery() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator() );
        state.txState().nodeDoReplaceProperty( 1l, Property.noNodeProperty( 1l, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        long nodeId = 1l;
        state.txState().nodeDoReplaceProperty( nodeId, noNodeProperty( nodeId, propertyKeyId ),
                stringProperty( propertyKeyId, value ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        Cursors.<LabelItem>empty() ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( nodeId, 2l, 3l ) ) );
    }

    @Test
    public void shouldIncludeUniqueCreatedNodeWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator() );

        long nodeId = 1l;
        state.txState().nodeDoReplaceProperty( nodeId, noNodeProperty( nodeId, propertyKeyId ),
                stringProperty( propertyKeyId, value ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        Cursors.<LabelItem>empty() ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( result, equalTo( nodeId ) );
    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        long nodeId = 1l;

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        asLabelCursor() ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( nodeId, 2l, 3l ) ) );
    }

    @Test
    public void shouldIncludeExistingUniqueNodeWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator() );

        long nodeId = 2l;

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        asLabelCursor() ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( result, equalTo( nodeId ) );
    }

    @Test
    public void shouldExcludeExistingNodesWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long nodeId = 1l;
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( nodeId, 2l, 3l ) ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        asLabelCursor( labelId ) ) );

        txContext.nodeRemoveLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeExistingUniqueNodeWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long nodeId = 1l;
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        asLabelCursor( labelId ) ) );

        txContext.nodeRemoveLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeNodesWithRemovedProperty() throws Exception
    {
        // Given
        when( indexReader.seek( value ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        long nodeId = 1l;
        state.txState().nodeDoReplaceProperty( nodeId, Property.noNodeProperty( nodeId, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor(),
                        asLabelCursor( labelId ) ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexSeek( state, indexDescriptor, value );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeUniqueNodeWithRemovedProperty() throws Exception
    {
        // Given
        long nodeId = 1l;
        when( indexReader.seek( value ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn(
                asNodeCursor( nodeId,
                        asPropertyCursor( stringProperty( propertyKeyId, value ) ),
                        asLabelCursor( labelId ) ) );

        txContext.nodeRemoveProperty( state, nodeId, propertyKeyId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    private void assertNoSuchNode( long node )
    {
        assertThat( node, equalTo( NO_SUCH_NODE ) );
    }

    private static PrimitiveLongResourceIterator asPrimitiveResourceIterator( long... values )
    {
        return PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( values ), new Resource()
        {
            @Override
            public void close()
            {
            }
        } );
    }
}
