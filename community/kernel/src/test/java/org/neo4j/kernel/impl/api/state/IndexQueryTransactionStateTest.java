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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreStatement;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.labels;
import static org.neo4j.test.mockito.answer.Neo4jMockitoAnswers.answerAsIteratorFrom;
import static org.neo4j.test.mockito.answer.Neo4jMockitoAnswers.answerAsPrimitiveLongIteratorFrom;

public class IndexQueryTransactionStateTest
{
    int labelId = 2;
    int propertyKeyId = 3;
    String value = "My Value";
    IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( labelId, propertyKeyId  );
    List<IndexDescriptor> indexes = Collections.singletonList( indexDescriptor );
    IndexQuery.ExactPredicate withValue = IndexQuery.exact( propertyKeyId, value );

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

        store = mock( StoreReadLayer.class );
        when( store.indexGetState( indexDescriptor ) ).thenReturn( InternalIndexState.ONLINE );
        when( store.indexesGetForLabel( labelId ) ).then( answerAsIteratorFrom( indexes ) );
        when( store.indexesGetAll() ).then( answerAsIteratorFrom( indexes ) );
        when( store.indexesGetRelatedToProperty( propertyKeyId ) ).then( answerAsIteratorFrom( indexes ) );
        when( store.constraintsGetForLabel( labelId ) ).thenReturn( Collections.emptyIterator() );
        when( store.indexGetForSchema( indexDescriptor.schema() ) ).thenReturn( indexDescriptor );

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
        long nodeId = 2L;
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 1L, nodeId, 3L ) ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId ) );

        txContext.nodeDelete( state, nodeId );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 1L, 3L ) ) );
    }

    @Test
    public void shouldExcludeRemovedNodeFromUniqueIndexQuery() throws Exception
    {
        // Given
        long nodeId = 1L;
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId ) );

        txContext.nodeDelete( state, nodeId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeChangedNodesWithMissingLabelFromIndexQuery() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2L, 3L ) ) );

        state.txState().nodeDoAddProperty( 1L, intProperty( propertyKeyId, 10 ) );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2L, 3L ) ) );
    }

    @Test
    public void shouldExcludeChangedNodeWithMissingLabelFromUniqueIndexQuery() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator() );
        state.txState().nodeDoAddProperty( 1L, intProperty( propertyKeyId, 10 ) );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2L, 3L ) ) );

        long nodeId = 1L;
        state.txState().nodeDoAddProperty( nodeId, stringProperty( propertyKeyId, value ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId, 40L ) );
        mockStoreProperty();

        when( store.indexesGetForLabel( labelId ) ).thenReturn( indexes.iterator() );
        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( nodeId, 2L, 3L ) ) );
    }

    @Test
    public void shouldIncludeUniqueCreatedNodeWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator() );

        long nodeId = 1L;
        state.txState().nodeDoAddProperty( nodeId, stringProperty( propertyKeyId, value ) );

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId, 40L ) );
        mockStoreProperty();

        when( store.indexesGetForLabel( labelId ) ).thenReturn( indexes.iterator() );
        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertThat( result, equalTo( nodeId ) );
    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2L, 3L ) ) );

        long nodeId = 1L;

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId, 40L ) );
        mockStoreProperty();

        when( store.indexesGetForLabel( labelId ) ).thenReturn( indexes.iterator() );
        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( nodeId, 2L, 3L ) ) );
    }

    @Test
    public void shouldIncludeExistingUniqueNodeWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator() );

        long nodeId = 2L;

        when( statement.acquireSingleNodeCursor( nodeId ) ).thenReturn( asNodeCursor( nodeId, 40L ) );
        mockStoreProperty();

        when( store.indexesGetForLabel( labelId ) ).thenReturn( indexes.iterator() );
        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertThat( result, equalTo( nodeId ) );
    }

    @Test
    public void shouldExcludeExistingNodesWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long nodeId = 1L;
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( nodeId, 2L, 3L ) ) );

        when( statement.acquireSingleNodeCursor( nodeId ) )
                .thenReturn( asNodeCursor( nodeId, 40L, labels( labelId ) ) );
        mockStoreProperty();

        txContext.nodeRemoveLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2L, 3L ) ) );
    }

    @Test
    public void shouldExcludeExistingUniqueNodeWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long nodeId = 1L;
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) )
                .thenReturn( asNodeCursor( nodeId, 40L, labels( labelId ) ) );
        mockStoreProperty();

        txContext.nodeRemoveLabel( state, nodeId, labelId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeNodesWithRemovedProperty() throws Exception
    {
        // Given
        when( indexReader.query( withValue ) ).then( answerAsPrimitiveLongIteratorFrom( asList( 2L, 3L ) ) );

        long nodeId = 1L;
        state.txState().nodeDoAddProperty( nodeId, intProperty( propertyKeyId, 10 ) );

        when( statement.acquireSingleNodeCursor( nodeId ) )
                .thenReturn( asNodeCursor( nodeId, labels( labelId ) ) );

        txContext.nodeAddLabel( state, nodeId, labelId );

        // When
        PrimitiveLongIterator result = txContext.indexQuery( state, indexDescriptor, withValue );

        // Then
        assertThat( PrimitiveLongCollections.toSet( result ), equalTo( asSet( 2L, 3L ) ) );
    }

    @Test
    public void shouldExcludeUniqueNodeWithRemovedProperty() throws Exception
    {
        // Given
        long nodeId = 1L;
        when( indexReader.query( withValue ) ).thenReturn( asPrimitiveResourceIterator( nodeId ) );

        when( statement.acquireSingleNodeCursor( nodeId ) )
                .thenReturn( asNodeCursor( nodeId, 40, labels( labelId ) ) );
        mockStoreProperty();

        txContext.nodeRemoveProperty( state, nodeId, propertyKeyId );

        // When
        long result = txContext.nodeGetFromUniqueIndexSeek( state, indexDescriptor, withValue );

        // Then
        assertNoSuchNode( result );
    }

    private void mockStoreProperty()
    {
        when( store.nodeGetProperties( eq( statement ), any( NodeItem.class ), any( AssertOpen.class ) ) )
                .thenReturn( asPropertyCursor( stringProperty( propertyKeyId, value ) ) );
        when( store.nodeGetProperty( eq( statement ), any( NodeItem.class ), eq( propertyKeyId ),
                any( AssertOpen.class ) ) )
                .thenReturn( asPropertyCursor( stringProperty( propertyKeyId, value ) ) );
    }

    private void assertNoSuchNode( long node )
    {
        assertThat( node, equalTo( NO_SUCH_NODE ) );
    }

    private static PrimitiveLongResourceIterator asPrimitiveResourceIterator( long... values )
    {
        return PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( values ), () -> {} );
    }
}
