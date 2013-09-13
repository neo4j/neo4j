/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.KernelStatement;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsIteratorFrom;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsPrimitiveLongIteratorFrom;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class IndexQueryTransactionStateTest
{

    @Test
    public void shouldExcludeRemovedNodesFromIndexQuery() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 1l, 2l, 3l ) ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );
        when( oldTxState.hasChanges() ).thenReturn( true );

        txContext.nodeDelete( state, 2l );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 3l ) ) );
    }

    @Test
    public void shouldExcludeRemovedNodeFromUniqueIndexQuery() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( 1l );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );
        when( oldTxState.hasChanges() ).thenReturn( true );

        txContext.nodeDelete( state, 1l );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeChangedNodesWithMissingLabelFromIndexQuery() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( asSet( 1l ), Collections.<Long>emptySet() ) );
        when( oldTxState.hasChanges() ).thenReturn( true );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeChangedNodeWithMissingLabelFromUniqueIndexQuery() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );
        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( asSet( 1l ), Collections.<Long>emptySet() ) );
        when( oldTxState.hasChanges() ).thenReturn( true );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );
        when( store.nodeGetProperty( eq( state ), anyLong(), eq( propertyKeyId ) ) ).thenReturn( Property
                .noNodeProperty( 1, propertyKeyId ) );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( IteratorUtil
                .<DefinedProperty>emptyIterator() );

        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        txContext.nodeAddLabel( state, 1l, labelId );
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldIncludeUniqueCreatedNodeWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );

        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );
        when( store.nodeGetProperty( eq( state ), anyLong(), eq( propertyKeyId ) ) ).thenReturn( Property
                .noNodeProperty( 1, propertyKeyId ) );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( IteratorUtil
                .<DefinedProperty>emptyIterator() );
        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( false );

        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        txContext.nodeAddLabel( state, 1l, labelId );

        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( result, equalTo( 1l ) );

    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( false );
        DefinedProperty stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( state, 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.nodeAddLabel( state, 1l, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldIncludeExistingUniqueNodeWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );

        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );
        when( store.nodeHasLabel( state, 2l, labelId ) ).thenReturn( false );

        DefinedProperty stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( state, 2l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.nodeAddLabel( state, 2l, labelId );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( result, equalTo( 2l ) );
    }

    @Test
    public void shouldExcludeExistingNodesWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 1l, 2l, 3l ) ) );
        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( true );

        DefinedProperty stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( state, 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.nodeRemoveLabel( state, 1l, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeExistingUniqueNodeWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( 1l );
        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( true );

        DefinedProperty stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( state, 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( eq( state ), anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.nodeRemoveLabel( state, 1l, labelId );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldExcludeNodesWithRemovedProperty() throws Exception
    {
        // Given
        int labelId = 2, propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( true );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( Collections.<Long>emptySet(), asSet( 1l ) ) );

        txContext.nodeAddLabel( state, 1l, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeUniqueNodeWithRemovedProperty() throws Exception
    {
        // Given
        int labelId = 2;
        int propertyKeyId = 3;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );

        when( store.nodeHasLabel( state, 1l, labelId ) ).thenReturn( true );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<>( Collections.<Long>emptySet(), asSet( 1l ) ) );
        when( oldTxState.hasChanges() ).thenReturn( true );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    // exists

    private StatementOperations store;
    private OldTxStateBridge oldTxState;
    private StateHandlingStatementOperations txContext;
    private KernelStatement state;

    @Before
    public void before() throws Exception
    {
        int labelId1 = 10, labelId2 = 12;
        store = mock( StatementOperations.class );
        when( store.indexesGetForLabel( state, labelId1 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( state, labelId2 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll( state ) ).then( answerAsIteratorFrom( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexCreate( eq( state ), anyInt(), anyInt() ) ).thenAnswer( new Answer<IndexDescriptor>()
        {
            @Override
            public IndexDescriptor answer( InvocationOnMock invocation ) throws Throwable
            {
                return new IndexDescriptor(
                        (Integer) invocation.getArguments()[0],
                        (Integer) invocation.getArguments()[1] );
            }
        } );

        oldTxState = mock( OldTxStateBridge.class );

        TxState txState = new TxStateImpl( oldTxState, mock( PersistenceManager.class ),
                mock( TxState.IdGeneration.class ) );
        state = StatementOperationsTestHelper.mockedState( txState );
        txContext = new StateHandlingStatementOperations( store, store, mock( AuxiliaryStoreOperations.class ),
                mock( ConstraintIndexCreator.class ) );
    }

    private void assertNoSuchNode( long node )
    {
        assertThat( node, equalTo( NO_SUCH_NODE ) );
    }
}
