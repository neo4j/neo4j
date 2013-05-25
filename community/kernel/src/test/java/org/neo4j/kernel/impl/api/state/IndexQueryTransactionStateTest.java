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
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class IndexQueryTransactionStateTest
{

    @Test
    public void shouldExcludeRemovedNodesFromIndexQuery() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.nodeDelete( 2l );

        // When
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 3l ) ) );
    }

    @Test
    public void shouldExcludeChangedNodesWithMissingLabelFromIndexQuery() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );
        when( store.nodeGetProperty( anyLong(), eq( propertyKeyId ) ) ).thenReturn( Property.noNodeProperty( 1, propertyKeyId ) );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( IteratorUtil.<Property>emptyIterator() );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        txContext.nodeAddLabel( 1l, labelId );
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        Property stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.nodeAddLabel( 1l, labelId );
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldExcludeExistingNodesWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );

        Property stringProperty = Property.stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.nodeRemoveLabel( 1l, labelId );
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }


    @Test
    public void shouldExcludeNodesWithRemovedProperty() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.nodesGetFromIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( Collections.<Long>emptySet(), asSet( 1l ) ) );

        // When
        txContext.nodeAddLabel( 1l, labelId );
        Iterator<Long> result = txContext.nodesGetFromIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    // exists

    private StatementContext store;
    private OldTxStateBridge oldTxState;
    private StateHandlingStatementContext txContext;

    @Before
    public void before() throws Exception
    {
        long labelId1 = 10, labelId2 = 12;
        store = mock( StatementContext.class );
        when( store.indexesGetForLabel( labelId1 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexCreate( anyLong(), anyLong() ) ).thenAnswer( new Answer<IndexDescriptor>()
        {
            @Override
            public IndexDescriptor answer( InvocationOnMock invocation ) throws Throwable
            {
                return new IndexDescriptor(
                        (Long) invocation.getArguments()[0],
                        (Long) invocation.getArguments()[1] );
            }
        } );

        oldTxState = mock( OldTxStateBridge.class );

        TxState state = new TxState( oldTxState, mock( PersistenceManager.class ),
                                     mock( TxState.IdGeneration.class ) );

        txContext = new StateHandlingStatementContext( store, mock( SchemaStateOperations.class),
                                                       state, mock( ConstraintIndexCreator.class ) );
    }

    private static <T> Answer<Iterator<T>> asAnswer( final Iterable<T> values )
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer( InvocationOnMock invocation ) throws Throwable
            {
                return values.iterator();
            }
        };
    }
}
