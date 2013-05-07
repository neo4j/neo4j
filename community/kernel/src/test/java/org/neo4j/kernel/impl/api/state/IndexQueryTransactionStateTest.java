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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        txContext.deleteNode( 2l );

        // When
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( store.getNodePropertyValue( 1l, propertyKeyId ) ).thenReturn( value );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( true );

        when( store.getNodePropertyValue( 1l, propertyKeyId ) ).thenReturn( value );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.removeLabelFromNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

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
        when( store.exactIndexLookup( indexDescriptor, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( true );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( Collections.<Long>emptySet(), asSet( 1l ) ) );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    // exists

    private final long labelId1 = 10, labelId2 = 12;

    private StatementContext store;
    private OldTxStateBridge oldTxState;
    private TxState state;
    private StateHandlingStatementContext txContext;

    @Before
    public void before() throws Exception
    {
        store = mock( StatementContext.class );
        when( store.getIndexes( labelId1 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.getIndexes( labelId2 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.getIndexes() ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.addIndex( anyLong(), anyLong() ) ).thenAnswer( new Answer<IndexDescriptor>()
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

        state = new TxState( oldTxState, mock( PersistenceManager.class ),
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
