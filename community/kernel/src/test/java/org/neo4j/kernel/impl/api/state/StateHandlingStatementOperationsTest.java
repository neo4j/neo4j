/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;

public class StateHandlingStatementOperationsTest
{
    // Note: Most of the behavior of this class is tested in separate classes,
    // based on the category of state being
    // tested. This contains general tests or things that are common to all
    // types of state.

    StoreReadLayer inner = mock( StoreReadLayer.class );

    @Test
    public void shouldNeverDelegateWrites() throws Exception
    {
        KernelStatement state = mockedState();

        when( state.txState() ).thenReturn( new TxState() );
        StoreStatement storeStatement = mock( StoreStatement.class );
        when( state.getStoreStatement() ).thenReturn( storeStatement );
        when( storeStatement.acquireSingleNodeCursor( anyLong() ) ).
                thenReturn( asNodeCursor( 0 ) );

        StateHandlingStatementOperations ctx = newTxStateOps( inner );

        // When
        ctx.indexCreate( state, 0, 0 );
        ctx.nodeAddLabel( state, 0, 0 );
        ctx.indexDrop( state, new IndexDescriptor( 0, 0 ) );
        ctx.nodeRemoveLabel( state, 0, 0 );

        // one for add and one for remove
        verify( storeStatement, times( 2 ) ).acquireSingleNodeCursor( 0 );
        verifyNoMoreInteractions( storeStatement );
    }

    @Test
    public void shouldNotAddConstraintAlreadyExistsInTheStore() throws Exception
    {
        // given
        PropertyConstraint constraint = new UniquenessConstraint( 10, 66 );
        TransactionState txState = mock( TransactionState.class );
        when( txState.nodesWithLabelChanged( anyInt() ) ).thenReturn( new DiffSets() );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( 10, 66 ) )
                .thenAnswer( asAnswer( asList( constraint ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );

        // when
        context.uniquePropertyConstraintCreate( state, 10, 66 );

        // then
        verify( txState ).constraintIndexDoUnRemove( any( IndexDescriptor.class ) );
    }

    @Test
    public void shouldGetConstraintsByLabelAndProperty() throws Exception
    {
        // given
        PropertyConstraint constraint = new UniquenessConstraint( 10, 66 );
        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( 10, 66 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );

        // when
        Set<NodePropertyConstraint> result = asSet(
                asIterable( context.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) ) );

        // then
        assertEquals( asSet( constraint ), result );
    }

    @Test
    public void shouldGetConstraintsByLabel() throws Exception
    {
        // given
        PropertyConstraint constraint2 = new UniquenessConstraint( 11, 99 );
        PropertyConstraint constraint1 = new UniquenessConstraint( 11, 66 );

        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( 10, 66 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabelAndPropertyKey( 11, 99 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabel( 10 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabel( 11 ) )
                .thenAnswer( asAnswer( asIterable( constraint1 ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );
        context.uniquePropertyConstraintCreate( state, 11, 99 );

        // when
        Set<NodePropertyConstraint> result = asSet( asIterable( context.constraintsGetForLabel( state, 11 ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    @Test
    public void shouldGetAllConstraints() throws Exception
    {
        // given
        PropertyConstraint constraint1 = new UniquenessConstraint( 10, 66 );
        PropertyConstraint constraint2 = new UniquenessConstraint( 11, 99 );

        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( 10, 66 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabelAndPropertyKey( 11, 99 ) )
                .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetAll() ).thenAnswer( asAnswer( asIterable( constraint2 ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );

        // when
        Set<PropertyConstraint> result = asSet( asIterable( context.constraintsGetAll( state ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexScan() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        IndexDescriptor index = new IndexDescriptor( 1, 2 );
        when( txState.indexUpdatesForScanOrSeek( index, null ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodesGetFromIndexScan( statement, index ) ).thenReturn(
                IteratorUtil.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexScan( statement, index );

        // Then
        assertEquals( asSet( 42L, 43L ), asSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexSeek() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        IndexDescriptor index = new IndexDescriptor( 1, 2 );
        when( txState.indexUpdatesForScanOrSeek( index, "value" ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodesGetFromIndexSeek( statement, index, "value" ) ).thenReturn(
                IteratorUtil.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexSeek( statement, index, "value" );

        // Then
        assertEquals( asSet( 42L, 43L ), asSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexRangeSeekByPrefix() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        IndexDescriptor index = new IndexDescriptor( 1, 2 );
        when( txState.indexUpdatesForRangeSeekByPrefix( index, "prefix" ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodesGetFromIndexRangeSeekByPrefix( statement, index, "prefix" ) ).thenReturn(
                IteratorUtil.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByPrefix( statement, index, "prefix" );

        // Then
        assertEquals( asSet( 42L, 43L ), asSet( results ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldConsiderTransactionStateDuringIndexBetweenRangeSeekByNumber() throws Exception
    {
        // Given
        final int propertyKey = 2;
        final int inRange = 15;
        int lower = 10;
        int upper = 20;

        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        IndexDescriptor index = new IndexDescriptor( 1, propertyKey );
        when( txState.indexUpdatesForRangeSeekByNumber( index, lower, true, upper, false ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );
        when( txState.augmentSingleNodeCursor( any( Cursor.class ), anyLong() ) ).thenAnswer(
                new Answer<Cursor<NodeItem>>()
                {
                    @Override
                    public Cursor<NodeItem> answer( InvocationOnMock invocationOnMock ) throws Throwable
                    {
                        long nodeId = (long) invocationOnMock.getArguments()[1];
                        return asNodeCursor( nodeId, asPropertyCursor( intProperty( propertyKey, inRange ) ),
                                Cursors.<LabelItem>empty() );
                    }
                } );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodesGetFromInclusiveNumericIndexRangeSeek( statement, index, lower, upper ) ).thenReturn(
                IteratorUtil.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );
        StoreStatement storeStatement = mock( StoreStatement.class );
        when( statement.getStoreStatement() ).thenReturn( storeStatement );
        when( storeStatement.acquireSingleNodeCursor( anyLong() ) ).thenAnswer(
                new Answer<Cursor<NodeItem>>()
                {
                    @Override
                    public Cursor<NodeItem> answer( InvocationOnMock invocationOnMock ) throws Throwable
                    {
                        long nodeId = (long) invocationOnMock.getArguments()[0];
                        return asNodeCursor( nodeId, asPropertyCursor( intProperty( propertyKey, inRange ) ),
                                Cursors.<LabelItem>empty() );
                    }
                } );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, true, upper, false );

        // Then
        assertEquals( asSet( 42L, 43L ), asSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexBetweenRangeSeekByString() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        IndexDescriptor index = new IndexDescriptor( 1, 2 );
        when( txState.indexUpdatesForRangeSeekByString( index, "Anne", true, "Bill", false ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodesGetFromIndexRangeSeekByString( statement, index, "Anne", true, "Bill", false ) )
                .thenReturn(
                        IteratorUtil.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByString( statement, index, "Anne", true, "Bill", false );

        // Then
        assertEquals( asSet( 42L, 43L ), asSet( results ) );
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

    private StateHandlingStatementOperations newTxStateOps( StoreReadLayer delegate )
    {
        return new StateHandlingStatementOperations( delegate,
                mock( LegacyPropertyTrackers.class ), mock( ConstraintIndexCreator.class ),
                mock( LegacyIndexStore.class ) );
    }
}
