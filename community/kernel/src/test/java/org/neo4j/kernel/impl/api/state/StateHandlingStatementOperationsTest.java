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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asIterable;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;
import static org.neo4j.test.MockedNeoStores.basicMockedNeoStores;

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
        when( txState.nodesWithLabelChanged( anyInt() ) ).thenReturn( new DiffSets<Long>() );
        when( txState.hasChanges() ).thenReturn( true );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( 10, 66 ) )
                .thenAnswer( invocation -> Iterators.iterator( constraint ) );
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
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );

        // when
        Set<NodePropertyConstraint> result = Iterables.asSet(
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
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabelAndPropertyKey( 11, 99 ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabel( 10 ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabel( 11 ) )
                .thenAnswer( invocation -> Iterators.iterator( constraint1 ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );
        context.uniquePropertyConstraintCreate( state, 11, 99 );

        // when
        Set<NodePropertyConstraint> result = Iterables.asSet( asIterable( context.constraintsGetForLabel( state, 11 ) ) );

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
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabelAndPropertyKey( 11, 99 ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetAll() )
                .thenAnswer( invocation -> Iterators.iterator( constraint2 ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, 10, 66 );

        // when
        Set<PropertyConstraint> result = Iterables.asSet( asIterable( context.constraintsGetAll( state ) ) );

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
        IndexReader indexReader = addMockedIndexReader( statement );
        when( indexReader.scan() ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexScan( statement, index );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
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
        IndexReader indexReader = addMockedIndexReader( statement );
        when( indexReader.seek( "value" ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexSeek( statement, index, "value" );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
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
        IndexReader indexReader = addMockedIndexReader( statement );
        when( indexReader.rangeSeekByPrefix( "prefix" ) )
                .thenReturn( PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByPrefix( statement, index, "prefix" );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
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
        StorageStatement storageStatement = mock( StorageStatement.class );
        when( statement.getStoreStatement() ).thenReturn( storageStatement );
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
        IndexReader indexReader = addMockedIndexReader( storageStatement );
        when( indexReader.rangeSeekByNumberInclusive( lower, upper ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );
        when( storageStatement.acquireSingleNodeCursor( anyLong() ) ).thenAnswer(
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
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, true,
                upper, false );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
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
        IndexReader indexReader = addMockedIndexReader( statement );
        when( indexReader.rangeSeekByString( "Anne", true, "Bill", false ) ) .thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.nodesGetFromIndexRangeSeekByString( statement, index, "Anne", true, "Bill", false );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void nodeGetFromUniqueIndexSeekClosesIndexReader() throws Exception
    {
        KernelStatement kernelStatement = mock( KernelStatement.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        IndexReader indexReader = mock( IndexReader.class );

        when( indexReader.seek( any() ) ).thenReturn( PrimitiveLongCollections.emptyIterator() );
        when( storeStatement.getFreshIndexReader( any() ) ).thenReturn( indexReader );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );

        StateHandlingStatementOperations operations = newTxStateOps( mock( StoreReadLayer.class ) );

        operations.nodeGetFromUniqueIndexSeek( kernelStatement, new IndexDescriptor( 1, 1 ), "foo" );

        verify( indexReader ).close();
    }

    @Test
    public void nodeCursorGetFromUniqueIndexSeekClosesIndexReader() throws Exception
    {
        KernelStatement kernelStatement = mock( KernelStatement.class );
        IndexReader indexReader = mock( IndexReader.class );
        StoreStatement storeStatement = new StoreStatementWithSingleFreshIndexReader( indexReader );

        when( indexReader.seek( any() ) ).thenReturn( PrimitiveLongCollections.emptyIterator() );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );

        StateHandlingStatementOperations operations = newTxStateOps( mock( StoreReadLayer.class ) );

        try ( Cursor<NodeItem> cursor = operations.nodeCursorGetFromUniqueIndexSeek( kernelStatement,
                new IndexDescriptor( 1, 1 ), "foo" ) )
        {
            while ( cursor.next() )
            {
                fail( "Cursor should be empty" );
            }
        }
        verify( indexReader ).close();
    }

    private StateHandlingStatementOperations newTxStateOps( StoreReadLayer delegate )
    {
        return new StateHandlingStatementOperations( delegate,
                mock( InternalAutoIndexing.class ), mock( ConstraintIndexCreator.class ),
                mock( LegacyIndexStore.class ) );
    }

    private IndexReader addMockedIndexReader( KernelStatement kernelStatement ) throws IndexNotFoundKernelException
    {
        StorageStatement storageStatement = mock( StorageStatement.class );
        when( kernelStatement.getStoreStatement() ).thenReturn( storageStatement );
        return addMockedIndexReader( storageStatement );
    }

    private IndexReader addMockedIndexReader( StorageStatement storeStatement )
            throws IndexNotFoundKernelException
    {
        IndexReader indexReader = mock( IndexReader.class );
        when( storeStatement.getIndexReader( any( IndexDescriptor.class ) ) ).thenReturn( indexReader );
        return indexReader;
    }

    private static class StoreStatementWithSingleFreshIndexReader extends StoreStatement
    {
        final IndexReader reader;

        StoreStatementWithSingleFreshIndexReader( IndexReader reader )
        {
            super( basicMockedNeoStores(), () -> mock( IndexReaderFactory.class ),
                    () -> mock( LabelScanReader.class ),  new ReentrantLockService() );
            this.reader = reader;
        }

        @Override
        public IndexReader getFreshIndexReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
        {
            return reader;
        }
    }
}
