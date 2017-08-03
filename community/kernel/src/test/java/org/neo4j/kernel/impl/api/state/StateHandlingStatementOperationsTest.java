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

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.legacyindex.AutoIndexOperations;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreStatement;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asIterable;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.iterator;
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

    private LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 10, 66 );
    private IndexDescriptor index = IndexDescriptorFactory.forLabel( 1, 2 );

    @Test
    public void shouldNeverDelegateWrites() throws Exception
    {
        KernelStatement state = mockedState();

        when( state.txState() ).thenReturn( new TxState() );
        StoreStatement storeStatement = mock( StoreStatement.class );
        when( state.getStoreStatement() ).thenReturn( storeStatement );
        when( inner.indexesGetForLabel( 0 ) ).thenReturn( iterator( IndexDescriptorFactory.forLabel( 0, 0 ) ) );
        when( storeStatement.acquireSingleNodeCursor( anyLong() ) ).thenReturn( asNodeCursor( 0 ) );
        when( inner.nodeGetProperties( eq( storeStatement ), any( NodeItem.class ), any( AssertOpen.class ) ) ).
                thenReturn( asPropertyCursor() );

        StateHandlingStatementOperations ctx = newTxStateOps( inner );

        // When
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 0 );
        ctx.indexCreate( state, descriptor );
        ctx.nodeAddLabel( state, 0, 0 );
        ctx.indexDrop( state, IndexDescriptorFactory.forSchema( descriptor ) );
        ctx.nodeRemoveLabel( state, 0, 0 );

        // one for add and one for remove
        verify( storeStatement, times( 2 ) ).acquireSingleNodeCursor( 0 );
        verifyNoMoreInteractions( storeStatement );
    }

    @Test
    public void shouldNotAddConstraintAlreadyExistsInTheStore() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        TransactionState txState = mock( TransactionState.class );
        when( txState.nodesWithLabelChanged( anyInt() ) ).thenReturn( new DiffSets<Long>() );
        when( txState.hasChanges() ).thenReturn( true );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForSchema( any() ) ).thenReturn( iterator( constraint ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );

        // when
        context.uniquePropertyConstraintCreate( state, descriptor );

        // then
        verify( txState ).indexDoUnRemove( eq( constraint.ownedIndexDescriptor() ) );
    }

    @Test
    public void shouldGetConstraintsByLabelAndProperty() throws Exception
    {
        // given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForSchema( constraint.schema() ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, descriptor );

        // when
        Set<ConstraintDescriptor> result = Iterables.asSet(
                asIterable( context.constraintsGetForSchema( state, constraint.schema() ) ) );

        // then
        assertEquals( asSet( constraint ), result );
    }

    @Test
    public void shouldGetConstraintsByLabel() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 2, 3 );
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 4, 5 );

        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForSchema( constraint1.schema() ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForSchema( constraint2.schema() ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabel( 1 ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForLabel( 2 ) )
                .thenAnswer( invocation -> iterator( constraint1 ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, constraint1.ownedIndexDescriptor().schema() );
        context.uniquePropertyConstraintCreate( state, constraint2.ownedIndexDescriptor().schema() );

        // when
        Set<ConstraintDescriptor> result = Iterables.asSet( asIterable( context.constraintsGetForLabel( state, 2 ) ) );

        // then
        assertEquals( asSet( constraint1 ), result );
    }

    @Test
    public void shouldGetAllConstraints() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 2, 3 );
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 4, 5 );

        TransactionState txState = new TxState();
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForSchema( constraint1.schema() ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetForSchema( constraint2.schema() ) )
                .thenAnswer( invocation -> Iterators.emptyIterator() );
        when( inner.constraintsGetAll() )
                .thenAnswer( invocation -> iterator( constraint2 ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquePropertyConstraintCreate( state, constraint1.schema() );
        context.uniquePropertyConstraintCreate( state, constraint2.schema() );

        // when
        Set<ConstraintDescriptor> result = Iterables.asSet( asIterable( context.constraintsGetAll( state ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexScanWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForScan( index ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery query = IndexQuery.exists( index.schema().getPropertyId() );
        when( indexReader.query( query ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, query );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexSeekWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForSeek( index, OrderedPropertyValues.ofUndefined( "value" ) ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.ExactPredicate query = IndexQuery.exact( index.schema().getPropertyId(), "value" );
        when( indexReader.query( query ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, query );

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
        when( txState.indexUpdatesForRangeSeekByPrefix( index, "prefix" ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.StringPrefixPredicate query = IndexQuery.stringPrefix( index.schema().getPropertyId(), "prefix" );
        when( indexReader.query( query ) )
                .thenReturn( PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, query );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexRangeSeekByPrefixWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForRangeSeekByPrefix( index, "prefix" ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.StringPrefixPredicate indexQuery = IndexQuery.stringPrefix( index.schema().getPropertyId(), "prefix" );
        when( indexReader.query( indexQuery ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, indexQuery );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexRangeSeekByContainsWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForScan( index ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.StringContainsPredicate indexQuery = IndexQuery.stringContains( index.schema().getPropertyId(), "contains" );
        when( indexReader.query( indexQuery ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, indexQuery );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexRangeSeekBySuffixWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForScan( index ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.StringSuffixPredicate indexQuery = IndexQuery.stringSuffix( index.schema().getPropertyId(), "suffix" );
        when( indexReader.query( indexQuery ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, indexQuery );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldConsiderTransactionStateDuringIndexBetweenRangeSeekByNumberWithIndexQuery() throws Exception
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
        when( txState.indexUpdatesForRangeSeekByNumber( index, lower, true, upper, false ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );
        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( txState.augmentSingleNodeCursor( any( Cursor.class ), anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            long nodeId = (long) invocationOnMock.getArguments()[1];
            when( txState.augmentSinglePropertyCursor( any( Cursor.class ), any( PropertyContainerState.class ),
                    eq( propertyKey ) ) ).thenReturn( asPropertyCursor( intProperty( propertyKey, inRange ) ) );
            return asNodeCursor( nodeId, nodeId + 20000 );
        } );

        IndexReader indexReader = addMockedIndexReader( storageStatement );
        IndexQuery.NumberRangePredicate indexQuery =
                IndexQuery.range( index.schema().getPropertyId(), lower, true, upper, false );
        when( indexReader.query( indexQuery ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null )
        );
        when( storageStatement.acquireSingleNodeCursor( anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            long nodeId = (long) invocationOnMock.getArguments()[0];
            when( storeReadLayer.nodeGetProperty( eq( storageStatement ), any( NodeItem.class ), eq( propertyKey ),
                    any( AssertOpen.class ) ) ).thenReturn( asPropertyCursor( intProperty( propertyKey, inRange ) ) );
            return asNodeCursor( nodeId, nodeId + 20000 );
        } );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery( statement, index, indexQuery );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void shouldConsiderTransactionStateDuringIndexBetweenRangeSeekByStringWithIndexQuery() throws Exception
    {
        // Given
        TransactionState txState = mock( TransactionState.class );
        KernelStatement statement = mock( KernelStatement.class );
        when( statement.hasTxStateWithChanges() ).thenReturn( true );
        when( statement.txState() ).thenReturn( txState );
        when( txState.indexUpdatesForRangeSeekByString( index, "Anne", true, "Bill", false ) ).thenReturn(
                new DiffSets<>( Collections.singleton( 42L ), Collections.singleton( 44L ) )
        );
        when( txState.addedAndRemovedNodes() ).thenReturn(
                new DiffSets<>( Collections.singleton( 45L ), Collections.singleton( 46L ) )
        );

        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        IndexReader indexReader = addMockedIndexReader( statement );
        IndexQuery.StringRangePredicate rangePredicate =
                IndexQuery.range( index.schema().getPropertyId(), "Anne", true, "Bill", false );
        when( indexReader.query( rangePredicate ) ).thenReturn(
                PrimitiveLongCollections.resourceIterator( PrimitiveLongCollections.iterator( 43L, 44L, 46L ), null ) );

        StateHandlingStatementOperations context = newTxStateOps( storeReadLayer );

        // When
        PrimitiveLongIterator results = context.indexQuery(
                statement, index, rangePredicate );

        // Then
        assertEquals( asSet( 42L, 43L ), PrimitiveLongCollections.toSet( results ) );
    }

    @Test
    public void indexQueryClosesIndexReader() throws Exception
    {
        KernelStatement kernelStatement = mock( KernelStatement.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        IndexReader indexReader = mock( IndexReader.class );

        when( indexReader.query( any() ) ).thenReturn( PrimitiveLongCollections.emptyIterator() );
        when( storeStatement.getFreshIndexReader( any() ) ).thenReturn( indexReader );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );

        StateHandlingStatementOperations operations = newTxStateOps( mock( StoreReadLayer.class ) );

        operations.nodeGetFromUniqueIndexSeek(
                kernelStatement,
                IndexDescriptorFactory.uniqueForLabel( 1, 1 ),
                IndexQuery.exact( 1, "foo" ) );

        verify( indexReader ).close();
    }

    @Test
    public void shouldNotRecordNodeSetPropertyOnSameValue() throws Exception
    {
        // GIVEN
        int propertyKeyId = 5;
        long nodeId = 0;
        String value = "The value";
        KernelStatement kernelStatement = mock( KernelStatement.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        Cursor<NodeItem> ourNode = nodeCursorWithProperty( propertyKeyId );
        when( storeStatement.acquireSingleNodeCursor( nodeId ) ).thenReturn( ourNode );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );
        InternalAutoIndexing autoIndexing = mock( InternalAutoIndexing.class );
        AutoIndexOperations autoIndexOps = mock( AutoIndexOperations.class );
        when( autoIndexing.nodes() ).thenReturn( autoIndexOps );
        when( autoIndexing.relationships() ).thenReturn( AutoIndexOperations.UNSUPPORTED );
        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        Cursor<PropertyItem> propertyItemCursor = propertyCursor( propertyKeyId, value );
        when( storeReadLayer.nodeGetProperty( eq( storeStatement ), any( NodeItem.class ),
                eq( propertyKeyId ), any( AssertOpen.class ) ) ).thenReturn( propertyItemCursor );
        StateHandlingStatementOperations operations = newTxStateOps( storeReadLayer, autoIndexing );

        // WHEN
        DefinedProperty newProperty = Property.stringProperty( propertyKeyId, value );
        operations.nodeSetProperty( kernelStatement, nodeId, newProperty );

        // THEN
        assertFalse( kernelStatement.hasTxStateWithChanges() );
        // although auto-indexing should still be notified
        verify( autoIndexOps ).propertyChanged( any( DataWriteOperations.class ), eq( nodeId ),
                eq( Property.stringProperty( propertyKeyId, value ) ), eq( newProperty ) );
    }

    @Test
    public void shouldNotRecordRelationshipSetPropertyOnSameValue() throws Exception
    {
        // GIVEN
        int propertyKeyId = 5;
        long relationshipId = 0;
        String value = "The value";
        KernelStatement kernelStatement = mock( KernelStatement.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        Cursor<RelationshipItem> ourRelationship = relationshipCursorWithProperty( propertyKeyId );
        when( storeStatement.acquireSingleRelationshipCursor( relationshipId ) ).thenReturn( ourRelationship );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );
        InternalAutoIndexing autoIndexing = mock( InternalAutoIndexing.class );
        AutoIndexOperations autoIndexOps = mock( AutoIndexOperations.class );
        when( autoIndexing.nodes() ).thenReturn( AutoIndexOperations.UNSUPPORTED );
        when( autoIndexing.relationships() ).thenReturn( autoIndexOps );
        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        Cursor<PropertyItem> propertyItemCursor = propertyCursor( propertyKeyId, value );
        when( storeReadLayer.relationshipGetProperty( eq( storeStatement ), any( RelationshipItem.class ),
                eq( propertyKeyId ), any( AssertOpen.class ) ) ).thenReturn( propertyItemCursor );
        StateHandlingStatementOperations operations = newTxStateOps( storeReadLayer, autoIndexing );

        // WHEN
        DefinedProperty newProperty = Property.stringProperty( propertyKeyId, value );
        operations.relationshipSetProperty( kernelStatement, relationshipId, newProperty );

        // THEN
        assertFalse( kernelStatement.hasTxStateWithChanges() );
        // although auto-indexing should still be notified
        verify( autoIndexOps ).propertyChanged( any( DataWriteOperations.class ), eq( relationshipId ),
                eq( newProperty ), eq( newProperty ) );
    }

    @Test
    public void shouldNotRecordGraphSetPropertyOnSameValue() throws Exception
    {
        // GIVEN
        int propertyKeyId = 5;
        String value = "The value";
        KernelStatement kernelStatement = mock( KernelStatement.class );
        StoreStatement storeStatement = mock( StoreStatement.class );
        when( kernelStatement.getStoreStatement() ).thenReturn( storeStatement );
        when( inner.graphGetAllProperties() ).thenReturn( iterator( Property.stringProperty( propertyKeyId, value ) ) );
        StateHandlingStatementOperations operations = newTxStateOps( inner );

        // WHEN
        DefinedProperty newProperty = Property.stringProperty( propertyKeyId, value );
        operations.graphSetProperty( kernelStatement, newProperty );

        // THEN
        assertFalse( kernelStatement.hasTxStateWithChanges() );
    }

    private Cursor<NodeItem> nodeCursorWithProperty( long propertyKeyId )
    {
        NodeItem item = mock( NodeItem.class );
        when( item.nextPropertyId()).thenReturn( propertyKeyId );
        return StubCursors.cursor( item );
    }

    private Cursor<RelationshipItem> relationshipCursorWithProperty( long propertyKeyId )
    {
        RelationshipItem item = mock( RelationshipItem.class );
        when( item.nextPropertyId() ).thenReturn( propertyKeyId );
        return StubCursors.cursor( item );
    }

    private Cursor<PropertyItem> propertyCursor( long propertyKeyId, String value )
    {
        PropertyItem propertyItem = mock( PropertyItem.class );
        when( propertyItem.propertyKeyId() ).thenReturn( (int) propertyKeyId );
        when( propertyItem.value() ).thenReturn( value );
        return StubCursors.cursor( propertyItem );
    }

    private StateHandlingStatementOperations newTxStateOps( StoreReadLayer delegate )
    {
        return newTxStateOps( delegate, mock( InternalAutoIndexing.class ) );
    }

    private StateHandlingStatementOperations newTxStateOps( StoreReadLayer delegate, AutoIndexing autoIndexing )
    {
        return new StateHandlingStatementOperations( delegate,
                autoIndexing, mock( ConstraintIndexCreator.class ),
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
}
