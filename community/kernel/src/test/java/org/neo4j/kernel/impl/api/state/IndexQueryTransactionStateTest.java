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

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsIteratorFrom;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsPrimitiveLongIteratorFrom;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.properties.Property.noNodeProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class IndexQueryTransactionStateTest
{
    @Test
    public void shouldExcludeRemovedNodesFromIndexQuery() throws Exception
    {
        // Given
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 1l, 2l, 3l ) ) );

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
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator( 1l ) );

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
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        state.txState().nodeDoReplaceProperty( 1l, Property.noNodeProperty( 1l, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldExcludeChangedNodeWithMissingLabelFromUniqueIndexQuery() throws Exception
    {
        // Given
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator() );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        state.txState().nodeDoReplaceProperty( 1l, Property.noNodeProperty( 1l, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );
        when( store.nodeGetProperty( anyLong(), eq( propertyKeyId ) ) ).thenReturn(
                noNodeProperty( 1, propertyKeyId ) );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( IteratorUtil
                .<DefinedProperty>emptyIterator() );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );

        state.txState().nodeDoReplaceProperty( 1l, noNodeProperty( 1l, propertyKeyId ),
                                                   stringProperty( propertyKeyId, value ) );
        txContext.nodeAddLabel( state, 1l, labelId );

        // When
        PrimitiveLongIterator result = txContext.nodesGetFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );
    }

    @Test
    public void shouldIncludeUniqueCreatedNodeWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator() );
        when( store.nodeGetProperty( anyLong(), eq( propertyKeyId ) ) ).thenReturn(
                noNodeProperty( 1, propertyKeyId ) );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( IteratorUtil
                .<DefinedProperty>emptyIterator() );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );

        state.txState().nodeDoReplaceProperty( 1l, noNodeProperty( 1l, propertyKeyId ),
                                                   stringProperty( propertyKeyId, value ) );
        txContext.nodeAddLabel( state, 1l, labelId );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertThat( result, equalTo( 1l ) );
    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( false );
        DefinedProperty stringProperty = stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );

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
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator() );
        when( store.nodeHasLabel( 2l, labelId ) ).thenReturn( false );

        DefinedProperty stringProperty = stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 2l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );

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
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 1l, 2l, 3l ) ) );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );

        DefinedProperty stringProperty = stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );

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
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator( 1l ) );
        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );

        DefinedProperty stringProperty = stringProperty( propertyKeyId, value );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty );
        when( store.nodeGetAllProperties( anyLong() ) ).thenReturn( iterator( stringProperty ) );

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
        when( store.nodesGetFromIndexLookup( state, indexDescriptor, value ) )
                .then( answerAsPrimitiveLongIteratorFrom( asList( 2l, 3l ) ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );
        state.txState().nodeDoReplaceProperty( 1l, Property.noNodeProperty( 1l, propertyKeyId ),
                Property.intProperty( propertyKeyId, 10 ) );

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
        when( store.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn(
                asPrimitiveResourceIterator( 1l ) );
        when( store.nodeGetProperty( 1l, propertyKeyId ) ).thenReturn( stringProperty( propertyKeyId, value ) );

        when( store.nodeHasLabel( 1l, labelId ) ).thenReturn( true );
        when( store.nodeGetLabels( 1l ) ).thenReturn( PrimitiveIntCollections.iterator( labelId ) );
        txContext.nodeRemoveProperty( state, 1l, propertyKeyId );

        // When
        long result = txContext.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // Then
        assertNoSuchNode( result );
    }

    // exists
    int labelId = 2;
    int propertyKeyId = 3;
    String value = "My Value";
    IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );

    private StoreReadLayer store;
    private EntityOperations txContext;
    private KernelStatement state;

    @Before
    public void before() throws Exception
    {
        TransactionState txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );

        int labelId1 = 10, labelId2 = 12;
        store = mock( StoreReadLayer.class );
        when( store.indexGetState( indexDescriptor )).thenReturn( InternalIndexState.ONLINE );
        when( store.indexesGetForLabel( labelId1 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( answerAsIteratorFrom( Collections.<IndexDescriptor>emptyList() ) );
        when( store.constraintsGetForLabel( labelId ) ).thenReturn( Collections.<UniquenessConstraint>emptyIterator() );
        when( store.nodeExists( anyLong() ) ).thenReturn( true );
        when( store.indexesGetForLabelAndPropertyKey( labelId, propertyKeyId ) )
                .thenReturn( new IndexDescriptor( labelId, propertyKeyId ) );

        StateHandlingStatementOperations stateHandlingOperations = new StateHandlingStatementOperations(
                store,
                mock( LegacyPropertyTrackers.class ),
                mock( ConstraintIndexCreator.class ),
                mock( LegacyIndexStore.class ) );
        txContext = new ConstraintEnforcingEntityOperations(
                stateHandlingOperations, stateHandlingOperations, stateHandlingOperations );
    }

    private void assertNoSuchNode( long node )
    {
        assertThat( node, equalTo( NO_SUCH_NODE ) );
    }

    private static PrimitiveLongResourceIterator asPrimitiveResourceIterator( long... values )
    {
        return resourceIterator( PrimitiveLongCollections.iterator( values ), new Resource()
        {
            @Override
            public void close()
            { }
        } );
    }
}
