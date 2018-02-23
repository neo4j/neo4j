/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreStatement;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;
import static org.neo4j.test.mockito.answer.Neo4jMockitoAnswers.answerAsIteratorFrom;
import static org.neo4j.test.mockito.answer.Neo4jMockitoAnswers.answerAsPrimitiveLongIteratorFrom;

class LabelTransactionStateTest
{
    @BeforeEach
    void before()
    {
        store = mock( StoreReadLayer.class );
        when( store.indexesGetForLabel( anyInt() ) ).then( answerAsIteratorFrom( Collections.emptyList() ) );
        when( store.indexesGetAll() ).then( answerAsIteratorFrom( Collections.emptyList() ) );

        txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );
        txContext = new StateHandlingStatementOperations( store, mock( InternalAutoIndexing.class ),
                mock( ConstraintIndexCreator.class ), mock( ExplicitIndexStore.class ) );

        storeStatement = mock( StoreStatement.class );
        when( state.getStoreStatement() ).thenReturn( storeStatement );
    }

    @Test
    void addOnlyLabelShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    void addAdditionalLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.nodeAddLabel( state, nodeId, labelId2 );

        // THEN
        assertLabels( labelId1, labelId2 );
    }

    @Test
    void addAlreadyExistingLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    void removeCommittedLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1, labelId2 );

        // WHEN
        txContext.nodeRemoveLabel( state, nodeId, labelId1 );

        // THEN
        assertLabels( labelId2 );
    }

    @Test
    void removeAddedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.nodeAddLabel( state, nodeId, labelId2 );
        txContext.nodeRemoveLabel( state, nodeId, labelId2 );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    void addRemovedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.nodeRemoveLabel( state, nodeId, labelId1 );
        txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    void addedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1, 2 ),
                labels( 1, 2, 3 ),
                labels( 2, 1, 3 ) );

        // WHEN
        List<IndexDescriptor> indexes = Collections.singletonList( IndexDescriptorFactory.forLabel( 2, 2 ) );
        when( store.indexesGetForLabel( 2 ) ).thenReturn( indexes.iterator() );
        txContext.nodeAddLabel( state, 2, 2 );

        // THEN
        assertEquals( asSet( 0L, 1L, 2L ), PrimitiveLongCollections.toSet( txContext.nodesGetForLabel( state, 2 ) ) );
    }

    @Test
    void removedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1, 2 ),
                labels( 1, 2, 3 ),
                labels( 2, 1, 3 ) );

        // WHEN
        txContext.nodeRemoveLabel( state, 1, 2 );

        // THEN
        assertEquals( asSet( 0L ), PrimitiveLongCollections.toSet( txContext.nodesGetForLabel( state, 2 ) ) );
    }

    @Test
    void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        boolean added = txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertTrue( added, "Should have been added now" );
    }

    @Test
    void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        boolean added = txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertFalse( added, "Shouldn't have been added now" );
    }

    @Test
    void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        boolean removed = txContext.nodeRemoveLabel( state, nodeId, labelId1 );

        // THEN
        assertTrue( removed, "Should have been removed now" );
    }

    @Test
    void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        txContext.nodeAddLabel( state, nodeId, labelId1 );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    void should_return_true_when_adding_new_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337 ) );
        when( store.nodeGetProperties( eq( storeStatement ), any( NodeItem.class ), any( AssertOpen.class ) ) )
                .thenReturn( asPropertyCursor() );

        // WHEN
        boolean added = txContext.nodeAddLabel( state, 1337, 12 );

        // THEN
        assertTrue( added, "Label should have been added" );
    }

    @Test
    void should_return_false_when_adding_existing_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337,
                StubCursors.labels( 12 ) ) );
        when( store.nodeGetProperties( eq( storeStatement ), any( NodeItem.class ), any( AssertOpen.class ) ) )
                .thenReturn( asPropertyCursor() );

        // WHEN
        boolean added = txContext.nodeAddLabel( state, 1337, 12 );

        // THEN
        assertFalse( added, "Label should have been added" );
    }

    @Test
    void should_return_true_when_removing_existing_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337,
                StubCursors.labels( 12 ) ) );
        when( store.nodeGetProperties( eq( storeStatement ), any( NodeItem.class ), any( AssertOpen.class ) ) )
                .thenReturn( asPropertyCursor() );

        // WHEN
        boolean added = txContext.nodeRemoveLabel( state, 1337, 12 );

        // THEN
        assertTrue( added, "Label should have been removed" );
    }

    @Test
    void should_return_true_when_removing_non_existant_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337 ) );

        // WHEN
        boolean removed = txContext.nodeRemoveLabel( state, 1337, 12 );

        // THEN
        assertFalse( removed, "Label should have been removed" );
    }

    // exists

    private final int labelId1 = 10;
    private final int labelId2 = 12;
    private final long nodeId = 20;

    private StoreReadLayer store;
    private TransactionState txState;
    private StateHandlingStatementOperations txContext;

    private KernelStatement state;
    private StoreStatement storeStatement;

    private static class Labels
    {
        private final long nodeId;
        private final int[] labelIds;

        Labels( long nodeId, int... labelIds )
        {
            this.nodeId = nodeId;
            this.labelIds = labelIds;
        }
    }

    private static Labels labels( long nodeId, int... labelIds )
    {
        return new Labels( nodeId, labelIds );
    }

    private void commitLabels( Labels... labels )
    {
        Map<Integer,Collection<Long>> allLabels = new HashMap<>();
        for ( Labels nodeLabels : labels )
        {
            when( storeStatement.acquireSingleNodeCursor( nodeLabels.nodeId ) )
                    .thenReturn( asNodeCursor( nodeLabels.nodeId, StubCursors.labels( nodeLabels.labelIds ) ) );
            when( store.nodeGetProperties( eq( storeStatement ), any( NodeItem.class ), any( AssertOpen.class ) ) )
                    .thenReturn( asPropertyCursor() );

            for ( int label : nodeLabels.labelIds )
            {
                Collection<Long> nodes = allLabels.computeIfAbsent( label, k -> new ArrayList<>() );
                nodes.add( nodeLabels.nodeId );
            }
        }

        for ( Map.Entry<Integer,Collection<Long>> entry : allLabels.entrySet() )
        {
            when( store.nodesGetForLabel( state.getStoreStatement(), entry.getKey() ) )
                    .then( answerAsPrimitiveLongIteratorFrom( entry.getValue() ) );
        }
    }

    private void commitNoLabels()
    {
        commitLabels( new int[0] );
    }

    private void commitLabels( int... labels )
    {
        commitLabels( labels( nodeId, labels ) );
    }

    private void assertLabels( int... labels ) throws EntityNotFoundException
    {
        txContext.nodeCursorById( state, nodeId )
                .forAll( node -> assertEquals( PrimitiveIntCollections.asSet( labels ), node.labels() ) );

        txContext.nodeCursorById( state, nodeId ).forAll( node ->
        {
            for ( int label : labels )
            {
                assertTrue( node.hasLabel( label ), "Expected labels not found on node" );
            }
        } );
    }
}
