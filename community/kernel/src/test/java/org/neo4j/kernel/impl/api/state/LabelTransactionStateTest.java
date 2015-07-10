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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.index.LegacyIndexStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsIteratorFrom;
import static org.neo4j.graphdb.Neo4jMockitoHelpers.answerAsPrimitiveLongIteratorFrom;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.state.StubCursors.asLabelCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNode;
import static org.neo4j.kernel.impl.api.state.StubCursors.asNodeCursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.asPropertyCursor;

public class LabelTransactionStateTest
{
    @Before
    public void before() throws Exception
    {
        store = mock( StoreReadLayer.class );
        when( store.indexesGetForLabel( labelId1 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( answerAsIteratorFrom( Collections
                .<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( answerAsIteratorFrom( Collections.<IndexDescriptor>emptyList() ) );

        txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );
        txContext = new StateHandlingStatementOperations( store, mock( LegacyPropertyTrackers.class ),
                mock( ConstraintIndexCreator.class ), mock( LegacyIndexStore.class ) );

        storeStatement = mock( StoreStatement.class );
        when( state.getStoreStatement() ).thenReturn( storeStatement );
    }

    @Test
    public void addOnlyLabelShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId1 );
            }
        }

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addAdditionalLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId2 );
            }
        }

        // THEN
        assertLabels( labelId1, labelId2 );
    }

    @Test
    public void addAlreadyExistingLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId1 );
            }
        }

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void removeCommittedLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1, labelId2 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeRemoveLabel( state, cursor.get(), labelId1 );
            }
        }
        // THEN
        assertLabels( labelId2 );
    }

    @Test
    public void removeAddedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId2 );
            }
        }

        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeRemoveLabel( state, cursor.get(), labelId2 );
            }
        }

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addRemovedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeRemoveLabel( state, cursor.get(), labelId1 );
            }
        }

        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId1 );
            }
        }

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1, 2 ),
                labels( 1, 2, 3 ),
                labels( 2, 1, 3 ) );

        // WHEN
        txContext.nodeAddLabel( state, asNode( 2 ), 2 );

        // THEN
        assertEquals( asSet( 0L, 1L, 2L ), asSet( txContext.nodesGetForLabel( state, 2 ) ) );
    }

    @Test
    public void removedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1, 2 ),
                labels( 1, 2, 3 ),
                labels( 2, 1, 3 ) );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, 1 ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeRemoveLabel( state, cursor.get(), 2 );
            }
        }

        // THEN
        assertEquals( asSet( 0L ), asSet( txContext.nodesGetForLabel( state, 2 ) ) );
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                boolean added = txContext.nodeAddLabel( state, cursor.get(), labelId1 );

                // THEN
                assertTrue( "Should have been added now", added );
            }
        }
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                boolean added = txContext.nodeAddLabel( state, cursor.get(), labelId1 );

                // THEN
                assertFalse( "Shouldn't have been added now", added );
            }
        }
    }

    @Test
    public void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                boolean removed = txContext.nodeRemoveLabel( state, cursor.get(), labelId1 );

                // THEN
                assertTrue( "Should have been removed now", removed );
            }

        }
    }

    @Test
    public void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                txContext.nodeAddLabel( state, cursor.get(), labelId1 );
            }
        }

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void should_return_true_when_adding_new_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337 ) );

        // WHEN and THEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, 1337 ) )
        {
            if ( cursor.next() )
            {
                assertTrue( "Label should have been added", txContext.nodeAddLabel( state, cursor.get(), 12 ) );
            }
        }
    }

    @Test
    public void should_return_false_when_adding_existing_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337, asPropertyCursor(),
                asLabelCursor( 12 ) ) );

        // WHEN and THEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, 1337 ) )
        {
            if ( cursor.next() )
            {
                assertFalse( "Label should have been added", txContext.nodeAddLabel( state, cursor.get(), 12 ) );
            }
        }
    }

    @Test
    public void should_return_true_when_removing_existing_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337, asPropertyCursor(),
                asLabelCursor( 12 ) ) );

        // WHEN and THEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, 1337 ) )
        {
            if ( cursor.next() )
            {
                assertTrue( "Label should have been removed", txContext.nodeRemoveLabel( state, cursor.get(), 12 ) );
            }
        }
    }

    @Test
    public void should_return_true_when_removing_non_existant_label() throws Exception
    {
        // GIVEN
        when( storeStatement.acquireSingleNodeCursor( 1337 ) ).thenReturn( asNodeCursor( 1337 ) );

        // WHEN and THEN
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, 1337 ) )
        {
            if ( cursor.next() )
            {
                assertFalse( "Label should have been removed",
                        txContext.nodeRemoveLabel( state, cursor.get(), 12 ) );
            }
        }
    }

    // exists

    private final int labelId1 = 10, labelId2 = 12;
    private final long nodeId = 20;

    private StoreReadLayer store;
    private TransactionState txState;
    private StateHandlingStatementOperations txContext;

    private KernelStatement state;
    private StoreStatement storeStatement;

    private static class Labels
    {
        private final long nodeId;
        private final Integer[] labelIds;

        Labels( long nodeId, Integer... labelIds )
        {
            this.nodeId = nodeId;
            this.labelIds = labelIds;
        }
    }

    private static Labels labels( long nodeId, Integer... labelIds )
    {
        return new Labels( nodeId, labelIds );
    }

    private void commitLabels( Labels... labels ) throws Exception
    {
        Map<Integer, Collection<Long>> allLabels = new HashMap<>();
        for ( Labels nodeLabels : labels )
        {
            when( storeStatement.acquireSingleNodeCursor( nodeLabels.nodeId ) ).thenReturn( StubCursors.asNodeCursor(
                    nodeLabels.nodeId, asPropertyCursor(), asLabelCursor( nodeLabels.labelIds ) ) );

            for ( int label : nodeLabels.labelIds )
            {
                Collection<Long> nodes = allLabels.get( label );
                if ( nodes == null )
                {
                    nodes = new ArrayList<>();
                    allLabels.put( label, nodes );
                }
                nodes.add( nodeLabels.nodeId );
            }
        }

        for ( Map.Entry<Integer, Collection<Long>> entry : allLabels.entrySet() )
        {
            when( store.nodesGetForLabel( state, entry.getKey() ) ).then( answerAsPrimitiveLongIteratorFrom( entry
                    .getValue() ) );
        }
    }

    private void commitNoLabels() throws Exception
    {
        commitLabels( new Integer[0] );
    }

    private void commitLabels( Integer... labels ) throws Exception
    {
        commitLabels( labels( nodeId, labels ) );
    }

    private void assertLabels( Integer... labels ) throws EntityNotFoundException
    {
        try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
        {
            if ( cursor.next() )
            {
                assertEquals( asSet( labels ), asSet( cursor.get().getLabels() ) );
            }
        }

        for ( int label : labels )
        {
            try ( Cursor<NodeItem> cursor = txContext.nodeCursor( state, nodeId ) )
            {
                if ( cursor.next() )
                {
                    assertTrue( "Expected labels not found on node", cursor.get().hasLabel( label ) );
                }
            }
        }
    }
}
