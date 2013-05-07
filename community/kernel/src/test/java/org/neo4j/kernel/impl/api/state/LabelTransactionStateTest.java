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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class LabelTransactionStateTest
{

    @Test
    public void addOnlyLabelShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addAdditionalLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.addLabelToNode( labelId2, nodeId );

        // THEN
        assertLabels( labelId1, labelId2 );
    }

    @Test
    public void addAlreadyExistingLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void removeCommittedLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1, labelId2 );

        // WHEN
        txContext.removeLabelFromNode( labelId1, nodeId );

        // THEN
        assertLabels( labelId2 );
    }

    @Test
    public void removeAddedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.addLabelToNode( labelId2, nodeId );
        txContext.removeLabelFromNode( labelId2, nodeId );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addRemovedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        txContext.removeLabelFromNode( labelId1, nodeId );
        txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void addedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1L, 2L ),
                labels( 1, 2L, 3L ),
                labels( 2, 1L, 3L ) );

        // WHEN
        txContext.addLabelToNode( 2, 2 );

        // THEN
        assertEquals( asSet( 0L, 1L, 2L ), asSet( txContext.getNodesWithLabel( 2 ) ) );
    }

    @Test
    public void removedLabelsShouldBeReflectedWhenGettingNodesForLabel() throws Exception
    {
        // GIVEN
        commitLabels(
                labels( 0, 1L, 2L ),
                labels( 1, 2L, 3L ),
                labels( 2, 1L, 3L ) );

        // WHEN
        txContext.removeLabelFromNode( 2, 1 );

        // THEN
        assertEquals( asSet( 0L ), asSet( txContext.getNodesWithLabel( 2 ) ) );
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitNoLabels();
        when( store.addLabelToNode( labelId1, nodeId ) ).thenReturn( true );


        // WHEN
        boolean added = txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertTrue( "Should have been added now", added );
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        boolean added = txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertFalse( "Shouldn't have been added now", added );
    }

    @Test
    public void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );

        // WHEN
        boolean removed = txContext.removeLabelFromNode( labelId1, nodeId );

        // THEN
        assertTrue( "Should have been removed now", removed );
    }

    @Test
    public void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        txContext.addLabelToNode( labelId1, nodeId );

        // THEN
        assertLabels( labelId1 );
    }

    @Test
    public void should_return_true_when_adding_new_label() throws Exception
    {
        // GIVEN
        when( store.isLabelSetOnNode( 12, 1337 ) ).thenReturn( false );

        // WHEN and THEN
        assertTrue( "Label should have been added", txContext.addLabelToNode( 12, 1337 ) );
    }

    @Test
    public void should_return_false_when_adding_existing_label() throws Exception
    {
        // GIVEN
        when( store.isLabelSetOnNode( 12, 1337 ) ).thenReturn( true );

        // WHEN and THEN
        assertFalse( "Label should have been added", txContext.addLabelToNode( 12, 1337 ) );
    }

    @Test
    public void should_return_true_when_removing_existing_label() throws Exception
    {
        // GIVEN
        when( store.isLabelSetOnNode( 12, 1337 ) ).thenReturn( true );

        // WHEN and THEN
        assertTrue( "Label should have been removed", txContext.removeLabelFromNode( 12, 1337 ) );
    }

    @Test
    public void should_return_true_when_removing_non_existant_label() throws Exception
    {
        // GIVEN
        when( store.isLabelSetOnNode( 12, 1337 ) ).thenReturn( false );

        // WHEN and THEN
        assertFalse( "Label should have been removed", txContext.removeLabelFromNode( 12, 1337 ) );
    }

    // exists

    private final long labelId1 = 10, labelId2 = 12, nodeId = 20;

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

        txContext = new StateHandlingStatementContext( store, mock( SchemaStateOperations.class), state,
                                                       mock( ConstraintIndexCreator.class ) );
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

    private static class Labels
    {
        private final long nodeId;
        private final Long[] labelIds;

        Labels( long nodeId, Long... labelIds )
        {
            this.nodeId = nodeId;
            this.labelIds = labelIds;
        }
    }

    private static Labels labels( long nodeId, Long... labelIds )
    {
        return new Labels( nodeId, labelIds );
    }

    private void commitLabels( Labels... labels ) throws EntityNotFoundException
    {
        Map<Long, Collection<Long>> allLabels = new HashMap<Long, Collection<Long>>();
        for ( Labels nodeLabels : labels )
        {
            when( store.getLabelsForNode( nodeLabels.nodeId ) ).then( asAnswer( Arrays.<Long>asList( nodeLabels
                    .labelIds ) ) );
            for ( long label : nodeLabels.labelIds )
            {
                when( store.isLabelSetOnNode( label, nodeLabels.nodeId ) ).thenReturn( true );
                when( store.removeLabelFromNode( label, nodeLabels.nodeId ) ).thenReturn( true );
                when( store.addLabelToNode( label, nodeLabels.nodeId ) ).thenReturn( false );

                Collection<Long> nodes = allLabels.get( label );
                if ( nodes == null )
                {
                    nodes = new ArrayList<Long>();
                    allLabels.put( label, nodes );
                }
                nodes.add( nodeLabels.nodeId );
            }
        }

        for ( Map.Entry<Long, Collection<Long>> entry : allLabels.entrySet() )
        {
            when( store.getNodesWithLabel( entry.getKey() ) ).then( asAnswer( entry.getValue() ) );
        }
    }

    private void commitNoLabels() throws EntityNotFoundException
    {
        commitLabels( new Long[0] );
    }

    private void commitLabels( Long... labels ) throws EntityNotFoundException
    {
        commitLabels( labels( nodeId, labels ) );
    }

    private void assertLabels( Long... labels ) throws EntityNotFoundException
    {
        assertEquals( asSet( labels ), asSet( txContext.getLabelsForNode( nodeId ) ) );
        for ( long label : labels )
        {
            assertTrue( "Expected labels not found on node", txContext.isLabelSetOnNode( label, nodeId ) );
        }
    }
}
