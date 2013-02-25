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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class TransactionStateAwareStatementContextTest
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
    public void addedRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexRule rule = txContext.addIndexRule( labelId1, key1 );

        // THEN
        assertEquals( asSet( rule ), asSet( txContext.getIndexRules( labelId1 ) ) );
        assertEquals( asSet( rule ), asSet( txContext.getIndexRules() ) );
        verify( store ).addIndexRule( labelId1, key1 );
    }

    @Test
    public void addedRulesShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexRule rule1 = txContext.addIndexRule( labelId1, key1 );
        IndexRule rule2 = txContext.addIndexRule( labelId2, key2 );

        // THEN
        assertEquals( asSet( rule1 ), asSet( txContext.getIndexRules( labelId1 ) ) );
        assertEquals( asSet( rule2 ), asSet( txContext.getIndexRules( labelId2 ) ) );
        assertEquals( asSet( rule1, rule2 ), asSet( txContext.getIndexRules() ) );
        verify( store ).addIndexRule( labelId1, key1 );
    }
    
    @Test
    public void addedAdditionalRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexRule rule1 = txContext.addIndexRule( labelId1, key1 );
        IndexRule rule2 = txContext.addIndexRule( labelId1, key2 );

        // THEN
        assertEquals( asSet( rule1, rule2 ), asSet( txContext.getIndexRules( labelId1 ) ) );
    }

    @Test
    public void unsuccessfulCloseShouldntDelegateDownLabels() throws Exception
    {
        // GIVEN
        commitNoLabels();
        txContext.addLabelToNode( labelId1, nodeId );
        
        // WHEN
        txContext.close( false );

        // THEN
        verify( store, never() ).addLabelToNode( labelId1, nodeId );
    }
    
    @Test
    public void successfulCloseShouldDelegateDownLabels() throws Exception
    {
        // GIVEN
        commitNoLabels();
        txContext.addLabelToNode( labelId1, nodeId );
        
        // WHEN
        txContext.close( true );

        // THEN
        verify( store ).addLabelToNode( labelId1, nodeId );
    }
    
    @Test
    public void successfulCloseShouldDelegateDownRules() throws Exception
    {
        // GIVEN
        commitNoLabels();
        txContext.addIndexRule( labelId1, key1 );

        // WHEN
        txContext.close( true );

        // THEN
        verify( store ).addIndexRule( labelId1, key1 );
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
    public void creatingAnIndexShouldBePopulatingStateWithinTX() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        IndexRule rule = txContext.addIndexRule( labelId1, key1 );

        // THEN
        assertEquals( InternalIndexState.POPULATING, txContext.getIndexState( rule ) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransaction() throws Exception
    {
        // GIVEN
        // -- non-existent rule added in the transaction
        txContext.addIndexRule( labelId1, key1 );
        
        // WHEN
        IndexRule rule = txContext.getIndexRule( labelId1, key1 );
        Iterable<IndexRule> labelRules = txContext.getIndexRules( labelId1 );
        
        // THEN
        IndexRule expectedRule = new IndexRule( rule.getId(), labelId1, key1 );
        assertEquals( expectedRule, rule );
        assertEquals( asSet( expectedRule ), asSet( labelRules ) );
    }
    
    @Test
    public void shouldNotReturnExistentRuleDroppedInTransaction() throws Exception
    {
        // GIVEN
        // -- a rule that exists in the store
        IndexRule rule = new IndexRule( ruleId, labelId1, key1 );
        when( store.getIndexRules( labelId1 ) ).thenReturn( option( rule ) );
        // -- that same rule dropped in the transaction
        txContext.dropIndexRule( rule );
        
        // WHEN
        assertException( getIndexRule(), SchemaRuleNotFoundException.class );
        Iterable<IndexRule> rulesByLabel = txContext.getIndexRules( labelId1 );

        // THEN
        assertEquals( asSet(), asSet( rulesByLabel ) );
    }
    
    private ExceptionExpectingFunction<SchemaRuleNotFoundException> getIndexRule()
    {
        return new ExceptionExpectingFunction<SchemaRuleNotFoundException>()
        {
            @Override
            public void call() throws SchemaRuleNotFoundException
            {
                txContext.getIndexRule( labelId1, key1 );
            }
        };
    }

    private interface ExceptionExpectingFunction<E extends Exception>
    {
        void call() throws E;
    }
    
    private <E extends Exception> void assertException( ExceptionExpectingFunction<E> function, Class<? extends E> exception )
    {
        try
        {
            function.call();
            fail( "Should have thrown " + exception.getClass().getName() + " exception" );
        }
        catch ( Exception e )
        {
            if ( !exception.isAssignableFrom( e.getClass() ) )
                throw launderedException( e );
        }
    }
    
    // exists
    
    private final long labelId1 = 10, labelId2 = 12, nodeId = 20;
    private final long key1 = 45, key2 = 46, ruleId = 9;
    private int rulesCreated;

    private StatementContext store;
    private TxState state;
    private StatementContext txContext;
    
    @Before
    public void before() throws Exception
    {
        store = mock( StatementContext.class );
        when( store.getIndexRules( labelId1 ) ).thenReturn( Collections.<IndexRule>emptyList() );
        when( store.getIndexRules( labelId2 ) ).thenReturn( Collections.<IndexRule>emptyList() );
        when( store.getIndexRules() ).thenReturn( Collections.<IndexRule>emptyList() );
        when( store.addIndexRule( anyLong(), anyLong() ) ).thenAnswer( new Answer<IndexRule>()
        {
            @Override
            public IndexRule answer( InvocationOnMock invocation ) throws Throwable
            {
                return new IndexRule( ruleId+rulesCreated++, (Long) invocation.getArguments()[0],
                        (Long) invocation.getArguments()[1] );
            }
        } );
        state = new TxState();
        txContext = new TransactionStateAwareStatementContext( store, state );
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
    
    private void commitLabels( Labels... labels )
    {
        Map<Long, Collection<Long>> allLabels = new HashMap<Long, Collection<Long>>();
        for ( Labels nodeLabels : labels )
        {
            when( store.getLabelsForNode( nodeLabels.nodeId ) ).thenReturn( Arrays.<Long>asList( nodeLabels.labelIds ) );
            for ( long label : nodeLabels.labelIds )
            {
                when( store.isLabelSetOnNode( label, nodeLabels.nodeId ) ).thenReturn( true );
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
            when( store.getNodesWithLabel( entry.getKey() ) ).thenReturn( entry.getValue() );
        }
    }
    
    private void commitNoLabels()
    {
        commitLabels( new Long[0] );
    }

    private void commitLabels( Long... labels )
    {
        commitLabels( labels( nodeId, labels ) );
    }

    private void assertLabels( Long... labels )
    {
        assertEquals( asSet( labels ), asSet( txContext.getLabelsForNode( nodeId ) ) );
        for ( long label : labels )
        {
            assertTrue( "Expected labels not found on node", txContext.isLabelSetOnNode( label, nodeId ) );
        }
    }
}
