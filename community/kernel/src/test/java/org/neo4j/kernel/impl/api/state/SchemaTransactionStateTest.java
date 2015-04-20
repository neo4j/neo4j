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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.index.LegacyIndexStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class SchemaTransactionStateTest
{
    @Test
    public void addedRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule = txContext.indexCreate( state, labelId1, key1 );

        // THEN
        assertEquals( asSet( rule ), IteratorUtil.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
        verify( store ).indexesGetForLabel( labelId1 );

        assertEquals( asSet( rule ), IteratorUtil.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedRulesShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = txContext.indexCreate( state, labelId1, key1 );
        IndexDescriptor rule2 = txContext.indexCreate( state, labelId2, key2 );

        // THEN
        assertEquals( asSet( rule1 ), IteratorUtil.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
        verify( store ).indexesGetForLabel( labelId1 );

        assertEquals( asSet( rule2 ), IteratorUtil.asSet( txContext.indexesGetForLabel( state, labelId2 ) ) );
        verify( store ).indexesGetForLabel( labelId2 );

        assertEquals( asSet( rule1, rule2 ), IteratorUtil.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedAdditionalRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = txContext.indexCreate( state, labelId1, key1 );
        IndexDescriptor rule2 = txContext.indexCreate( state, labelId1, key2 );

        // THEN
        assertEquals( asSet( rule1, rule2 ), IteratorUtil.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
    }

    @Test
    public void creatingAnIndexShouldBePopulatingStateWithinTX() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        IndexDescriptor rule = txContext.indexCreate( state, labelId1, key1 );

        // THEN
        assertEquals( InternalIndexState.POPULATING, txContext.indexGetState(state, rule) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransaction() throws Exception
    {
        // GIVEN
        // -- non-existent rule added in the transaction
        txContext.indexCreate( state, labelId1, key1 );

        // WHEN
        IndexDescriptor rule = txContext.indexesGetForLabelAndPropertyKey( state, labelId1, key1 );
        Iterator<IndexDescriptor> labelRules = txContext.indexesGetForLabel( state, labelId1 );

        // THEN
        IndexDescriptor expectedRule = new IndexDescriptor( labelId1, key1 );
        assertEquals(expectedRule, rule);
        assertEquals( asSet( expectedRule ), asSet( labelRules ) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransactionFromLookup() throws Exception
    {
        // GIVEN
        // -- the store already have an index on the label and a different property
        IndexDescriptor existingRule1 = new IndexDescriptor( labelId1, key1 );
        when( store.indexesGetForLabelAndPropertyKey( labelId1, key1 ) ).thenReturn( existingRule1 );
        // -- the store already have an index on a different label with the same property
        IndexDescriptor existingRule2 = new IndexDescriptor( labelId2, key2 );
        when( store.indexesGetForLabelAndPropertyKey( labelId2, key2 ) ).thenReturn( existingRule2 );
        // -- a non-existent rule has been added in the transaction
        txContext.indexCreate( state, labelId1, key2 );

        // WHEN
        IndexDescriptor rule = txContext.indexesGetForLabelAndPropertyKey( state, labelId1, key2 );

        // THEN
        assertEquals( new IndexDescriptor( labelId1, key2 ), rule );
    }

    @Test
    public void shouldNotReturnRulesAddedInTransactionWithDifferentLabelOrPropertyFromLookup() throws Exception
    {
        // GIVEN
        // -- the store already have an index on the label and a different property
        IndexDescriptor existingRule1 = new IndexDescriptor( labelId1, key1 );
        when( store.indexesGetForLabelAndPropertyKey(labelId1, key1) ).thenReturn( existingRule1 );
        // -- the store already have an index on a different label with the same property
        IndexDescriptor existingRule2 = new IndexDescriptor( labelId2, key2 );
        when( store.indexesGetForLabelAndPropertyKey( labelId2, key2 ) ).thenReturn( existingRule2 );
        // -- a non-existent rule has been added in the transaction
        txContext.indexCreate( state, labelId1, key2 );

        // WHEN
        IndexDescriptor lookupRule1 = txContext.indexesGetForLabelAndPropertyKey( state, labelId1, key1 );
        IndexDescriptor lookupRule2 = txContext.indexesGetForLabelAndPropertyKey( state, labelId2, key2 );

        // THEN
        assertEquals( existingRule1, lookupRule1 );
        assertEquals( existingRule2, lookupRule2 );
    }

    @Test
    public void shouldNotReturnExistentRuleDroppedInTransaction() throws Exception
    {
        // GIVEN
        // -- a rule that exists in the store
        IndexDescriptor rule = new IndexDescriptor( labelId1, key1 );
        when( store.indexesGetForLabel( labelId1 ) ).thenReturn( option( rule ).iterator() );
        // -- that same rule dropped in the transaction
        txContext.indexDrop( state, rule );

        // WHEN
        assertNull( txContext.indexesGetForLabelAndPropertyKey( state, labelId1, key1 ) );
        Iterator<IndexDescriptor> rulesByLabel = txContext.indexesGetForLabel( state, labelId1 );

        // THEN
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( rulesByLabel ) );
    }

    private interface ExceptionExpectingFunction<E extends Exception>
    {
        void call() throws E;
    }

    private <E extends Exception> void assertException( ExceptionExpectingFunction<E> function,
                                                        Class<? extends E> exception )
    {
        try
        {
            function.call();
            fail( "Should have thrown " + exception.getName() + " exception" );
        }
        catch ( Exception e )
        {
            if ( !exception.isAssignableFrom( e.getClass() ) )
            {
                throw launderedException( e );
            }
        }
    }

    // exists

    private final int labelId1 = 10, labelId2 = 12, key1 = 45, key2 = 46;
    private final long nodeId = 20;

    private StoreReadLayer store;
    private TransactionState txState;
    private StateHandlingStatementOperations txContext;
    private KernelStatement state;

    @Before
    public void before() throws Exception
    {
        txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );

        store = mock( StoreReadLayer.class );
        when( store.indexesGetForLabel( labelId1 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );

        txContext = new StateHandlingStatementOperations( store, mock( LegacyPropertyTrackers.class ),
                mock( ConstraintIndexCreator.class ), mock( LegacyIndexStore.class ) );
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
            when( store.nodeGetLabels( nodeLabels.nodeId ) ).then(
                    asAnswer( Arrays.<Integer>asList( nodeLabels.labelIds ) ) );
            for ( int label : nodeLabels.labelIds )
            {
                when( store.nodeHasLabel( nodeLabels.nodeId, label ) ).thenReturn( true );

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
            when( store.nodesGetForLabel( state, entry.getKey() ) ).then( asAnswer( entry.getValue() ) );
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
}
