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

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class SchemaTransactionStateTest
{
    @Test
    public void addedRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule = txContext.addIndex( labelId1, key1 );

        // THEN
        assertEquals( asSet( rule ), IteratorUtil.asSet( txContext.getIndexes( labelId1 ) ) );
        verify( store ).getIndexes( labelId1 );

        assertEquals( asSet( rule ), IteratorUtil.asSet( txContext.getIndexes() ) );
        verify( store ).getIndexes();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedRulesShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = txContext.addIndex( labelId1, key1 );
        IndexDescriptor rule2 = txContext.addIndex( labelId2, key2 );

        // THEN
        assertEquals( asSet( rule1 ), IteratorUtil.asSet( txContext.getIndexes( labelId1 ) ) );
        verify( store ).getIndexes( labelId1 );

        assertEquals( asSet( rule2 ), IteratorUtil.asSet( txContext.getIndexes( labelId2 ) ) );
        verify( store ).getIndexes( labelId2 );

        assertEquals( asSet( rule1, rule2 ), IteratorUtil.asSet( txContext.getIndexes() ) );
        verify( store ).getIndexes();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedAdditionalRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = txContext.addIndex( labelId1, key1 );
        IndexDescriptor rule2 = txContext.addIndex( labelId1, key2 );

        // THEN
        assertEquals( asSet( rule1, rule2 ), IteratorUtil.asSet( txContext.getIndexes( labelId1 ) ) );
    }

    @Test
    public void creatingAnIndexShouldBePopulatingStateWithinTX() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        IndexDescriptor rule = txContext.addIndex( labelId1, key1 );

        // THEN
        assertEquals( InternalIndexState.POPULATING, txContext.getIndexState( rule ) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransaction() throws Exception
    {
        // GIVEN
        // -- non-existent rule added in the transaction
        txContext.addIndex( labelId1, key1 );

        // WHEN
        IndexDescriptor rule = txContext.getIndex( labelId1, key1 );
        Iterator<IndexDescriptor> labelRules = txContext.getIndexes( labelId1 );

        // THEN
        IndexDescriptor expectedRule = new IndexDescriptor( labelId1, key1 );
        assertEquals( expectedRule, rule );
        assertEquals( asSet( expectedRule ), asSet( labelRules ) );
    }

    @Test
    public void shouldNotReturnExistentRuleDroppedInTransaction() throws Exception
    {
        // GIVEN
        // -- a rule that exists in the store
        IndexDescriptor rule = new IndexDescriptor( labelId1, key1 );
        when( store.getIndexes( labelId1 ) ).thenReturn( option( rule ).iterator() );
        // -- that same rule dropped in the transaction
        txContext.dropIndex( rule );

        // WHEN
        assertException( getIndexRule(), SchemaRuleNotFoundException.class );
        Iterator<IndexDescriptor> rulesByLabel = txContext.getIndexes( labelId1 );

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
                txContext.getIndex( labelId1, key1 );
            }
        };
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
            fail( "Should have thrown " + exception.getClass().getName() + " exception" );
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

    private final long labelId1 = 10, labelId2 = 12, nodeId = 20;
    private final long key1 = 45, key2 = 46;

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
                return new IndexDescriptor((Long) invocation.getArguments()[0],
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
}
