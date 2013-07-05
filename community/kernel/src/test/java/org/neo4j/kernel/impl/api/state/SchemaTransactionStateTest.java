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
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
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
        verify( store ).indexesGetForLabel( state, labelId1 );

        assertEquals( asSet( rule ), IteratorUtil.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll( state );

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
        verify( store ).indexesGetForLabel( state, labelId1 );

        assertEquals( asSet( rule2 ), IteratorUtil.asSet( txContext.indexesGetForLabel( state, labelId2 ) ) );
        verify( store ).indexesGetForLabel( state, labelId2 );

        assertEquals( asSet( rule1, rule2 ), IteratorUtil.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll( state );

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
        assertEquals( InternalIndexState.POPULATING, txContext.indexGetState( state, rule ) );
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
        assertEquals( expectedRule, rule );
        assertEquals( asSet( expectedRule ), asSet( labelRules ) );
    }

    @Test
    public void shouldNotReturnExistentRuleDroppedInTransaction() throws Exception
    {
        // GIVEN
        // -- a rule that exists in the store
        IndexDescriptor rule = new IndexDescriptor( labelId1, key1 );
        when( store.indexesGetForLabel( state, labelId1 ) ).thenReturn( option( rule ).iterator() );
        // -- that same rule dropped in the transaction
        txContext.indexDrop( state, rule );

        // WHEN
        assertException( getIndexRule(), SchemaRuleNotFoundException.class );
        Iterator<IndexDescriptor> rulesByLabel = txContext.indexesGetForLabel( state, labelId1 );

        // THEN
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( rulesByLabel ) );
    }

    private ExceptionExpectingFunction<SchemaRuleNotFoundException> getIndexRule()
    {
        return new ExceptionExpectingFunction<SchemaRuleNotFoundException>()
        {
            @Override
            public void call() throws SchemaRuleNotFoundException
            {
                txContext.indexesGetForLabelAndPropertyKey( state, labelId1, key1 );
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

    private StatementOperations store;
    private OldTxStateBridge oldTxState;
    private TxState txState;
    private StateHandlingStatementOperations txContext;
    private StatementState state;

    @Before
    public void before() throws Exception
    {
        oldTxState = mock( OldTxStateBridge.class );

        txState = new TxStateImpl( oldTxState, mock( PersistenceManager.class ),
                mock( TxState.IdGeneration.class ) );
        state = StatementOperationsTestHelper.mockedState( txState );
        
        store = mock( StatementOperations.class );
        when( store.indexesGetForLabel( state, labelId1 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( state, labelId2 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll( state ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexCreate( eq( state ), anyLong(), anyLong() ) ).thenAnswer( new Answer<IndexDescriptor>()
        {
            @Override
            public IndexDescriptor answer( InvocationOnMock invocation ) throws Throwable
            {
                return new IndexDescriptor((Long) invocation.getArguments()[0],
                        (Long) invocation.getArguments()[1] );
            }
        } );

        txContext = new StateHandlingStatementOperations( store, store, mock( AuxiliaryStoreOperations.class ),
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
            when( store.nodeGetLabels( state, nodeLabels.nodeId ) ).then( asAnswer( Arrays.<Long>asList( nodeLabels
                    .labelIds ) ) );
            for ( long label : nodeLabels.labelIds )
            {
                when( store.nodeHasLabel( state, nodeLabels.nodeId, label ) ).thenReturn( true );
                when( store.nodeRemoveLabel( state, nodeLabels.nodeId, label ) ).thenReturn( true );
                when( store.nodeAddLabel( state, nodeLabels.nodeId, label ) ).thenReturn( false );

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
            when( store.nodesGetForLabel( state, entry.getKey() ) ).then( asAnswer( entry.getValue() ) );
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
