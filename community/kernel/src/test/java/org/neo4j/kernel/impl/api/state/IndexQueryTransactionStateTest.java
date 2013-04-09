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

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.NO_INDEX_PROVIDER;

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
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.TransactionStateStatementContext;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class IndexQueryTransactionStateTest
{

    @Test
    public void shouldExcludeRemovedNodesFromIndexQuery() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        NodeImpl mockNodeImpl = mock( NodeImpl.class );

        txContext.deleteNode( 2l );

        // When
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 3l ) ) );
    }

    @Test
    public void shouldExcludeChangedNodesWithMissingLabelFromIndexQuery() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }

    @Test
    public void shouldIncludeCreatedNodesWithCorrectLabelAndProperty() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( asSet( 1l ), Collections.<Long>emptySet() ) );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldIncludeExistingNodesWithCorrectPropertyAfterAddingLabel() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( false );
        when( store.getNodePropertyValue( 1l, propertyKeyId ) ).thenReturn( value );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 1l, 2l, 3l ) ) );

    }

    @Test
    public void shouldExcludeExistingNodesWithCorrectPropertyAfterRemovingLabel() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 1l, 2l, 3l ) ) );
        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( true );

        when( store.getNodePropertyValue( 1l, propertyKeyId ) ).thenReturn( value );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn( new DiffSets<Long>() );

        // When
        txContext.removeLabelFromNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
    }


    @Test
    public void shouldExcludeNodesWithRemovedProperty() throws Exception
    {
        // Given
        long labelId = 2l;
        long propertyKeyId = 3l;
        String value = "My Value";

        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        when( store.getIndexDescriptor( 1337l ) ).thenReturn( indexDescriptor );
        when( store.exactIndexLookup( 1337l, value ) ).then( asAnswer( asList( 2l, 3l ) ) );

        when( store.isLabelSetOnNode( labelId, 1l ) ).thenReturn( true );
        when( oldTxState.getNodesWithChangedProperty( propertyKeyId, value ) ).thenReturn(
                new DiffSets<Long>( Collections.<Long>emptySet(), asSet( 1l ) ) );

        // When
        txContext.addLabelToNode( labelId, 1l );
        Iterator<Long> result = txContext.exactIndexLookup( 1337l, value );

        // Then
        assertThat( asSet( result ), equalTo( asSet( 2l, 3l ) ) );
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
    private final long key1 = 45, key2 = 46, ruleId = 9;
    private int rulesCreated;

    private StatementContext store;
    private OldTxStateBridge oldTxState;
    private TxState state;
    private TransactionStateStatementContext txContext;

    @Before
    public void before() throws Exception
    {
        store = mock( StatementContext.class );
        when( store.getIndexRules( labelId1 ) ).then( asAnswer( Collections.<IndexRule>emptyList() ) );
        when( store.getIndexRules( labelId2 ) ).then( asAnswer( Collections.<IndexRule>emptyList() ) );
        when( store.getIndexRules() ).then( asAnswer( Collections.<IndexRule>emptyList() ) );
        when( store.addIndexRule( anyLong(), anyLong() ) ).thenAnswer( new Answer<IndexRule>()
        {
            @Override
            public IndexRule answer( InvocationOnMock invocation ) throws Throwable
            {
                return new IndexRule( ruleId + rulesCreated++,
                        (Long) invocation.getArguments()[0],
                        (SchemaIndexProvider.Descriptor) invocation.getArguments()[1],
                        (Long) invocation.getArguments()[2] );
            }
        } );

        oldTxState = mock( OldTxStateBridge.class );

        state = new TxState( oldTxState, mock( PersistenceManager.class ),
                mock( TxState.IdGeneration.class ), new DefaultSchemaIndexProviderMap( NO_INDEX_PROVIDER ) );

        txContext = new TransactionStateStatementContext( store, state );
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

    private void commitLabels( Labels... labels )
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
