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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationsTestHelper;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreStatement;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.emptySetOf;

public class SchemaTransactionStateTest
{
    private static IndexDescriptor indexCreate( StateHandlingStatementOperations txContext, KernelStatement state,
            int labelId, int propertyKey )
    {
        return txContext.indexCreate( state, SchemaDescriptorFactory.forLabel( labelId, propertyKey ) );
    }

    private static IndexDescriptor indexGetForLabelAndPropertyKey(
            StateHandlingStatementOperations txContext, KernelStatement state, int labelId, int propertyKey )
    {
        LabelSchemaDescriptor schemaDescriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKey );
        return txContext.indexGetForSchema( state, schemaDescriptor );
    }

    private static IndexDescriptor indexGetForLabelAndPropertyKey( StoreReadLayer store, int labelId,
            int propertyKey )
    {
        return store.indexGetForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyKey ) );
    }

    @Test
    public void addedRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor index = indexCreate( txContext, state, labelId1, key1 );

        // THEN
        assertEquals( asSet( index ), Iterators.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
        verify( store ).indexesGetForLabel( labelId1 );

        assertEquals( asSet( index ), Iterators.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedRulesShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = indexCreate( txContext, state, labelId1, key1 );
        IndexDescriptor rule2 = indexCreate( txContext, state, labelId2, key2 );

        // THEN
        assertEquals( asSet( rule1 ), Iterators.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
        verify( store ).indexesGetForLabel( labelId1 );

        assertEquals( asSet( rule2 ), Iterators.asSet( txContext.indexesGetForLabel( state, labelId2 ) ) );
        verify( store ).indexesGetForLabel( labelId2 );

        assertEquals( asSet( rule1, rule2 ), Iterators.asSet( txContext.indexesGetAll( state ) ) );
        verify( store ).indexesGetAll();

        verifyNoMoreInteractions( store );
    }

    @Test
    public void addedAdditionalRuleShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitNoLabels();

        // WHEN
        IndexDescriptor rule1 = indexCreate( txContext, state, labelId1, key1 );
        IndexDescriptor rule2 = indexCreate( txContext, state, labelId1, key2 );

        // THEN
        assertEquals( asSet( rule1, rule2 ), Iterators.asSet( txContext.indexesGetForLabel( state, labelId1 ) ) );
    }

    @Test
    public void creatingAnIndexShouldBePopulatingStateWithinTX() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        IndexDescriptor rule = indexCreate( txContext, state, labelId1, key1 );

        // THEN
        assertEquals( InternalIndexState.POPULATING, txContext.indexGetState( state, rule ) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransaction() throws Exception
    {
        // GIVEN
        // -- non-existent rule added in the transaction
        indexCreate( txContext, state, labelId1, key1 );

        // WHEN
        IndexDescriptor index = indexGetForLabelAndPropertyKey( txContext, state, labelId1, key1 );
        Iterator<IndexDescriptor> labelRules = txContext.indexesGetForLabel( state, labelId1 );

        // THEN
        IndexDescriptor expectedRule = IndexDescriptorFactory.forLabel( labelId1, key1 );
        assertEquals( expectedRule, index );
        assertEquals( asSet( expectedRule ), asSet( labelRules ) );
    }

    @Test
    public void shouldReturnNonExistentRuleAddedInTransactionFromLookup() throws Exception
    {
        // GIVEN
        // -- the store already have an index on the label and a different property
        IndexDescriptor existingRule1 = IndexDescriptorFactory.forLabel( labelId1, key1 );
        when( indexGetForLabelAndPropertyKey( store, labelId1, key1 ) ).thenReturn( existingRule1 );
        // -- the store already have an index on a different label with the same property
        IndexDescriptor existingRule2 = IndexDescriptorFactory.forLabel( labelId2, key2 );
        when( indexGetForLabelAndPropertyKey( store, labelId2, key2 ) ).thenReturn( existingRule2 );
        // -- a non-existent index has been added in the transaction
        indexCreate( txContext, state, labelId1, key2 );

        // WHEN
        IndexDescriptor index = indexGetForLabelAndPropertyKey( txContext, state, labelId1, key2 );

        // THEN
        assertEquals( IndexDescriptorFactory.forLabel( labelId1, key2 ), index );
    }

    @Test
    public void shouldNotReturnRulesAddedInTransactionWithDifferentLabelOrPropertyFromLookup() throws Exception
    {
        // GIVEN
        // -- the store already have an index on the label and a different property
        IndexDescriptor existingIndex1 = IndexDescriptorFactory.forLabel( labelId1, key1 );
        when( indexGetForLabelAndPropertyKey( store,labelId1, key1) ).thenReturn( existingIndex1 );
        // -- the store already have an index on a different label with the same property
        IndexDescriptor existingIndex2 = IndexDescriptorFactory.forLabel( labelId2, key2 );
        when( indexGetForLabelAndPropertyKey( store, labelId2, key2 ) ).thenReturn( existingIndex2 );
        // -- a non-existent rule has been added in the transaction
        indexCreate( txContext, state, labelId1, key2 );

        // WHEN
        IndexDescriptor lookupRule1 = indexGetForLabelAndPropertyKey( txContext, state, labelId1, key1 );
        IndexDescriptor lookupRule2 = indexGetForLabelAndPropertyKey( txContext, state, labelId2, key2 );

        // THEN
        assertEquals( existingIndex1, lookupRule1 );
        assertEquals( existingIndex2, lookupRule2 );
    }

    @Test
    public void shouldNotReturnExistentRuleDroppedInTransaction() throws Exception
    {
        // GIVEN
        // -- a rule that exists in the store
        IndexDescriptor index = IndexDescriptorFactory.forLabel( labelId1, key1 );
        when( store.indexesGetForLabel( labelId1 ) ).thenReturn( option( index ).iterator() );
        // -- that same rule dropped in the transaction
        txContext.indexDrop( state, index );

        // WHEN
        assertNull( indexGetForLabelAndPropertyKey( txContext, state, labelId1, key1 ) );
        Iterator<IndexDescriptor> rulesByLabel = txContext.indexesGetForLabel( state, labelId1 );

        // THEN
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( rulesByLabel ) );
    }

    // exists

    private final int labelId1 = 10, labelId2 = 12, key1 = 45, key2 = 46;
    private final long nodeId = 20;

    private StoreReadLayer store;
    private TransactionState txState;
    private StateHandlingStatementOperations txContext;
    private KernelStatement state;
    private StoreStatement storeStatement;

    @Before
    public void before() throws Exception
    {
        txState = new TxState();
        state = StatementOperationsTestHelper.mockedState( txState );

        store = mock( StoreReadLayer.class );
        when( store.indexesGetForLabel( labelId1 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetForLabel( labelId2 ) ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );
        when( store.indexesGetAll() ).then( asAnswer( Collections.<IndexDescriptor>emptyList() ) );

        txContext = new StateHandlingStatementOperations( store, mock( InternalAutoIndexing.class ),
                mock( ConstraintIndexCreator.class ), mock( LegacyIndexStore.class ) );

        storeStatement = mock(StoreStatement.class);
        when( state.getStoreStatement() ).thenReturn( storeStatement );
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

    private void commitLabels( Labels... labels ) throws Exception
    {
        Map<Integer, Collection<Long>> allLabels = new HashMap<>();
        for ( Labels nodeLabels : labels )
        {
            when( storeStatement.acquireSingleNodeCursor( nodeLabels.nodeId ) ).thenReturn(
                    StubCursors.asNodeCursor( nodeLabels.nodeId, StubCursors.labels( nodeLabels.labelIds ) ) );

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
            when( store.nodesGetForLabel( state.getStoreStatement(), entry.getKey() ) )
                    .then( asAnswer( entry.getValue() ) );
        }
    }

    private void commitNoLabels() throws Exception
    {
        commitLabels( new int[0] );
    }

    private void commitLabels( int... labels ) throws Exception
    {
        commitLabels( labels( nodeId, labels ) );
    }
}
