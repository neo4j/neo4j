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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.StatementContext;

public class TransactionStateAwareStatementContextTest
{
    @Test
    public void addOnlyLabelShouldBeVisibleInTx() throws Exception
    {
        // GIVEN
        commitLabels();
        
        // WHEN
        tx.addLabelToNode( labelId1, nodeId );
        
        // THEN
        assertLabels( labelId1 );
    }
    
    @Test
    public void addAdditionalLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        
        // WHEN
        tx.addLabelToNode( labelId2, nodeId );
        
        // THEN
        assertLabels( labelId1, labelId2 );
    }
    
    @Test
    public void addAlreadyExistingLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        
        // WHEN
        tx.addLabelToNode( labelId1, nodeId );
        
        // THEN
        assertLabels( labelId1 );
    }
    
    @Test
    public void removeCommittedLabelShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1, labelId2 );
        
        // WHEN
        tx.removeLabelFromNode( labelId1, nodeId );
        
        // THEN
        assertLabels( labelId2 );
    }

    @Test
    public void removeAddedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        
        // WHEN
        tx.addLabelToNode( labelId2, nodeId );
        tx.removeLabelFromNode( labelId2, nodeId );
        
        // THEN
        assertLabels( labelId1 );
    }
    
    @Test
    public void addRemovedLabelInTxShouldBeReflectedWithinTx() throws Exception
    {
        // GIVEN
        commitLabels( labelId1 );
        
        // WHEN
        tx.removeLabelFromNode( labelId1, nodeId );
        tx.addLabelToNode( labelId1, nodeId );
        
        // THEN
        assertLabels( labelId1 );
    }
    
    @Test
    public void unsuccessfulCloseShouldntDelegateDown() throws Exception
    {
        // GIVEN
        commitLabels();
        tx.addLabelToNode( labelId1, nodeId );
        
        // WHEN
        tx.close( false );

        // THEN
        verify( store, never() ).addLabelToNode( labelId1, nodeId );
    }
    
    @Test
    public void successfulCloseShouldDelegateDown() throws Exception
    {
        // GIVEN
        commitLabels();
        tx.addLabelToNode( labelId1, nodeId );
        
        // WHEN
        tx.close( true );

        // THEN
        verify( store ).addLabelToNode( labelId1, nodeId );
    }
    
    private final long labelId1 = 10, labelId2 = 12, nodeId = 20;
    private StatementContext store;
    private TxState state;
    private TransactionStateAwareStatementContext tx;
    
    @Before
    public void before() throws Exception
    {
        store = mock( StatementContext.class );
        state = new TxState();
        tx = new TransactionStateAwareStatementContext( store, state );
    }

    private void commitLabels( Long... labels )
    {
        when( store.getLabelsForNode( nodeId ) ).thenReturn( Arrays.<Long>asList( labels ) );
        for ( long label : labels )
        {
            when( store.isLabelSetOnNode( label, nodeId ) ).thenReturn( true );
        }
    }

    private void assertLabels( Long... labels )
    {
        assertEquals( asSet( labels ), asSet( tx.getLabelsForNode( nodeId ) ) );
        for ( long label : labels )
        {
            assertTrue( "Expected labels not found on node", tx.isLabelSetOnNode( label, nodeId ) );
        }
    }
}
