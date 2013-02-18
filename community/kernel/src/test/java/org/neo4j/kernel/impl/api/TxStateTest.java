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
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Set;

import org.junit.Test;

public class TxStateTest
{
    @Test
    public void shouldGetAddedLabels() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 2, 1 );

        // WHEN
        Set<Long> addedLabels = state.getNodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( asSet( 1L, 2L ), addedLabels );
    }
    
    @Test
    public void shouldGetRemovedLabels() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 2, 1 );

        // WHEN
        Set<Long> removedLabels = state.getNodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( asSet( 1L, 2L ), removedLabels );
    }
    
    @Test
    public void removeAddedLabelShouldRemoveFromAdded() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 2, 1 );

        // WHEN
        state.removeLabelFromNode( 1, 1 );

        // THEN
        assertEquals( asSet( 2L ), state.getNodeStateLabelDiffSets( 1 ).getAdded() );
    }
    
    @Test
    public void addRemovedLabelShouldRemoveFromRemoved() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 2, 1 );

        // WHEN
        state.addLabelToNode( 1, 1 );

        // THEN
        assertEquals( asSet( 2L ), state.getNodeStateLabelDiffSets( 1 ).getRemoved() );
    }
    
    @Test
    public void shouldMapFromAddedLabelToNodes() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.addLabelToNode( 1, 0 );
        state.addLabelToNode( 2, 0 );
        state.addLabelToNode( 1, 1 );
        state.addLabelToNode( 3, 1 );
        state.addLabelToNode( 2, 2 );

        // WHEN
        Iterable<Long> nodes = state.getAddedNodesWithLabel( 2 );

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes() throws Exception
    {
        // GIVEN
        TxState state = new TxState();
        state.removeLabelFromNode( 1, 0 );
        state.removeLabelFromNode( 2, 0 );
        state.removeLabelFromNode( 1, 1 );
        state.removeLabelFromNode( 3, 1 );
        state.removeLabelFromNode( 2, 2 );

        // WHEN
        Iterable<Long> nodes = state.getRemovedNodesWithLabel( 2 );

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }
}
