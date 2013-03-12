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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.api.PersistenceCache.CachedNodeEntity;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.cache.LockStripedCache;

public class PersistenceCacheTest
{
    @Test
    public void shouldAddLabelsFromTxState() throws Exception
    {
        when( loader.loadById( nodeId ) ).thenReturn( new CachedNodeEntity( nodeId ) );
        Set<Long> labels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        @SuppressWarnings( "unchecked" )
        DiffSets<Long> diff = mock( DiffSets.class );
        when( diff.getAdded() ).thenReturn( labels );
        when( state.getNodeStateLabelDiffSets( nodeId ) ).thenReturn( diff );
        nodeState.getLabelDiffSets().addAll( labels );
        when( state.getNodeStates() ).thenReturn( asList( nodeState ) );
        cache.getLabels( nodeId );

        // WHEN
        cache.apply( state );

        // THEN
        assertEquals( labels, cache.getLabels( nodeId ) );
    }

    @Test
    public void shouldRemoveLabelsFromTxState() throws Exception
    {
        CachedNodeEntity cachedNode = new CachedNodeEntity( nodeId );
        Set<Long> initialLabels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        cachedNode.addLabels( initialLabels );
        when( loader.loadById( nodeId ) ).thenReturn( cachedNode );
        @SuppressWarnings( "unchecked" )
        DiffSets<Long> diff = mock( DiffSets.class );
        when( diff.getAdded() ).thenReturn( initialLabels );
        when( state.getNodeStateLabelDiffSets( nodeId ) ).thenReturn( diff );
        Set<Long> removedLabels = new HashSet<Long>( asList( 2L ) );
        nodeState.getLabelDiffSets().removeAll( removedLabels );
        when( state.getNodeStates() ).thenReturn( asList( nodeState ) );
        cache.getLabels( nodeId );

        // WHEN
        cache.apply( state );

        // THEN
        Set<Long> expectedLabels = new HashSet<Long>( initialLabels );
        expectedLabels.removeAll( removedLabels );
        assertEquals( expectedLabels, cache.getLabels( nodeId ) );
    }
    
    private final long nodeId = 5;
    private LockStripedCache.Loader<PersistenceCache.CachedNodeEntity> loader;
    private PersistenceCache cache;
    private TxState state;
    private TxState.NodeState nodeState;
    
    @Before
    public void before() throws Exception
    {
        loader = mock( LockStripedCache.Loader.class );
        cache = new PersistenceCache( loader );
        state = mock( TxState.class );
        nodeState = new TxState.NodeState( nodeId );
    }
}
