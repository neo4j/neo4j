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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.neo4j.kernel.impl.cache.LockStripedCache;

public class PersistenceCacheTest
{
    @Test
    public void shouldAddLabelsFromTxState() throws Exception
    {
        long nodeId = 5;
        LockStripedCache.Loader<PersistenceCache.CachedNodeEntity> loader = mock( LockStripedCache.Loader.class );
        when( loader.loadById( nodeId ) ).thenReturn( new PersistenceCache.CachedNodeEntity( nodeId ) );
        PersistenceCache cache = new PersistenceCache( loader );
        TxState state = mock( TxState.class );
        Set<Long> labels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        when( state.getAddedLabels( eq( nodeId ), anyBoolean() ) ).thenReturn( labels );
        TxState.NodeState nodeState = new TxState.NodeState( nodeId );
        nodeState.getAddedLabels().addAll( labels );
        when( state.getNodes() ).thenReturn( asList( nodeState ) );
        cache.getLabels( nodeId );

        // WHEN
        cache.apply( state );

        // THEN
        assertEquals( labels, cache.getLabels( nodeId ) );
    }

    @Test
    public void shouldRemoveLabelsFromTxState() throws Exception
    {
        long nodeId = 5;
        LockStripedCache.Loader<PersistenceCache.CachedNodeEntity> loader = mock( LockStripedCache.Loader.class );
        PersistenceCache.CachedNodeEntity cachedNode = new PersistenceCache.CachedNodeEntity( nodeId );
        Set<Long> initialLabels = new HashSet<Long>( asList( 1L, 2L, 3L ) );
        cachedNode.addLabels( initialLabels );
        when( loader.loadById( nodeId ) ).thenReturn( cachedNode );
        PersistenceCache cache = new PersistenceCache( loader );
        TxState state = mock( TxState.class );
        when( state.getAddedLabels( eq( nodeId ), anyBoolean() ) ).thenReturn( initialLabels );
        TxState.NodeState nodeState = new TxState.NodeState( nodeId );
        Set<Long> removedLabels = new HashSet<Long>( asList( 2L ) );
        nodeState.getRemovedLabels().addAll( removedLabels );
        when( state.getNodes() ).thenReturn( asList( nodeState ) );
        cache.getLabels( nodeId );

        // WHEN
        cache.apply( state );

        // THEN
        Set<Long> expectedLabels = new HashSet<Long>( initialLabels );
        expectedLabels.removeAll( removedLabels );
        assertEquals( expectedLabels, cache.getLabels( nodeId ) );
    }
}
