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

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.Thunk;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.cache.LockStripedCache;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class PersistenceCacheTest
{
    @Test
    public void shouldLoadAndCacheNodeLabels() throws Exception
    {
        // GIVEN
        final Set<Long> labels = asSet( 1L, 2L, 3L );
        @SuppressWarnings( "unchecked" )
        CacheLoader<Set<Long>> loader = mock( CacheLoader.class );
        when( loader.load( state, nodeId ) ).thenReturn( labels );
        NodeImpl node = new NodeImpl( nodeId );
        when( nodeCache.get( nodeId ) ).thenReturn( node );
        
        // WHEN
        boolean hasLabel1 = persistenceCache.nodeHasLabel( state, nodeId, 1, loader );
        boolean hasLabel2 = persistenceCache.nodeHasLabel( state, nodeId, 2, loader );
        
        // THEN
        assertTrue( hasLabel1 );
        assertTrue( hasLabel2 );
        verify( loader, times( 1 ) ).load( state, nodeId );
        verify( nodeCache, times( 2 ) ).get( nodeId );
    }
    
    @Test
    public void shouldEvictNode() throws Exception
    {
        // WHEN
        persistenceCache.evictNode( nodeId );
        
        // THEN
        verify( nodeCache, times( 1 ) ).remove( nodeId );
    }
    
    private PersistenceCache persistenceCache;
    private LockStripedCache<NodeImpl> nodeCache;
    private final long nodeId = 1;
    private final StatementState state = mock( StatementState.class );
    
    @SuppressWarnings( "unchecked" )
    @Before
    public void init()
    {
        nodeCache = mock( LockStripedCache.class );
        LockStripedCache<RelationshipImpl> relCache = mock( LockStripedCache.class );
        persistenceCache = new PersistenceCache( nodeCache, relCache, mock( Thunk.class ) );
    }
}
