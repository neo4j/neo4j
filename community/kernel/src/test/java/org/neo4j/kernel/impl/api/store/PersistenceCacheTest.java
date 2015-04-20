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
package org.neo4j.kernel.impl.api.store;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipLoader;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

public class PersistenceCacheTest
{
    @Test
    public void shouldLoadAndCacheNodeLabels() throws Exception
    {
        // GIVEN
        int[] labels = new int[] {1, 2, 3};
        @SuppressWarnings( "unchecked" )
        CacheLoader<int[]> loader = mock( CacheLoader.class );
        when( loader.load( nodeId ) ).thenReturn( labels );
        NodeImpl node = new NodeImpl( nodeId );
        when( nodeCache.get( nodeId ) ).thenReturn( node );

        // WHEN
        boolean hasLabel1 = persistenceCache.nodeHasLabel( nodeId, 1, loader );
        boolean hasLabel2 = persistenceCache.nodeHasLabel( nodeId, 2, loader );

        // THEN
        assertTrue( hasLabel1 );
        assertTrue( hasLabel2 );
        verify( loader, times( 1 ) ).load( nodeId );
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

    @Test
    public void shouldApplyUpdates() throws Exception
    {
        // GIVEN
        NodeImpl node = mock(NodeImpl.class);
        when( nodeCache.getIfCached( nodeId ) ).thenReturn( node );

        // WHEN
        persistenceCache.apply(asList( labelChanges( nodeId, new long[]{2l}, new long[]{1l} )));

        // THEN
        verify(node).commitLabels( new int[]{1} );
    }

    private PersistenceCache persistenceCache;
    private AutoLoadingCache<NodeImpl> nodeCache;
    private final long nodeId = 1;

    @SuppressWarnings( "unchecked" )
    @Before
    public void init()
    {
        nodeCache = mock( AutoLoadingCache.class );
        AutoLoadingCache<RelationshipImpl> relCache = mock( AutoLoadingCache.class );
        EntityFactory entityFactory = mock( EntityFactory.class );
        GraphPropertiesImpl graphProperties = mock( GraphPropertiesImpl.class );
        when( entityFactory.newGraphProperties() ).thenReturn( graphProperties );
        persistenceCache = new PersistenceCache( nodeCache, relCache, entityFactory,
                mock( RelationshipLoader.class ), null, null, null );
    }
}
