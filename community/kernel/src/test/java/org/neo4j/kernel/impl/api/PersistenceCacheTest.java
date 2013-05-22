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
import org.mockito.Mockito;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.PersistenceCache.CachedNodeEntity;
import org.neo4j.kernel.impl.cache.LockStripedCache;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.set;

public class PersistenceCacheTest
{

    @Test
    public void shouldEvictNodeWhenITellItTo() throws Exception
    {
        // Given
        when( loader.loadById( nodeId ) ).thenReturn( new CachedNodeEntity( nodeId ) );
        cache.getLabels( nodeId );

        // When
        cache.evictNode( nodeId );

        // Then
        cache.getLabels( nodeId );
        verify( loader, times( 2 ) ).loadById( nodeId );
    }

    @Test
    public void shouldLoadAndCacheLabelsWhenIAskForStuff() throws Exception
    {
        // Given
        CachedNodeEntity node = new CachedNodeEntity( nodeId );
        Set<Long> labels = set( 1l, 2l, 3l );
        node.addLabels( labels );
        when( loader.loadById( nodeId ) ).thenReturn( node );

        // When
        Set<Long> l1 = cache.getLabels( nodeId );
        Set<Long> l2 = cache.getLabels( nodeId );
        Set<Long> l3 = cache.getLabels( nodeId );

        // Then
        verify( loader, times( 1 ) ).loadById( nodeId );

        assertThat( l1, equalTo(labels) );
        assertThat( l2, equalTo(labels) );
        assertThat( l3, equalTo(labels) );
    }

    @Test
    public void shouldHandleNodeNotExistingAtAll() throws Exception
    {
        // Given
        when( loader.loadById( nodeId ) ).thenReturn( null );

        // When
        try
        {
            cache.getLabels( nodeId );
            fail( "Expected exception" );
        }
        catch(EntityNotFoundException e)
        {
            // Yay!
        }

        // Then
        verify( loader, times( 1 ) ).loadById( nodeId );
    }

    private final long nodeId = 5;
    private LockStripedCache.Loader<PersistenceCache.CachedNodeEntity> loader;
    private PersistenceCache cache;

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws Exception
    {
        loader = Mockito.mock( LockStripedCache.Loader.class );
        cache = new PersistenceCache( loader );
    }
}
