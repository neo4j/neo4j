/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.test.TestGraphDatabaseFactory;

public class TestCacheTypes extends AbstractNeo4jTestCase
{
    private GraphDatabaseAPI newDb( String cacheType )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( GraphDatabaseSettings.cache_type.name(), cacheType ).newGraphDatabase();
    }

    @Test
    public void testDefaultCache()
    {
        GraphDatabaseAPI db = newDb( null );
        assertEquals( SoftCacheProvider.NAME, db.getNodeManager().getCacheType().getName() );
        db.shutdown();
    }

    @Test
    public void testWeakRefCache()
    {
        GraphDatabaseAPI db = newDb( WeakCacheProvider.NAME );
        assertEquals( WeakCacheProvider.NAME, db.getNodeManager().getCacheType().getName() );
        db.shutdown();
    }

    @Test
    public void testSoftRefCache()
    {
        GraphDatabaseAPI db = newDb( SoftCacheProvider.NAME );
        assertEquals( SoftCacheProvider.NAME, db.getNodeManager().getCacheType().getName() );
        db.shutdown();
    }

    @Test
    public void testNoCache()
    {
        GraphDatabaseAPI db = newDb( NoCacheProvider.NAME );
        assertEquals( NoCacheProvider.NAME, db.getNodeManager().getCacheType().getName() );
        db.shutdown();
    }

    @Test
    public void testStrongCache()
    {
        GraphDatabaseAPI db = newDb( StrongCacheProvider.NAME );
        assertEquals( StrongCacheProvider.NAME, db.getNodeManager().getCacheType().getName() );
        db.shutdown();
    }

    @Test
    public void testInvalidCache()
    {
        // invalid cache type should fail
        GraphDatabaseAPI db = null;
        try
        {
            db = newDb( "whatever" );
            fail( "Wrong cache type should not be allowed" );
        }
        catch( Exception e )
        {
            // Ok
        }
    }

    @Test
    public void softCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new SoftLruCache<EntityWithSizeObject>( "test" ) );
    }

    @Test
    public void weakCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new WeakLruCache<EntityWithSizeObject>( "test" ) );
    }

    @Test
    public void strongCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new StrongReferenceCache<EntityWithSizeObject>( "test" ) );
    }

    private void assertCacheHonorsPutsSemantics( Cache<EntityWithSizeObject> cache )
    {
        Entity version1 = new Entity( 10 );
        assertTrue( version1 == cache.put( version1 ) );

        // WHEN
        Entity version2 = new Entity( 10 );

        // THEN
        assertTrue( version1 == cache.put( version2 ) );
    }

    public static class Entity implements EntityWithSizeObject
    {
        private int registeredSize;
        private final long id;

        Entity( long id )
        {
            this.id = id;
        }

        @Override
        public int sizeOfObjectInBytesIncludingOverhead()
        {
            return 0;
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public void setRegisteredSize( int size )
        {
            registeredSize = size;
        }

        @Override
        public int getRegisteredSize()
        {
            return registeredSize;
        }
    }
}
