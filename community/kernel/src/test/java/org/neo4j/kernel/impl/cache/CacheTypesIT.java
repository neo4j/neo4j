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
package org.neo4j.kernel.impl.cache;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CacheTypesIT
{
    private GraphDatabaseAPI newDb( String cacheType )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( GraphDatabaseSettings.cache_type.name(), cacheType ).newGraphDatabase();
    }

    @Test
    public void testDefaultCache()
    {
        GraphDatabaseAPI db = newDb( null );
        assertEquals( SoftCacheProvider.NAME, caches( db ).getProvider().getName() );
        db.shutdown();
    }

    @Test
    public void testWeakRefCache()
    {
        GraphDatabaseAPI db = newDb( WeakCacheProvider.NAME );
        assertEquals( WeakCacheProvider.NAME, caches( db ).getProvider().getName() );
        db.shutdown();
    }

    @Test
    public void testSoftRefCache()
    {
        GraphDatabaseAPI db = newDb( SoftCacheProvider.NAME );
        assertEquals( SoftCacheProvider.NAME, caches( db ).getProvider().getName() );
        db.shutdown();
    }

    @Test
    public void testNoCache()
    {
        GraphDatabaseAPI db = newDb( NoCacheProvider.NAME );
        assertEquals( NoCacheProvider.NAME, caches( db ).getProvider().getName() );
        db.shutdown();
    }

    @Test
    public void testStrongCache()
    {
        GraphDatabaseAPI db = newDb( StrongCacheProvider.NAME );
        assertEquals( StrongCacheProvider.NAME, caches( db ).getProvider().getName() );
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

    private Caches caches( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( Caches.class );
    }
}
