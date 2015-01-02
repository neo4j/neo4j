/**
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
package org.neo4j.graphdb.factory;

import java.util.ArrayList;

import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetCacheProvidersTest
{
    @Test
    public void testSetNoCache()
    {
        ArrayList<CacheProvider> cacheList = new ArrayList<>();
        TestGraphDatabaseFactory gdbf = new TestGraphDatabaseFactory();
        gdbf.setCacheProviders( cacheList );
        try
        {
            gdbf.newImpermanentDatabase();
        }
        catch ( IllegalArgumentException iae )
        {
            assertTrue( iae.getMessage().contains( "No provider for cache type" ) );
            assertTrue( iae.getMessage().contains( "register" ) );
            assertTrue( iae.getMessage().contains( "missing" ) );
        }
    }

    @Test
    public void testSetSoftRefCache()
    {
        ArrayList<CacheProvider> cacheList = new ArrayList<>();
        TestGraphDatabaseFactory gdbf = new TestGraphDatabaseFactory();
        cacheList.add( new SoftCacheProvider() );
        gdbf.setCacheProviders( cacheList );
        GraphDatabaseAPI db = (GraphDatabaseAPI) gdbf.newImpermanentDatabase();
        assertEquals( SoftCacheProvider.NAME, db.getDependencyResolver().resolveDependency( NodeManager.class )
                .getCacheType().getName() );
    }
}
