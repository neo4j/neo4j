/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.cache;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.IteratorUtil.first;

@Ignore( "Impermanent graph database doesn't use High-Performance Cache" )
public class TestCacheObjectReuse
{
    @Test
    public void highPerformanceCachesCanBeReusedBetweenSessions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cache_type, HighPerformanceCacheProvider.NAME ).newGraphDatabase();
        Cache<?> firstCache = first( nodeManager( db ).caches() );
        db.shutdown();
        
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cache_type, HighPerformanceCacheProvider.NAME ).newGraphDatabase();
        try
        {
            Cache<?> secondCache = first( nodeManager( db ).caches() );
            assertEquals( firstCache, secondCache );
        }
        finally
        {
            db.shutdown();
        }
    }

    private NodeManager nodeManager( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( NodeManager.class );
    }

    @Test
    public void highPerformanceCachesAreRecreatedBetweenSessionsIfConfigChanges() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cache_type, HighPerformanceCacheProvider.NAME ).newGraphDatabase();
        Cache<?> firstCache = first( nodeManager( db ).caches() );
        db.shutdown();
        
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cache_type, HighPerformanceCacheProvider.NAME )
                .setConfig( HighPerformanceCacheSettings.node_cache_array_fraction, "10" )
                .newGraphDatabase();
        try
        {
            Cache<?> secondCache = first( nodeManager( db ).caches() );
            assertFalse( firstCache.equals( secondCache ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}
