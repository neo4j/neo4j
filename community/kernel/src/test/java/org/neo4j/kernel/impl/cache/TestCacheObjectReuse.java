/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.IteratorUtil.first;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestCacheObjectReuse
{
    @Test
    public void gcrCachesCanBeReusedBetweenSessions() throws Exception
    {
        AbstractGraphDatabase db = new ImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE,
                CacheType.gcr.name() ) );
        Cache<?> firstCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
        db.shutdown();

        db = new ImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE, CacheType.gcr.name() ) );
        try
        {
            Cache<?> secondCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
            assertEquals( firstCache, secondCache );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void gcrCachesAreRecreatedBetweenSessionsIfConfigChanges() throws Exception
    {
        AbstractGraphDatabase db = new ImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE,
                CacheType.gcr.name() ) );
        Cache<?> firstCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
        db.shutdown();

        db = new ImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE, CacheType.gcr.name(),
                Config.NODE_CACHE_ARRAY_FRACTION, "10" ) );

        try
        {
            Cache<?> secondCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
            assertFalse( firstCache.equals( secondCache ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}