/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.IteratorUtil.first;

import java.util.Map;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.ha.HaCaches;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestCacheObjectReuse
{
    @Test
    public void gcrCachesCanBeReusedBetweenSessions() throws Exception
    {
        AbstractGraphDatabase db = new HaImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE,
                GCResistantCacheProvider.NAME ) );
        Cache<?> firstCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
        db.shutdown();

        db = new HaImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE, GCResistantCacheProvider.NAME ) );
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
        AbstractGraphDatabase db = new HaImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE,
                GCResistantCacheProvider.NAME ) );
        Cache<?> firstCache = first( db.getConfig().getGraphDbModule().getNodeManager().caches() );
        db.shutdown();

        db = new HaImpermanentGraphDatabase( MapUtil.stringMap( Config.CACHE_TYPE, GCResistantCacheProvider.NAME,
                HaConfig.NODE_CACHE_ARRAY_FRACTION, "10" ) );

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

    private static class HaImpermanentGraphDatabase extends ImpermanentGraphDatabase
    {
        private static String lastStoreDir;
        private static HaCaches caches;

        public HaImpermanentGraphDatabase( Map<String, String> config )
        {
            super( config );
        }

        @Override
        protected Caches createCaches( StringLogger logger )
        {
            if ( caches == null )
                caches = new HaCaches( getMessageLog() );
            else if ( lastStoreDir == null || !lastStoreDir.equals( getStoreDir() ) ) caches.invalidate();
            lastStoreDir = getStoreDir();
            return caches;
        }
    }
}