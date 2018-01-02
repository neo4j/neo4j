/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.valueOf;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;
import static org.neo4j.unsafe.impl.batchimport.Configuration.MAX_PAGE_CACHE_MEMORY;

public class ConfigurationTest
{
    @Test
    public void shouldOverrideBigPageCacheMemorySettingContainingUnit() throws Exception
    {
        // GIVEN
        Config dbConfig = new Config( stringMap( pagecache_memory.name(), "2g" ) );
        Configuration config = new Configuration.Overridden( dbConfig );

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals( MAX_PAGE_CACHE_MEMORY, memory );
    }

    @Test
    public void shouldOverrideSmallPageCacheMemorySettingContainingUnit() throws Exception
    {
        // GIVEN
        long overridden = parseLongWithUnit( "10m" );
        Config dbConfig = new Config( stringMap( pagecache_memory.name(), valueOf( overridden ) ) );
        Configuration config = new Configuration.Overridden( dbConfig );

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals( overridden, memory );
    }

    @Test
    public void shouldParseDefaultPageCacheMemorySetting() throws Exception
    {
        // GIVEN
        Configuration config = Configuration.DEFAULT;

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertTrue( within( memory, new Config().get( pagecache_memory ), MAX_PAGE_CACHE_MEMORY ) );
    }

    private boolean within( long value, long firstBound, long otherBound )
    {
        return value >= min( firstBound, otherBound ) && value <= max( firstBound, otherBound );
    }
}
