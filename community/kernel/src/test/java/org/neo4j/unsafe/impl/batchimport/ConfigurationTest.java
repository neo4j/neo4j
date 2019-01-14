/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;

import static org.hamcrest.Matchers.lessThan;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;
import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;
import static org.neo4j.unsafe.impl.batchimport.Configuration.MAX_PAGE_CACHE_MEMORY;

public class ConfigurationTest
{
    @Test
    public void shouldOverrideBigPageCacheMemorySettingContainingUnit()
    {
        // GIVEN
        Config dbConfig = Config.defaults( pagecache_memory, "2g" );
        Configuration config = new Configuration.Overridden( dbConfig );

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals( MAX_PAGE_CACHE_MEMORY, memory );
    }

    @Test
    public void shouldOverrideSmallPageCacheMemorySettingContainingUnit()
    {
        // GIVEN
        long overridden = parseLongWithUnit( "10m" );
        Config dbConfig = Config.defaults( pagecache_memory, valueOf( overridden ) );
        Configuration config = new Configuration.Overridden( dbConfig );

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals( overridden, memory );
    }

    @Test
    public void shouldParseDefaultPageCacheMemorySetting()
    {
        // GIVEN
        Configuration config = Configuration.DEFAULT;

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        long heuristic = ConfiguringPageCacheFactory.defaultHeuristicPageCacheMemory();
        assertTrue( within( memory, heuristic, MAX_PAGE_CACHE_MEMORY ) );
    }

    @Test
    public void shouldCalculateCorrectMaxMemorySetting() throws Exception
    {
        long totalMachineMemory = OsBeanUtil.getTotalPhysicalMemory();
        assumeTrue( totalMachineMemory != VALUE_UNAVAILABLE );

        // given
        int percent = 70;
        Configuration config = new Configuration()
        {
            @Override
            public long maxMemoryUsage()
            {
                return Configuration.calculateMaxMemoryFromPercent( percent );
            }
        };

        // when
        long memory = config.maxMemoryUsage();

        // then
        long expected = (long) ((totalMachineMemory - Runtime.getRuntime().maxMemory()) * (percent / 100D));
        long diff = abs( expected - memory );
        assertThat( diff, lessThan( (long)(expected / 10D) ) );
    }

    private boolean within( long value, long firstBound, long otherBound )
    {
        return value >= min( firstBound, otherBound ) && value <= max( firstBound, otherBound );
    }
}
