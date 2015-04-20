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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class GraphDatabaseSettingsTest
{
    private static final long KiB = 1024;
    private static final long MiB = KiB * 1024;
    private static final long GiB = MiB * 1024;
    @Test
    public void mustHaveReasonableDefaultPageCacheMemorySizeInBytes() throws Exception
    {
        long bytes = new Config().get( GraphDatabaseSettings.pagecache_memory );
        assertThat( bytes, greaterThanOrEqualTo( 32 * MiB ) );
        assertThat( bytes, lessThanOrEqualTo( 1024 * GiB ) );
    }

    @Test
    public void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue() throws Exception
    {
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        assertThat( new Config( stringMap( name, "16384" ) ).get( setting ), is( 16 * KiB ) );
        assertThat( new Config( stringMap( name, "2244g" ) ).get( setting ), is( 2244 * GiB ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void pageCacheSettingMustRejectOverlyConstrainedMemorySetting() throws Exception
    {
        long pageSize = new Config().get( GraphDatabaseSettings.mapped_memory_page_size );
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        // We configure the page cache to have one byte less than two pages worth of memory. This must throw:
        new Config( stringMap( name, "" + (pageSize * 2 - 1) ) ).get( setting );
    }
}
