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
package org.neo4j.kernel.impl.pagecache;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConfiguringPageCacheFactoryTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Test
    public void shouldUseConfiguredPageSizeAndFitAsManyPagesAsItCan() throws Throwable
    {
        // Given
        Config config = new Config();
        config.applyChanges( stringMap(
                mapped_memory_page_size.name(), "4096",
                pagecache_memory.name(), Integer.toString( 4096 * 16 ) ) );

        // When
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fsRule.get(), config, PageCacheTracer.NULL );
        PageCache cache = pageCacheFactory.getOrCreatePageCache();

        // Then
        try
        {
            assertThat( cache.pageSize(), equalTo( 4096 ) );
            assertThat( cache.maxCachedPages(), equalTo( 16 ) );
        }
        finally
        {
            cache.close();
        }
    }
}
