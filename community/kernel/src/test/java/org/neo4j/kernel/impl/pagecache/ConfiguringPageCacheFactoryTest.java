/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_swapper;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;

public class ConfiguringPageCacheFactoryTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Before
    public void setUp()
    {
        PageSwapperFactoryForTesting.createdCounter.set( 0 );
        PageSwapperFactoryForTesting.configuredCounter.set( 0 );
        PageSwapperFactoryForTesting.cachePageSizeHint.set( 0 );
        PageSwapperFactoryForTesting.cachePageSizeHintIsStrict.set( false );
    }

    @Test
    public void shouldUseConfiguredPageSizeAndFitAsManyPagesAsItCan() throws Throwable
    {
        // Given
        final int pageSize = 4096;
        final int maxPages = 60;
        Config config = new Config( stringMap(
                mapped_memory_page_size.name(), "" + pageSize,
                pagecache_memory.name(), Integer.toString( pageSize * maxPages ) ) );

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fsRule.get(), config, PageCacheTracer.NULL, NullLog.getInstance() );

        // Then
        try ( PageCache cache = factory.getOrCreatePageCache() )
        {
            assertThat( cache.pageSize(), equalTo( pageSize ) );
            assertThat( cache.maxCachedPages(), equalTo( maxPages ) );
        }
    }

    @Test
    public void mustUseAndLogConfiguredPageSwapper() throws Exception
    {
        // Given
        Config config = new Config( stringMap(
                pagecache_memory.name(), "8m",
                pagecache_swapper.name(), TEST_PAGESWAPPER_NAME ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( PageCache.class );

        // When
        new ConfiguringPageCacheFactory( fsRule.get(), config, PageCacheTracer.NULL, log );

        // Then
        assertThat( PageSwapperFactoryForTesting.countCreatedPageSwapperFactories(), is( 1 ) );
        assertThat( PageSwapperFactoryForTesting.countConfiguredPageSwapperFactories(), is( 1 ) );
        logProvider.assertContainsMessageContaining( TEST_PAGESWAPPER_NAME );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowIfConfiguredPageSwapperCannotBeFound() throws Exception
    {
        // Given
        Config config = new Config( stringMap(
                pagecache_memory.name(), "8m",
                pagecache_swapper.name(), "non-existing" ) );

        // When
        new ConfiguringPageCacheFactory( fsRule.get(), config, PageCacheTracer.NULL, NullLog.getInstance() );
    }

    @Test
    public void mustUsePageSwapperCachePageSizeHintAsDefault() throws Exception
    {
        // Given
        int cachePageSizeHint = 16 * 1024;
        PageSwapperFactoryForTesting.cachePageSizeHint.set( cachePageSizeHint );
        Config config = new Config( stringMap(
                GraphDatabaseSettings.pagecache_swapper.name(), TEST_PAGESWAPPER_NAME ) );

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fsRule.get(), config, PageCacheTracer.NULL, NullLog.getInstance() );

        // Then
        try ( PageCache cache = factory.getOrCreatePageCache() )
        {
            assertThat( cache.pageSize(), is( cachePageSizeHint ) );
        }
    }

    @Test
    public void mustIgnoreExplicitlySpecifiedCachePageSizeIfPageSwapperHintIsStrict() throws Exception
    {
        // Given
        int cachePageSizeHint = 16 * 1024;
        PageSwapperFactoryForTesting.cachePageSizeHint.set( cachePageSizeHint );
        PageSwapperFactoryForTesting.cachePageSizeHintIsStrict.set( true );
        Config config = new Config( stringMap(
                GraphDatabaseSettings.mapped_memory_page_size.name(), "4096",
                GraphDatabaseSettings.pagecache_swapper.name(), TEST_PAGESWAPPER_NAME ) );

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fsRule.get(), config, PageCacheTracer.NULL, NullLog.getInstance() );

        // Then
        try ( PageCache cache = factory.getOrCreatePageCache() )
        {
            assertThat( cache.pageSize(), is( cachePageSizeHint ) );
        }
    }

}
