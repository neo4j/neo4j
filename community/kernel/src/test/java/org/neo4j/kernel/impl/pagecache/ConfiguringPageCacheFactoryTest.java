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
package org.neo4j.kernel.impl.pagecache;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
    }

    @Test
    public void shouldFitAsManyPagesAsItCan()
    {
        // Given
        long pageCount = 60;
        long memory = MuninnPageCache.memoryRequiredForPages( pageCount );
        Config config = Config.defaults(
                pagecache_memory, Long.toString( memory ) );

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fsRule.get(), config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                NullLog.getInstance(), EmptyVersionContextSupplier.EMPTY );

        // Then
        try ( PageCache cache = factory.getOrCreatePageCache() )
        {
            assertThat( cache.pageSize(), equalTo( PageCache.PAGE_SIZE ) );
            assertThat( cache.maxCachedPages(), equalTo( pageCount ) );
        }
    }

    @Test
    public void shouldWarnWhenCreatedWithConfiguredPageCache()
    {
        // Given
        Config config = Config.defaults( stringMap(
                GraphDatabaseSettings.mapped_memory_page_size.name(), "4096",
                pagecache_swapper.name(), TEST_PAGESWAPPER_NAME ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( PageCache.class );

        // When
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fsRule.get(), config,
                PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, log, EmptyVersionContextSupplier.EMPTY );

        // Then
        try ( PageCache ignore = pageCacheFactory.getOrCreatePageCache() )
        {
            logProvider.assertContainsLogCallContaining(
                    "The setting unsupported.dbms.memory.pagecache.pagesize does not have any effect. It is " +
                            "deprecated and will be removed in a future version." );
        }
    }

    @Test
    public void mustUseAndLogConfiguredPageSwapper()
    {
        // Given
        Config config = Config.defaults( stringMap(
                pagecache_memory.name(), "8m",
                pagecache_swapper.name(), TEST_PAGESWAPPER_NAME ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( PageCache.class );

        // When
        ConfiguringPageCacheFactory cacheFactory = new ConfiguringPageCacheFactory( fsRule.get(), config, PageCacheTracer.NULL,
                        PageCursorTracerSupplier.NULL, log, EmptyVersionContextSupplier.EMPTY );
        cacheFactory.getOrCreatePageCache().close();

        // Then
        assertThat( PageSwapperFactoryForTesting.countCreatedPageSwapperFactories(), is( 1 ) );
        assertThat( PageSwapperFactoryForTesting.countConfiguredPageSwapperFactories(), is( 1 ) );
        logProvider.assertContainsMessageContaining( TEST_PAGESWAPPER_NAME );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowIfConfiguredPageSwapperCannotBeFound()
    {
        // Given
        Config config = Config.defaults( stringMap(
                pagecache_memory.name(), "8m",
                pagecache_swapper.name(), "non-existing" ) );

        // When
        new ConfiguringPageCacheFactory( fsRule.get(), config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                NullLog.getInstance(), EmptyVersionContextSupplier.EMPTY ).getOrCreatePageCache().close();
    }
}
