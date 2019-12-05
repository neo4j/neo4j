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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;

@ExtendWith( EphemeralFileSystemExtension.class )
class ConfiguringPageCacheFactoryTest
{
    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp()
    {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void shouldFitAsManyPagesAsItCan()
    {
        // Given
        long pageCount = 60;
        long memory = MuninnPageCache.memoryRequiredForPages( pageCount );
        Config config = Config.defaults(
                pagecache_memory, Long.toString( memory ) );

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
            fs, config, PageCacheTracer.NULL, NullLog.getInstance(), EmptyVersionContextSupplier.EMPTY, jobScheduler );

        // Then
        try ( PageCache cache = factory.getOrCreatePageCache() )
        {
            assertThat( cache.pageSize() ).isEqualTo( PageCache.PAGE_SIZE );
            assertThat( cache.maxCachedPages() ).isEqualTo( pageCount );
        }
    }
}
