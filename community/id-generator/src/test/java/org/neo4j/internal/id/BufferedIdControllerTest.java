/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;

@EphemeralPageCacheExtension
class BufferedIdControllerTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    private BufferingIdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setUp()
    {
        idGeneratorFactory = new BufferingIdGeneratorFactory( new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME ) );
        idGeneratorFactory.initialize( () -> () -> true, EmptyMemoryTracker.INSTANCE );
    }

    @Test
    void shouldStopWhenNotStarted()
    {
        BufferedIdController controller = newController( new CursorContextFactory( PageCacheTracer.NULL, EMPTY ) );

        assertDoesNotThrow( controller::stop );
    }

    @Test
    void reportPageCacheMetricsOnMaintenance() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory( pageCacheTracer, EMPTY );

        try ( var idGenerator = idGeneratorFactory.create( pageCache, testDirectory.file( "foo" ), TestIdType.TEST, 100L, true, 1000L, writable(),
                Config.defaults(), contextFactory, immutable.empty(), SINGLE_IDS ) )
        {
            idGenerator.marker( NULL_CONTEXT ).markDeleted( 1L );
            idGenerator.clearCache( NULL_CONTEXT );

            long initialPins = pageCacheTracer.pins();
            long initialUnpins = pageCacheTracer.unpins();
            long initialHits = pageCacheTracer.hits();

            var controller = newController( contextFactory );
            controller.maintenance();

            assertThat( pageCacheTracer.pins() - initialPins ).isOne();
            assertThat( pageCacheTracer.unpins() - initialUnpins ).isOne();
            assertThat( pageCacheTracer.hits() - initialHits ).isOne();
        }
    }

    private BufferedIdController newController( CursorContextFactory contextFactory )
    {
        BufferedIdController controller = new BufferedIdController( idGeneratorFactory, new OnDemandJobScheduler(), contextFactory, "test db" );
        controller.initialize( () -> () -> true, EmptyMemoryTracker.INSTANCE );
        return controller;
    }
}
