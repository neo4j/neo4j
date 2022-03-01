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

import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
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
@ExtendWith( LifeExtension.class )
class BufferedIdControllerTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private LifeSupport life;

    private BufferingIdGeneratorFactory idGeneratorFactory;
    private BufferedIdController controller;

    void setUp( CursorContextFactory contextFactory ) throws IOException
    {
        idGeneratorFactory = new BufferingIdGeneratorFactory( new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME ) );
        controller = new BufferedIdController( idGeneratorFactory, new OnDemandJobScheduler(), contextFactory, "test db" );
        controller.initialize( fs, testDirectory.file( "buffer" ), Config.defaults(),
                () -> new IdController.TransactionSnapshot( LongSets.immutable.empty(), 0, 0 ), s -> true, EmptyMemoryTracker.INSTANCE );
        life.add( controller );
    }

    @Test
    void shouldStopWhenNotStarted() throws IOException
    {
        setUp( new CursorContextFactory( PageCacheTracer.NULL, EMPTY ) );

        assertDoesNotThrow( controller::stop );
    }

    @Test
    void reportPageCacheMetricsOnMaintenance() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory( pageCacheTracer, EMPTY );
        setUp( contextFactory );

        try ( var idGenerator = idGeneratorFactory.create( pageCache, testDirectory.file( "foo" ), TestIdType.TEST, 100L, true, 1000L, writable(),
                Config.defaults(), contextFactory, immutable.empty(), SINGLE_IDS ) )
        {
            idGenerator.start( FreeIds.NO_FREE_IDS, NULL_CONTEXT );
            try ( var marker = idGenerator.marker( NULL_CONTEXT ) )
            {
                marker.markDeleted( 1L );
            }
            idGenerator.clearCache( NULL_CONTEXT );

            long initialPins = pageCacheTracer.pins();
            long initialUnpins = pageCacheTracer.unpins();
            long initialHits = pageCacheTracer.hits();

            controller.maintenance();

            assertThat( pageCacheTracer.pins() - initialPins ).isEqualTo( 3 );
            assertThat( pageCacheTracer.unpins() - initialUnpins ).isEqualTo( 3 );
            assertThat( pageCacheTracer.hits() - initialHits ).isEqualTo( 3 );
        }
    }
}
