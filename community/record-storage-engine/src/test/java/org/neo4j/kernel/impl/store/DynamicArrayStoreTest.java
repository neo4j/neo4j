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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

@EphemeralPageCacheExtension
class DynamicArrayStoreTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    private final Path storeFile = Path.of( "store" );
    private final Path idFile = Path.of( "idStore" );

    private static Stream<Supplier<Object>> data()
    {
        return Stream.of( () -> new String[]{"a"},
                () -> new PointValue[]{Values.pointValue( WGS84, 0, 1 )},
                () -> new LocalDate[]{LocalDate.MIN},
                () -> new LocalTime[]{LocalTime.MIDNIGHT},
                () -> new LocalDateTime[]{LocalDateTime.MIN},
                () -> new OffsetTime[]{OffsetTime.MIN},
                () -> new ZonedDateTime[]{ZonedDateTime.now()},
                () -> new DurationValue[]{DurationValue.MAX_VALUE},
                () -> new double[]{0,1},
                () -> new float[]{0,1},
                () -> new byte[]{0,1},
                () -> new int[]{0,1} );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void tracePageCacheAccessOnRecordAllocation( Supplier<Object> dataSupplier ) throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var store = dynamicArrayStore() )
        {
            tracePageCacheAccessOnAllocation( store, pageCacheTracer, dataSupplier.get() );
        }
    }

    private static void tracePageCacheAccessOnAllocation( DynamicArrayStore store, DefaultPageCacheTracer pageCacheTracer, Object array )
    {
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnAllocation" ) ) )
        {
            assertZeroCursor( cursorContext );
            prepareDirtyGenerator( store );

            store.allocateRecords( new ArrayList<>(), array, cursorContext, INSTANCE );

            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 1 );
        }
    }

    private static void prepareDirtyGenerator( DynamicArrayStore store )
    {
        var idGenerator = store.getIdGenerator();
        try ( var marker = idGenerator.marker( NULL ) )
        {
            marker.markDeleted( 1L );
        }
        idGenerator.clearCache( NULL );
    }

    private static void assertZeroCursor( CursorContext cursorContext )
    {
        var cursorTracer = cursorContext.getCursorTracer();
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
    }

    private DynamicArrayStore dynamicArrayStore() throws IOException
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME );
        DynamicArrayStore store = new DynamicArrayStore( storeFile, idFile, Config.defaults(), RecordIdType.ARRAY_BLOCK, idGeneratorFactory, pageCache,
                NullLogProvider.getInstance(), 1, Standard.LATEST_RECORD_FORMATS, writable(), DEFAULT_DATABASE_NAME, immutable.empty() );
        store.initialise( true, NULL );
        store.start( NULL );
        return store;
    }
}
