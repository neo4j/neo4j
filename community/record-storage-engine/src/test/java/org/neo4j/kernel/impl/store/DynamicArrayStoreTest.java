/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

@EphemeralPageCacheExtension
class DynamicArrayStoreTest {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    private final Path storeFile = Path.of("store");
    private final Path idFile = Path.of("idStore");

    private static Stream<Supplier<Object>> data() {
        return Stream.of(
                () -> new String[] {"a"},
                () -> new PointValue[] {Values.pointValue(WGS_84, 0, 1)},
                () -> new LocalDate[] {LocalDate.MIN},
                () -> new LocalTime[] {LocalTime.MIDNIGHT},
                () -> new LocalDateTime[] {LocalDateTime.MIN},
                () -> new OffsetTime[] {OffsetTime.MIN},
                () -> new ZonedDateTime[] {ZonedDateTime.now()},
                () -> new DurationValue[] {DurationValue.MAX_VALUE},
                () -> new double[] {0, 1},
                () -> new float[] {0, 1},
                () -> new byte[] {0, 1},
                () -> new int[] {0, 1});
    }

    @ParameterizedTest
    @MethodSource("data")
    void tracePageCacheAccessOnRecordAllocation(Supplier<Object> dataSupplier) throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        try (var store = dynamicArrayStore()) {
            tracePageCacheAccessOnAllocation(store, contextFactory, dataSupplier.get());
        }
    }

    private static void tracePageCacheAccessOnAllocation(
            DynamicArrayStore store, CursorContextFactory contextFactory, Object array) {
        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnAllocation")) {
            assertZeroCursor(cursorContext);
            prepareDirtyGenerator(store);

            store.allocateRecords(
                    new ArrayList<>(),
                    array,
                    new StandardDynamicRecordAllocator(store.getIdGenerator(), store.getRecordDataSize()),
                    cursorContext,
                    INSTANCE);

            assertThat(cursorContext.getCursorTracer().pins()).isEqualTo(1);
        }
    }

    private static void prepareDirtyGenerator(DynamicArrayStore store) {
        var idGenerator = store.getIdGenerator();
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markDeleted(1L);
        }
        idGenerator.clearCache(NULL_CONTEXT);
    }

    private static void assertZeroCursor(CursorContext cursorContext) {
        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.hits()).isZero();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
    }

    private DynamicArrayStore dynamicArrayStore() throws IOException {
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), PageCacheTracer.NULL, DEFAULT_DATABASE_NAME);
        DynamicArrayStore store = new DynamicArrayStore(
                fs,
                storeFile,
                idFile,
                Config.defaults(),
                RecordIdType.ARRAY_BLOCK,
                idGeneratorFactory,
                pageCache,
                PageCacheTracer.NULL,
                NullLogProvider.getInstance(),
                1,
                defaultFormat(),
                false,
                DEFAULT_DATABASE_NAME,
                immutable.empty());
        store.initialise(new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
        store.start(NULL_CONTEXT);
        return store;
    }
}
