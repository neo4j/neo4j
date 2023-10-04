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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.PageCachedNumberArrayFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLog;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

@PageCacheExtension
class StringCollisionValuesTest {
    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private PageCache pageCache;

    @BeforeEach
    void before() {
        random.withConfiguration(new RandomValues.Default() {
            @Override
            public int stringMaxLength() {
                return (1 << Short.SIZE) - 1;
            }
        });
        random.reset();
    }

    private static Stream<BiFunction<PageCache, Path, NumberArrayFactory>> data() {
        return Stream.of(
                (PageCache pageCache, Path homePath) -> NumberArrayFactories.HEAP,
                (PageCache pageCache, Path homePath) -> NumberArrayFactories.OFF_HEAP,
                (PageCache pageCache, Path homePath) -> NumberArrayFactories.AUTO_WITHOUT_PAGECACHE,
                (PageCache pageCache, Path homePath) -> NumberArrayFactories.CHUNKED_FIXED_SIZE,
                (PageCache pageCache, Path homePath) -> new PageCachedNumberArrayFactory(
                        pageCache,
                        new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                        homePath,
                        NullLog.getInstance(),
                        DEFAULT_DATABASE_NAME));
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldStoreAndLoadStrings(BiFunction<PageCache, Path, NumberArrayFactory> factory) {
        // given
        try (StringCollisionValues values =
                new StringCollisionValues(factory.apply(pageCache, testDirectory.homePath()), 10_000, INSTANCE)) {
            // when
            long[] offsets = new long[100];
            String[] strings = new String[offsets.length];
            for (int i = 0; i < offsets.length; i++) {
                String string = random.nextAlphaNumericString();
                offsets[i] = values.add(string);
                strings[i] = string;
            }

            // then
            for (int i = 0; i < offsets.length; i++) {
                assertEquals(strings[i], values.get(offsets[i]));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldMoveOverToNextChunkOnNearEnd(BiFunction<PageCache, Path, NumberArrayFactory> factory) {
        // given
        try (StringCollisionValues values =
                new StringCollisionValues(factory.apply(pageCache, testDirectory.homePath()), 10_000, INSTANCE)) {
            char[] chars = new char[PAGE_SIZE - 3];
            Arrays.fill(chars, 'a');

            // when
            String string = String.valueOf(chars);
            long offset = values.add(string);
            String secondString = "abcdef";
            long secondOffset = values.add(secondString);

            // then
            String readString = (String) values.get(offset);
            assertEquals(string, readString);
            String readSecondString = (String) values.get(secondOffset);
            assertEquals(secondString, readSecondString);
        }
    }
}
