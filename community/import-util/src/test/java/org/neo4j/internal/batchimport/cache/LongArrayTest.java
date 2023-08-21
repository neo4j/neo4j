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
package org.neo4j.internal.batchimport.cache;

import static java.lang.System.currentTimeMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.stream;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLog;

class LongArrayTest extends NumberArrayPageCacheTestSupport {
    private final long seed = currentTimeMillis();
    private final Random random = new Random(seed);
    private static Fixture fixture;

    @BeforeAll
    static void setUp() throws IOException {
        fixture = prepareDirectoryAndPageCache(LongArrayTest.class);
    }

    @AfterAll
    static void closeFixture() throws Exception {
        fixture.close();
    }

    @TestFactory
    Stream<DynamicTest> shouldHandleSomeRandomSetAndGet() {
        ThrowingConsumer<NumberArrayFactory> arrayFactoryConsumer = factory -> {
            int length = random.nextInt(100_000) + 100;
            long defaultValue = random.nextInt(2) - 1; // 0 or -1
            try (LongArray array = factory.newLongArray(length, defaultValue, INSTANCE)) {
                long[] expected = new long[length];
                Arrays.fill(expected, defaultValue);

                // WHEN
                int operations = random.nextInt(1_000) + 10;
                for (int i = 0; i < operations; i++) {
                    // THEN
                    int index = random.nextInt(length);
                    long value = random.nextLong();
                    switch (random.nextInt(3)) {
                        case 0: // set
                            array.set(index, value);
                            expected[index] = value;
                            break;
                        case 1: // get
                            assertEquals(expected[index], array.get(index), "Seed:" + seed);
                            break;
                        default: // swap
                            int toIndex = random.nextInt(length);
                            array.swap(index, toIndex);
                            swap(expected, index, toIndex);
                            break;
                    }
                }
            }
        };
        return stream(arrayFactories(), getNumberArrayFactoryName(), arrayFactoryConsumer);
    }

    @TestFactory
    Stream<DynamicTest> shouldHandleMultipleCallsToClose() {
        return stream(arrayFactories(), getNumberArrayFactoryName(), numberArrayFactory -> {
            LongArray array = numberArrayFactory.newLongArray(10, -1, INSTANCE);

            // WHEN
            array.close();

            // THEN should also work
            array.close();
        });
    }

    private static Iterator<NumberArrayFactory> arrayFactories() {
        PageCache pageCache = fixture.pageCache;
        Path dir = fixture.directory;
        var contextFactory = fixture.contextFactory;
        NullLog log = NullLog.getInstance();
        NumberArrayFactory autoWithPageCacheFallback = NumberArrayFactories.auto(
                pageCache, contextFactory, dir, true, NumberArrayFactories.NO_MONITOR, log, DEFAULT_DATABASE_NAME);
        NumberArrayFactory pageCacheArrayFactory =
                new PageCachedNumberArrayFactory(pageCache, contextFactory, dir, log, DEFAULT_DATABASE_NAME);
        return Iterators.iterator(
                NumberArrayFactories.HEAP,
                NumberArrayFactories.OFF_HEAP,
                autoWithPageCacheFallback,
                pageCacheArrayFactory);
    }

    private static Function<NumberArrayFactory, String> getNumberArrayFactoryName() {
        return factory -> factory.getClass().getName();
    }

    private static void swap(long[] expected, int fromIndex, int toIndex) {
        long fromValue = expected[fromIndex];
        expected[fromIndex] = expected[toIndex];
        expected[toIndex] = fromValue;
    }
}
