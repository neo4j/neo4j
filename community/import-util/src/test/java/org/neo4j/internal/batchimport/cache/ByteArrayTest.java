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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.NO_MONITOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLog;

public class ByteArrayTest extends NumberArrayPageCacheTestSupport {
    private static final byte[] DEFAULT = new byte[50];
    private static final int LENGTH = 1_000;
    private static Fixture fixture;

    private static Stream<Arguments> argumentsProvider() throws IOException {
        fixture = prepareDirectoryAndPageCache(ByteArrayTest.class);
        PageCache pageCache = fixture.pageCache;
        Path dir = fixture.directory;
        var contextFactory = fixture.contextFactory;
        NullLog log = NullLog.getInstance();
        NumberArrayFactory autoWithPageCacheFallback =
                NumberArrayFactories.auto(pageCache, contextFactory, dir, true, NO_MONITOR, log, DEFAULT_DATABASE_NAME);
        NumberArrayFactory pageCacheArrayFactory =
                new PageCachedNumberArrayFactory(pageCache, contextFactory, dir, log, DEFAULT_DATABASE_NAME);
        int chunkSize = LENGTH / ChunkedNumberArrayFactory.MAGIC_CHUNK_COUNT;
        return Stream.of(
                Arguments.of(NumberArrayFactories.HEAP.newByteArray(LENGTH, DEFAULT, INSTANCE)),
                Arguments.of(NumberArrayFactories.HEAP.newDynamicByteArray(chunkSize, DEFAULT, INSTANCE)),
                Arguments.of(NumberArrayFactories.OFF_HEAP.newByteArray(LENGTH, DEFAULT, INSTANCE)),
                Arguments.of(NumberArrayFactories.OFF_HEAP.newDynamicByteArray(chunkSize, DEFAULT, INSTANCE)),
                Arguments.of(NumberArrayFactories.AUTO_WITHOUT_PAGECACHE.newByteArray(LENGTH, DEFAULT, INSTANCE)),
                Arguments.of(
                        NumberArrayFactories.AUTO_WITHOUT_PAGECACHE.newDynamicByteArray(chunkSize, DEFAULT, INSTANCE)),
                Arguments.of(autoWithPageCacheFallback.newByteArray(LENGTH, DEFAULT, INSTANCE)),
                Arguments.of(autoWithPageCacheFallback.newDynamicByteArray(chunkSize, DEFAULT, INSTANCE)),
                Arguments.of(pageCacheArrayFactory.newByteArray(LENGTH, DEFAULT, INSTANCE)),
                Arguments.of(pageCacheArrayFactory.newDynamicByteArray(chunkSize, DEFAULT, INSTANCE)));
    }

    @AfterAll
    public static void closeFixture() throws Exception {
        fixture.close();
    }

    private ByteArray array;

    @AfterEach
    public void after() {
        array.close();
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void shouldSetAndGetBasicTypes(ByteArray array) {
        this.array = array;

        int index = 0;
        byte[] actualBytes = new byte[DEFAULT.length];
        byte[] expectedBytes = new byte[actualBytes.length];
        ThreadLocalRandom.current().nextBytes(actualBytes);

        int len = LENGTH - 1; // subtract one because we access TWO elements.
        for (int i = 0; i < len; i++) {
            try {
                // WHEN
                setSimpleValues(index);
                setArray(index + 1, actualBytes);

                // THEN
                verifySimpleValues(index);
                verifyArray(index + 1, actualBytes, expectedBytes);
            } catch (Throwable throwable) {
                throw new AssertionError("Failure at index " + i, throwable);
            }
        }
    }

    private void setSimpleValues(int index) {
        array.setByte(index, 0, (byte) 123);
        array.setShort(index, 1, (short) 1234);
        array.setInt(index, 5, 12345);
        array.setLong(index, 9, Long.MAX_VALUE - 100);
        array.set3ByteInt(index, 17, 0b10101010_10101010_10101010);
        array.set5ByteLong(index, 20, 0b10101010_10101010_10101010_10101010_10101010L);
        array.set6ByteLong(index, 25, 0b10101010_10101010_10101010_10101010_10101010_10101010L);
    }

    private void verifySimpleValues(int index) {
        assertEquals((byte) 123, array.getByte(index, 0));
        assertEquals((short) 1234, array.getShort(index, 1));
        assertEquals(12345, array.getInt(index, 5));
        assertEquals(Long.MAX_VALUE - 100, array.getLong(index, 9));
        assertEquals(0b10101010_10101010_10101010, array.get3ByteInt(index, 17));
        assertEquals(0b10101010_10101010_10101010_10101010_10101010L, array.get5ByteLong(index, 20));
        assertEquals(0b10101010_10101010_10101010_10101010_10101010_10101010L, array.get6ByteLong(index, 25));
    }

    private void setArray(int index, byte[] bytes) {
        array.set(index, bytes);
    }

    private void verifyArray(int index, byte[] actualBytes, byte[] scratchBuffer) {
        array.get(index, scratchBuffer);
        assertArrayEquals(actualBytes, scratchBuffer);
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void shouldDetectMinusOneFor3ByteInts(ByteArray array) {
        this.array = array;

        // WHEN
        array.set3ByteInt(10, 2, -1);
        array.set3ByteInt(10, 5, -1);

        // THEN
        assertEquals(-1L, array.get3ByteInt(10, 2));
        assertEquals(-1L, array.get3ByteInt(10, 5));
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void shouldDetectMinusOneFor5ByteLongs(ByteArray array) {
        this.array = array;

        // WHEN
        array.set5ByteLong(10, 2, -1);
        array.set5ByteLong(10, 7, -1);

        // THEN
        assertEquals(-1L, array.get5ByteLong(10, 2));
        assertEquals(-1L, array.get5ByteLong(10, 7));
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void shouldDetectMinusOneFor6ByteLongs(ByteArray array) {
        this.array = array;

        // WHEN
        array.set6ByteLong(10, 2, -1);
        array.set6ByteLong(10, 8, -1);

        // THEN
        assertEquals(-1L, array.get6ByteLong(10, 2));
        assertEquals(-1L, array.get6ByteLong(10, 8));
    }

    @ParameterizedTest
    @MethodSource("argumentsProvider")
    public void shouldHandleMultipleCallsToClose(ByteArray array) {
        this.array = array;

        // WHEN
        array.close();

        // THEN should also work
        array.close();
    }
}
