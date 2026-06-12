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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.batchimport.cache.BufferFactories.fileBacked;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

@TestDirectoryExtension
@RandomSupportExtension
class CollisionValuesTest {
    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void before() {
        random.withConfiguration(RandomValues.newConfigurationBuilder()
                        .stringMaxLength(RandomValues.MAX_BMP_CODE_POINT)
                        .build())
                .reset();
    }

    private static Stream<Arguments> data() {
        List<Arguments> arguments = new ArrayList<>();
        arguments.add(Arguments.of(Named.of("OFF_HEAP", (BiFunction<FileSystemAbstraction, Path, NumberArrayFactory>)
                (fs, homePath) -> NumberArrayFactories.OFF_HEAP)));
        arguments.add(
                Arguments.of(Named.of("AUTO_WITHOUT_SWAP", (BiFunction<FileSystemAbstraction, Path, NumberArrayFactory>)
                        (fs, homePath) -> NumberArrayFactories.AUTO_WITHOUT_SWAP)));
        arguments.add(Arguments.of(
                Named.of("AUTO_WITH_SWAP", (BiFunction<FileSystemAbstraction, Path, NumberArrayFactory>)
                        (fs, homePath) -> NumberArrayFactories.fromBufferFactory(fileBacked(fs, homePath)))));
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldStoreAndLoadStrings(BiFunction<FileSystemAbstraction, Path, NumberArrayFactory> factory) {
        // given
        try (NumberArrayFactory arrayFactory = factory.apply(testDirectory.getFileSystem(), testDirectory.homePath());
                CollisionValues values = new CollisionValues(arrayFactory, 10_000, INSTANCE)) {
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
                assertThat(values.get(offsets[i])).isEqualTo(strings[i]);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldMoveOverToNextChunkOnNearEnd(BiFunction<FileSystemAbstraction, Path, NumberArrayFactory> factory) {
        // given
        try (NumberArrayFactory arrayFactory = factory.apply(testDirectory.getFileSystem(), testDirectory.homePath());
                CollisionValues values = new CollisionValues(arrayFactory, 10_000, INSTANCE)) {
            char[] chars = new char[PAGE_SIZE - 3];
            Arrays.fill(chars, 'a');

            // when
            String string = String.valueOf(chars);
            long offset = values.add(string);
            String secondString = "abcdef";
            long secondOffset = values.add(secondString);

            // then
            String readString = (String) values.get(offset);
            assertThat(readString).isEqualTo(string);
            String readSecondString = (String) values.get(secondOffset);
            assertThat(readSecondString).isEqualTo(secondString);
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldStoreAndLoadLongs(BiFunction<FileSystemAbstraction, Path, NumberArrayFactory> factory) {
        // given
        try (NumberArrayFactory arrayFactory = factory.apply(testDirectory.getFileSystem(), testDirectory.homePath());
                CollisionValues values = new CollisionValues(arrayFactory, 100, INSTANCE)) {
            // when
            long[] offsets = new long[100];
            long[] longs = new long[offsets.length];
            for (int i = 0; i < offsets.length; i++) {
                long value = random.nextLong(Long.MAX_VALUE);
                offsets[i] = values.add(value);
                longs[i] = value;
            }

            // then
            for (int i = 0; i < offsets.length; i++) {
                assertThat((long) values.get(offsets[i])).isEqualTo(longs[i]);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldStoreAndLoadMixedStringsAndLongs(BiFunction<FileSystemAbstraction, Path, NumberArrayFactory> factory) {
        // given
        try (NumberArrayFactory arrayFactory = factory.apply(testDirectory.getFileSystem(), testDirectory.homePath());
                CollisionValues values = new CollisionValues(arrayFactory, 100, INSTANCE)) {
            // when
            long[] offsets = new long[100];
            Object[] data = new Object[offsets.length];
            for (int i = 0; i < offsets.length; i++) {
                data[i] = random.nextBoolean() ? random.nextLong(Long.MAX_VALUE) : random.nextAlphaNumericString();
                offsets[i] = values.add(data[i]);
            }

            // then
            for (int i = 0; i < offsets.length; i++) {
                assertThat(values.get(offsets[i])).isEqualTo(data[i]);
            }
        }
    }
}
