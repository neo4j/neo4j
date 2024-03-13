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
package org.neo4j.csv.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class MagicInputStreamTest {

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @MethodSource("allTheMagic")
    void create(Magic headerMagic, boolean withMarkSupport) throws IOException {
        final var bytes = random.nextBytes(new byte[42 + headerMagic.length()]);
        System.arraycopy(headerMagic.bytes(), 0, bytes, 0, headerMagic.length());

        final var streamCreationCount = new AtomicInteger();
        final var closed = new AtomicBoolean();
        final var fileSystemProvider = mock(FileSystemProvider.class);
        when(fileSystemProvider.newInputStream(any())).thenAnswer(call -> {
            streamCreationCount.incrementAndGet();
            return new ByteArrayInputStream(bytes) {
                @Override
                public boolean markSupported() {
                    return withMarkSupport;
                }

                @Override
                public void close() {
                    closed.set(true);
                }
            };
        });

        final var fileSystem = mock(FileSystem.class);
        when(fileSystem.provider()).thenReturn(fileSystemProvider);

        final var path = mock(Path.class);
        when(path.getFileSystem()).thenReturn(fileSystem);

        try (var stream = MagicInputStream.create(path)) {
            if (withMarkSupport) {
                assertThat(closed.get())
                        .as("input should be reset but NOT closed")
                        .isFalse();
            } else {
                assertThat(closed.get())
                        .as("as input can't be reset, initial input read must be closed")
                        .isTrue();
            }
            assertThat(stream.path()).isSameAs(path);
            assertThat(stream.magic().bytes()).isEqualTo(headerMagic.bytes());
            assertThat(stream.readAllBytes()).isEqualTo(bytes);
            assertThat(streamCreationCount.get()).isEqualTo(withMarkSupport ? 1 : 2);
        }

        if (withMarkSupport) {
            assertThat(closed.get())
                    .as("the delegated input should be now be closed")
                    .isTrue();
        }
    }

    private static Stream<Arguments> allTheMagic() {
        return Stream.of(
                of(Magic.NONE, true),
                of(Magic.GZIP, true),
                of(Magic.ZIP, true),
                of(Magic.BOM_UTF_8, true),
                of(Magic.BOM_UTF_16_BE, true),
                of(Magic.BOM_UTF_16_LE, true),
                of(Magic.BOM_UTF_32_BE, true),
                of(Magic.BOM_UTF_32_LE, true),
                of(Magic.NONE, false),
                of(Magic.GZIP, false),
                of(Magic.ZIP, false),
                of(Magic.BOM_UTF_8, false),
                of(Magic.BOM_UTF_16_BE, false),
                of(Magic.BOM_UTF_16_LE, false),
                of(Magic.BOM_UTF_32_BE, false),
                of(Magic.BOM_UTF_32_LE, false));
    }
}
