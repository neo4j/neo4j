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
package org.neo4j.cloud.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PathBasedOutputStreamTest {
    private static final String CONTENT = "some content";

    @Inject
    private TestDirectory directory;

    private Path output;

    @BeforeEach
    void setup() {
        output = directory.file("output");
    }

    @Test
    void replicate() throws IOException {
        try (var outputStream = new PathBasedOutputStream(output)) {
            outputStream.replicate(path -> Files.writeString(path, CONTENT));
        }

        assertThat(Files.readString(output)).isEqualTo(CONTENT);
    }

    @Test
    void write() throws IOException {
        try (var outputStream = new PathBasedOutputStream(output)) {
            outputStream.write(CONTENT.getBytes(StandardCharsets.UTF_8));
        }

        assertThat(Files.readString(output, StandardCharsets.UTF_8)).isEqualTo(CONTENT);
    }

    @Test
    void cannotReplicateIfAlreadyWritten() throws IOException {
        try (var outputStream = new PathBasedOutputStream(output)) {
            outputStream.write(CONTENT.getBytes(StandardCharsets.UTF_8));

            assertThatThrownBy(() -> outputStream.replicate(path -> Files.writeString(path, "boom")))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void cannotWriteIfAlreadyReplicated() throws IOException {
        try (var outputStream = new PathBasedOutputStream(output)) {
            outputStream.replicate(path -> Files.writeString(path, CONTENT));

            assertThatThrownBy(() -> outputStream.write(CONTENT.getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void lifecycle() throws IOException {
        final var outputStream = new PathBasedOutputStream(output);
        outputStream.close();

        assertThatThrownBy(() -> outputStream.write(CONTENT.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is already closed");
        assertThatThrownBy(() -> outputStream.replicate(path -> Files.writeString(path, CONTENT)))
                .isInstanceOf(IOException.class)
                .hasMessage("Stream is already closed");
    }
}
