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
package org.neo4j.kernel.api.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.baseSchemaIndexFolder;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexProviderDescriptor;

class IndexDirectoryStructureTest {
    private final IndexProviderDescriptor provider = new IndexProviderDescriptor("test", "0.5");
    private final Path databaseStoreDir = Path.of("db").toAbsolutePath();
    private final Path baseIndexDirectory = baseSchemaIndexFolder(databaseStoreDir);
    private final long indexId = 15;

    @Test
    void shouldSeeCorrectDirectoriesForProvider() {
        assertCorrectDirectories(
                directoriesByProvider(databaseStoreDir).forProvider(provider),
                baseIndexDirectory.resolve(provider.getKey() + "-" + provider.getVersion()),
                baseIndexDirectory
                        .resolve(provider.getKey() + "-" + provider.getVersion())
                        .resolve(String.valueOf(indexId)));
    }

    @Test
    void shouldHandleWeirdCharactersInProviderKey() {
        IndexProviderDescriptor providerWithWeirdName = new IndexProviderDescriptor("some+thing", "1.0");
        assertCorrectDirectories(
                directoriesByProvider(databaseStoreDir).forProvider(providerWithWeirdName),
                baseIndexDirectory.resolve("some_thing-1.0"),
                baseIndexDirectory.resolve("some_thing-1.0").resolve(String.valueOf(indexId)));
    }

    private void assertCorrectDirectories(
            IndexDirectoryStructure directoryStructure, Path expectedRootDirectory, Path expectedIndexDirectory) {
        // when
        Path rootDirectory = directoryStructure.rootDirectory();
        Path indexDirectory = directoryStructure.directoryForIndex(indexId);

        // then
        assertEquals(expectedRootDirectory, rootDirectory);
        assertEquals(expectedIndexDirectory, indexDirectory);
    }
}
