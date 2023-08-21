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
package org.neo4j.files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class NativeIndexFileFilterTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    private Path storeDir;
    private NativeIndexFileFilter filter;

    @BeforeEach
    void before() {
        storeDir = directory.homePath();
        filter = new NativeIndexFileFilter(storeDir);
    }

    @Test
    void shouldNotAcceptTextIndex() throws IOException {
        // given
        Path dir = directoriesByProvider(storeDir)
                .forProvider(TextIndexProvider.DESCRIPTOR)
                .directoryForIndex(1);
        shouldNotAcceptFileInDirectory(dir);
    }

    @Test
    void shouldNotAcceptFulltextIndex() throws IOException {
        // given
        Path dir = directoriesByProvider(storeDir)
                .forProvider(FulltextIndexProviderFactory.DESCRIPTOR)
                .directoryForIndex(1);
        shouldNotAcceptFileInDirectory(dir);
    }

    @Test
    void shouldAcceptRangeIndexFile() throws IOException {
        // given
        Path dir = directoriesByProvider(storeDir)
                .forProvider(RangeIndexProvider.DESCRIPTOR)
                .directoryForIndex(1);
        shouldAcceptFileInDirectory(dir);
    }

    private void shouldAcceptFileInDirectory(Path dir) throws IOException {
        Path file = dir.resolve("some-file");
        createFile(file);

        // when
        boolean accepted = filter.test(file);

        // then
        assertTrue(accepted, "Expected to accept file " + file);
    }

    private void shouldNotAcceptFileInDirectory(Path dir) throws IOException {
        Path file = dir.resolve("some-file");
        createFile(file);

        // when
        boolean accepted = filter.test(file);

        // then
        assertFalse(accepted, "Did not expect to accept file " + file);
    }

    private void createFile(Path file) throws IOException {
        fs.mkdirs(file.getParent());
        ((StoreChannel) fs.write(file)).close();
    }
}
