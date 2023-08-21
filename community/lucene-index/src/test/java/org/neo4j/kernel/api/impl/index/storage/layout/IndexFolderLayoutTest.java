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
package org.neo4j.kernel.api.impl.index.storage.layout;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class IndexFolderLayoutTest {
    private final Path indexRoot = Path.of("indexRoot");

    @Test
    void testIndexFolder() {
        IndexFolderLayout indexLayout = createTestIndex();
        Path indexFolder = indexLayout.getIndexFolder();

        assertEquals(indexRoot, indexFolder);
    }

    @Test
    void testIndexPartitionFolder() {
        IndexFolderLayout indexLayout = createTestIndex();

        Path indexFolder = indexLayout.getIndexFolder();
        Path partitionFolder1 = indexLayout.getPartitionFolder(1);
        Path partitionFolder3 = indexLayout.getPartitionFolder(3);

        assertEquals(partitionFolder1.getParent(), partitionFolder3.getParent());
        assertEquals(indexFolder, partitionFolder1.getParent());
        assertEquals("1", partitionFolder1.getFileName().toString());
        assertEquals("3", partitionFolder3.getFileName().toString());
    }

    private IndexFolderLayout createTestIndex() {
        return new IndexFolderLayout(indexRoot);
    }
}
