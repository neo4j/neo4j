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
package org.neo4j.io.pagecache.impl.muninn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class StoreFileTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void baseSegmentMustNotBeNull() {
        assertThatThrownBy(() -> new StoreFile(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void exposesBaseSegment() {
        Path base = directory.file("store.db");
        assertThat(new StoreFile(base).baseSegment()).isEqualTo(base);
    }

    @Test
    void storeBaseFileNameIsTheBaseSegmentFileName() {
        StoreFile storeFile = new StoreFile(directory.file("store.db"));
        assertThat(storeFile.storeBaseFileName()).isEqualTo(Path.of("store.db"));
    }

    @Test
    void recordEqualityIsBasedOnBaseSegment() {
        StoreFile one = new StoreFile(directory.file("store.db"));
        StoreFile same = new StoreFile(directory.file("store.db"));
        StoreFile different = new StoreFile(directory.file("other.db"));

        assertThat(one).isEqualTo(same);
        assertThat(one).isNotEqualTo(different);
    }

    @Test
    void segmentPathOfIndexZeroIsTheMainSegment() {
        Path base = directory.file("store.db");
        assertThat(StoreFile.segmentPath(base, 0)).isEqualTo(base);
    }

    @Test
    void segmentPathAppendsIndexAsSiblingSuffix() {
        Path base = directory.file("store.db");
        assertThat(StoreFile.segmentPath(base, 1)).isEqualTo(base.resolveSibling("store.db.1"));
        assertThat(StoreFile.segmentPath(base, 7)).isEqualTo(base.resolveSibling("store.db.7"));
    }

    @Test
    void allSegmentsReturnsBaseWhenBaseDoesNotExist() {
        Path base = directory.file("store.db");
        assertThat(new StoreFile(base).allSegments(fs)).containsExactly(base);
    }

    @Test
    void allSegmentsReturnsConsecutiveSegments() throws IOException {
        Path base = createFile("store.db", 1);
        Path one = createFile("store.db.1", 1);
        Path two = createFile("store.db.2", 1);

        assertThat(new StoreFile(base).allSegments(fs)).containsExactly(base, one, two);
    }

    @Test
    void existsReflectsBaseSegmentPresence() throws IOException {
        Path base = directory.file("store.db");
        StoreFile storeFile = new StoreFile(base);
        assertThat(storeFile.exists(fs)).isFalse();

        createFile("store.db", 1);
        assertThat(storeFile.exists(fs)).isTrue();
    }

    @Test
    void sizeSumsAllSegments() throws IOException {
        Path base = createFile("store.db", 10);
        createFile("store.db.1", 20);
        createFile("store.db.2", 30);

        assertThat(new StoreFile(base).size(fs)).isEqualTo(60);
    }

    @Test
    void sizeOfSingleSegment() throws IOException {
        Path base = createFile("store.db", 42);
        assertThat(new StoreFile(base).size(fs)).isEqualTo(42);
    }

    @Test
    void deleteRemovesEverySegment() throws IOException {
        Path base = createFile("store.db", 1);
        Path one = createFile("store.db.1", 1);
        Path two = createFile("store.db.2", 1);

        new StoreFile(base).delete(fs);

        assertThat(fs.fileExists(base)).isFalse();
        assertThat(fs.fileExists(one)).isFalse();
        assertThat(fs.fileExists(two)).isFalse();
    }

    @Test
    void deleteOfMissingBaseIsNoOp() throws IOException {
        Path base = directory.file("store.db");
        new StoreFile(base).delete(fs);
        assertThat(fs.fileExists(base)).isFalse();
    }

    @Test
    void renameMovesEverySegmentPreservingContentAndIndex() throws IOException {
        Path base = createFile("store.db", 10);
        Path one = createFile("store.db.1", 20);
        Path two = createFile("store.db.2", 30);

        Path destinationBase = directory.file("renamed.db");
        new StoreFile(base).rename(new StoreFile(destinationBase), fs);

        assertThat(fs.fileExists(base)).isFalse();
        assertThat(fs.fileExists(one)).isFalse();
        assertThat(fs.fileExists(two)).isFalse();

        assertThat(fs.getFileSize(destinationBase)).isEqualTo(10);
        assertThat(fs.getFileSize(destinationBase.resolveSibling("renamed.db.1")))
                .isEqualTo(20);
        assertThat(fs.getFileSize(destinationBase.resolveSibling("renamed.db.2")))
                .isEqualTo(30);
    }

    @Test
    void renameOfSingleSegment() throws IOException {
        Path base = createFile("store.db", 5);

        Path destinationBase = directory.file("renamed.db");
        new StoreFile(base).rename(new StoreFile(destinationBase), fs);

        assertThat(fs.fileExists(base)).isFalse();
        assertThat(fs.getFileSize(destinationBase)).isEqualTo(5);
        assertThat(fs.fileExists(destinationBase.resolveSibling("renamed.db.1")))
                .isFalse();
    }

    private Path createFile(String name, int size) throws IOException {
        Path path = directory.file(name);
        try (OutputStream out = fs.openAsOutputStream(path, false)) {
            out.write(new byte[size]);
        }
        return path;
    }
}
