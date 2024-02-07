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
package org.neo4j.kernel.impl.util;

import static java.nio.file.Path.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.fs.FileUtils.pathToFileAfterMove;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class FileUtilsTest {
    @Inject
    private TestDirectory testDirectory;

    private Path path;

    @BeforeEach
    void beforeEach() {
        path = testDirectory.directory("path");
    }

    @Test
    void moveFileToDirectory() throws Exception {
        Path file = touchFile("source");
        Path targetDir = directory("dir");

        Path newLocationOfFile = FileUtils.moveFileToDirectory(file, targetDir);
        assertTrue(Files.exists(newLocationOfFile));
        assertFalse(Files.exists(file));
        Path[] files = FileUtils.listPaths(targetDir);
        assertNotNull(files);
        assertEquals(newLocationOfFile, files[0]);
    }

    @Test
    void moveFileToDirectoryCreatesNonExistingDirectory() throws Exception {
        Path file = touchFile("source");
        Path targetDir = path.resolve("nonexisting");

        Path newLocationOfFile = FileUtils.moveFileToDirectory(file, targetDir);
        assertTrue(Files.exists(newLocationOfFile));
        assertFalse(Files.exists(file));
        Path[] files = FileUtils.listPaths(targetDir);
        assertNotNull(files);
        assertEquals(newLocationOfFile, files[0]);
    }

    @Test
    void moveFile() throws Exception {
        Path file = touchFile("source");
        Path targetDir = directory("dir");

        Path newLocationOfFile = targetDir.resolve("new-name");
        FileUtils.moveFile(file, newLocationOfFile);
        assertTrue(Files.exists(newLocationOfFile));
        assertFalse(Files.exists(file));
        Path[] files = FileUtils.listPaths(targetDir);
        assertNotNull(files);
        assertEquals(newLocationOfFile, files[0]);
    }

    @Test
    void deletePathRecursively() throws IOException {
        Path root = testDirectory.directory("a");
        Path child = root.resolve("b");
        Path file = child.resolve("c");

        Files.createDirectories(child);
        Files.createFile(file);

        FileUtils.deleteDirectory(root);

        assertFalse(Files.exists(file));
        assertFalse(Files.exists(child));
    }

    @Test
    void deletePathRecursivelyWithFilter() throws IOException {
        Path root = testDirectory.directory("a");
        Path child = root.resolve("b");
        Path file = child.resolve("c");

        Path toKeepDir = root.resolve("d");
        Path toKeepFile = toKeepDir.resolve("e");

        Files.createDirectories(child);
        Files.createFile(file);
        Files.createDirectories(toKeepDir);
        Files.createFile(toKeepFile);

        FileUtils.deleteDirectory(root, path -> !path.equals(toKeepFile));

        assertFalse(Files.exists(file));
        assertFalse(Files.exists(child));

        assertTrue(Files.exists(toKeepFile));
        assertTrue(Files.exists(toKeepDir));
    }

    @Test
    void deleteNestedPathRecursivelyWithFilter() throws IOException {
        Path root = testDirectory.directory("a");
        Path child = root.resolve("a");
        Path file = child.resolve("aaFile");

        Path toKeepDelete = root.resolve("b");

        Files.createDirectories(child);
        Files.createFile(file);
        Files.createDirectories(toKeepDelete);

        FileUtils.deleteDirectory(root, path -> !path.equals(file));

        assertTrue(Files.exists(file));
        assertTrue(Files.exists(child));

        assertFalse(Files.exists(toKeepDelete));
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromShorter() {
        Path file = Path.of("/a");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/c");

        assertThrows(IllegalArgumentException.class, () -> pathToFileAfterMove(from, to, file));
    }

    // INVALID
    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromSameLength() {
        Path file = Path.of("/a/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/c");

        assertThrows(IllegalArgumentException.class, () -> pathToFileAfterMove(from, to, file));
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromLonger() {
        Path file = Path.of("/a/c/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/c");

        assertThrows(IllegalArgumentException.class, () -> pathToFileAfterMove(from, to, file));
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFromDirIsCompletePathToFile() {
        Path file = Path.of("/a/b/f");
        Path from = Path.of("/a/b/f");
        Path to = Path.of("/a/c");

        assertThrows(IllegalArgumentException.class, () -> pathToFileAfterMove(from, to, file));
    }

    // SIBLING
    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSibling() {
        Path file = Path.of("/a/b/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/c/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSiblingAndFileHasSubDir() {
        Path file = Path.of("/a/b/d/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/c/d/f"));
    }

    // DEEPER
    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSubDir() {
        Path file = Path.of("/a/b/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/b/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/b/c/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSubDirAndFileHasSubDir() {
        Path file = Path.of("/a/b/d/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/b/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/b/c/d/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingOutOfDir() {
        Path file = Path.of("/a/b/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/c/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingOutOfDirAndFileHasSubDir() {
        Path file = Path.of("/a/b/d/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/c");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/c/d/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfNotMovingAtAll() {
        Path file = Path.of("/a/b/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/b");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/b/f"));
    }

    @Test
    void pathToFileAfterMoveMustWorkIfNotMovingAtAllAndFileHasSubDir() {
        Path file = Path.of("/a/b/d/f");
        Path from = Path.of("/a/b");
        Path to = Path.of("/a/b");

        assertThat(pathToFileAfterMove(from, to, file)).isEqualTo(path("/a/b/d/f"));
    }

    @Test
    void nonExistingDirectoryCanBeDeleted() throws IOException {
        Path dir = path.resolve("dir");
        FileUtils.deleteFile(dir);
    }

    @Test
    void emptyDirectoryCanBeDeleted() throws Exception {
        Path dir = directory("dir");
        FileUtils.deleteFile(dir);
    }

    @Test
    void nonEmptyDirectoryCannotBeDeleted() throws Exception {
        Path dir = directory("dir");
        Path file = dir.resolve("file");

        Files.createFile(file);
        assertThrows(DirectoryNotEmptyException.class, () -> FileUtils.deleteFile(dir));
    }

    @Test
    void copySubTree() throws IOException {
        // Setup directory structure
        // dir/
        // dir/file1
        // dir/sub1/
        // dir/sub2/
        // dir/sub2/file2

        Path dir = Files.createTempDirectory("dir");
        Files.writeString(dir.resolve("file1"), "file1", StandardCharsets.UTF_8);
        Files.createDirectory(dir.resolve("sub1"));
        Path sub2 = dir.resolve("sub2");
        Files.createDirectory(sub2);
        Files.writeString(sub2.resolve("file2"), "file2", StandardCharsets.UTF_8);

        // Copy
        FileUtils.copyDirectory(dir, dir.resolve("sub2"));

        // Validate result
        // dir/
        // dir/file1
        // dir/sub1/
        // dir/sub2/
        // dir/sub2/file1
        // dir/sub2/file2
        // dir/sub2/sub1/
        // dir/sub2/sub2/
        // dir/sub2/sub2/file2

        Set<Path> structure = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.forEach(path -> structure.add(dir.relativize(path)));
        }
        assertThat(structure)
                .containsExactly(
                        of(""),
                        of("file1"),
                        of("sub1"),
                        of("sub2"),
                        of("sub2/file1"),
                        of("sub2/file2"),
                        of("sub2/sub1"),
                        of("sub2/sub2"),
                        of("sub2/sub2/file2"));
    }

    @Test
    void copyWithFilter() throws IOException {
        // Setup directory structure
        // source/
        // source/file1
        // source/file2
        // source/file3
        // source/file14

        Path source = Files.createTempDirectory("source");
        Files.writeString(source.resolve("file1"), "file1", StandardCharsets.UTF_8);
        Files.writeString(source.resolve("file2"), "file2", StandardCharsets.UTF_8);
        Files.writeString(source.resolve("file3"), "file3", StandardCharsets.UTF_8);
        Files.writeString(source.resolve("file14"), "file14", StandardCharsets.UTF_8);

        Path target = Files.createTempDirectory("target");

        // Copy
        FileUtils.copyDirectory(
                source, target, path -> path.getFileName().toString().startsWith("file1"));

        // Validate result
        // target/
        // target/file1
        // target/file14

        Set<Path> structure = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(target)) {
            walk.forEach(path -> structure.add(target.relativize(path)));
        }
        assertThat(structure).containsExactly(of(""), of("file1"), of("file14"));
    }

    @Test
    void copyWithFilterInSubTree() throws IOException {
        // Setup directory structure
        // dir/
        // dir/file1
        // dir/sub1/
        // dir/sub2/
        // dir/sub2/file2

        Path dir = Files.createTempDirectory("dir");
        Files.writeString(dir.resolve("file1"), "file1", StandardCharsets.UTF_8);
        Files.createDirectory(dir.resolve("sub1"));
        Path sub2 = dir.resolve("sub2");
        Files.createDirectory(sub2);
        Files.writeString(sub2.resolve("file2"), "file2", StandardCharsets.UTF_8);

        // Copy
        FileUtils.copyDirectory(
                dir,
                dir.resolve("sub2"),
                path -> Files.isDirectory(path) || path.getFileName().toString().startsWith("file1"));

        // Validate result
        // dir/
        // dir/file1
        // dir/sub1/
        // dir/sub2/
        // dir/sub2/file1
        // dir/sub2/sub1/
        // dir/sub2/sub2/

        Set<Path> structure = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.forEach(path -> structure.add(dir.relativize(path)));
        }
        assertThat(structure)
                .containsExactly(
                        of(""),
                        of("file1"),
                        of("sub1"),
                        of("sub2"),
                        of("sub2/file1"),
                        of("sub2/file2"),
                        of("sub2/sub1"),
                        of("sub2/sub2"));
    }

    @ParameterizedTest(name = "target exists: {0}")
    @ValueSource(booleans = {false, true})
    void getCanonicalFileResolvesMultipleSymlinks(boolean targetExists) throws IOException {
        // Assume the following folder structure
        //        /neo4j -> /data/root (s)
        //
        //        /data  (d)
        //        /data/root (d)
        //        /data/root/db   -> /data/repo/my_root_db (s)
        //        /data/repo -> /clusterfs/main (s)
        //        /clusterfs/main/my_root_db (d)
        //        /clusterfs/main/my_root_db/myrealfile (f)
        //
        // When we resolve the path
        //
        //        /neo4j/db/myrealfile
        //
        // we expect
        //
        //       /clusterfs/main/my_root_db/myrealfile
        //
        // regardless if the file exists or not.

        // Assume that /path/to/testdir is root (/).
        Path root = testDirectory.homePath();

        // Create the folder structure
        Files.createDirectories(root.resolve("data/root"));
        Files.createDirectories(root.resolve("clusterfs/main/my_root_db"));

        // Create the symbolic links
        Files.createSymbolicLink(root.resolve("neo4j"), root.resolve("data/root"));
        Files.createSymbolicLink(root.resolve("data/root/db"), root.resolve("data/repo/my_root_db"));
        Files.createSymbolicLink(root.resolve("data/repo"), root.resolve("clusterfs/main"));

        if (targetExists) {
            Files.createFile(root.resolve("clusterfs/main/my_root_db/myrealfile"));
        }

        var expected = Path.of("clusterfs/main/my_root_db/myrealfile");
        var actual = FileUtils.getCanonicalFile(root.resolve("neo4j/db/myrealfile"));

        assertThat(root.relativize(actual)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource(value = "examplePaths")
    void getCanonicalFileResolvedExpected(Path input, Path expected) {
        assertThat(FileUtils.getCanonicalFile(input)).isEqualTo(expected);
    }

    static Stream<Arguments> examplePaths() {
        var cwd = Path.of("").toAbsolutePath();

        return Stream.of(
                Arguments.of(
                        // Absolute path
                        Path.of("/does/not/exist"), Path.of("/does/not/exist")),
                Arguments.of(
                        // Non-normalized absolute path
                        Path.of("/does/not/../maybe/exist"), Path.of("/does/maybe/exist")),
                Arguments.of(
                        // Relative path
                        Path.of("does/not/exist"), cwd.resolve("does/not/exist")),
                Arguments.of(
                        // Non-normalized relative path
                        Path.of("does/not/../maybe/exist"), cwd.resolve("does/maybe/exist")));
    }

    private Path directory(String name) throws IOException {
        Path dir = path.resolve(name);
        Files.createDirectories(dir);
        return dir;
    }

    private Path touchFile(String name) throws IOException {
        Path file = path.resolve(name);
        Files.createFile(file);
        return file;
    }

    private static Path path(String path) {
        return Path.of(path);
    }
}
