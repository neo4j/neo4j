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
package org.neo4j.io.fs;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.fs.FileHandle.HANDLE_DELETE;
import static org.neo4j.io.fs.FileHandle.handleRename;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.function.Predicates;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public abstract class FileSystemAbstractionTest {
    @Inject
    TestDirectory testDirectory;

    private final int recordSize = 9;
    private final int maxPages = 20;
    private final int pageCachePageSize = 32;
    private final int recordsPerFilePage = pageCachePageSize / recordSize;
    private final int recordCount = 25 * maxPages * recordsPerFilePage;
    protected FileSystemAbstraction fsa;
    protected Path path;

    @BeforeEach
    void before() {
        fsa = buildFileSystemAbstraction();
        path = testDirectory.homePath().resolve(UUID.randomUUID().toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        fsa.close();
    }

    protected abstract FileSystemAbstraction buildFileSystemAbstraction();

    @Test
    void shouldCreatePath() throws Exception {
        fsa.mkdirs(path);

        assertTrue(fsa.fileExists(path));
    }

    @Test
    void shouldCreateDeepPath() throws Exception {
        path = path.resolve(UUID.randomUUID() + "/" + UUID.randomUUID());

        fsa.mkdirs(path);

        assertTrue(fsa.fileExists(path));
    }

    @Test
    void shouldCreatePathThatAlreadyExists() throws Exception {
        fsa.mkdirs(path);
        assertTrue(fsa.fileExists(path));

        fsa.mkdirs(path);

        assertTrue(fsa.fileExists(path));
    }

    @Test
    void shouldNotCreatePathThatPointsToFile() throws Exception {
        fsa.mkdirs(path);
        assertTrue(fsa.fileExists(path));
        path = path.resolve("some_file");
        try (StoreChannel channel = fsa.write(path)) {
            assertThat(channel).isNotNull();
            assertThrows(IOException.class, () -> fsa.mkdirs(path));
        }
    }

    @Test
    void moveToDirectoryMustMoveFile() throws Exception {
        Path source = path.resolve("source");
        Path target = path.resolve("target");
        Path file = source.resolve("file");
        Path fileAfterMove = target.resolve("file");
        fsa.mkdirs(source);
        fsa.mkdirs(target);
        fsa.write(file).close();
        assertTrue(fsa.fileExists(file));
        assertFalse(fsa.fileExists(fileAfterMove));
        fsa.moveToDirectory(file, target);
        assertFalse(fsa.fileExists(file));
        assertTrue(fsa.fileExists(fileAfterMove));
    }

    @Test
    void copyToDirectoryCopiesFile() throws IOException {
        Path source = path.resolve("source");
        Path target = path.resolve("target");
        Path file = source.resolve("file");
        Path fileAfterCopy = target.resolve("file");
        fsa.mkdirs(source);
        fsa.mkdirs(target);
        fsa.write(file).close();
        assertTrue(fsa.fileExists(file));
        assertFalse(fsa.fileExists(fileAfterCopy));
        fsa.copyToDirectory(file, target);
        assertTrue(fsa.fileExists(file));
        assertTrue(fsa.fileExists(fileAfterCopy));
    }

    @Test
    void copyToDirectoryReplaceExistingFile() throws Exception {
        Path source = path.resolve("source");
        Path target = path.resolve("target");
        Path file = source.resolve("file");
        Path targetFile = target.resolve("file");
        fsa.mkdirs(source);
        fsa.mkdirs(target);
        fsa.write(file).close();

        writeIntegerIntoFile(targetFile);

        fsa.copyToDirectory(file, target);
        assertTrue(fsa.fileExists(file));
        assertTrue(fsa.fileExists(targetFile));
        assertEquals(0L, fsa.getFileSize(targetFile));
    }

    @Test
    void copyFileShouldFailOnExistingTargetIfNoReplaceCopyOptionSupplied() throws Exception {
        // given
        fsa.mkdirs(path);
        Path source = path.resolve("source");
        Path target = path.resolve("target");
        fsa.write(source).close();
        fsa.write(target).close();

        // then
        assertThrows(
                FileAlreadyExistsException.class,
                () -> fsa.copyFile(source, target, FileSystemAbstraction.EMPTY_COPY_OPTIONS));
    }

    @Test
    void deleteRecursivelyMustDeleteAllFilesInDirectory() throws Exception {
        fsa.mkdirs(path);
        Path a = path.resolve("a");
        fsa.write(a).close();
        Path b = path.resolve("b");
        fsa.write(b).close();
        Path c = path.resolve("c");
        fsa.write(c).close();
        Path d = path.resolve("d");
        fsa.write(d).close();

        fsa.deleteRecursively(path);

        assertFalse(fsa.fileExists(a));
        assertFalse(fsa.fileExists(b));
        assertFalse(fsa.fileExists(c));
        assertFalse(fsa.fileExists(d));
    }

    @Test
    void deleteRecursivelyMustDeleteGivenDirectory() throws Exception {
        fsa.mkdirs(path);
        fsa.deleteRecursively(path);
        assertFalse(fsa.fileExists(path));
    }

    @Test
    void deleteRecursivelyMustDeleteGivenFile() throws Exception {
        fsa.mkdirs(path);
        Path file = path.resolve("file");
        fsa.write(file).close();
        fsa.delete(file);
        assertFalse(fsa.fileExists(file));
    }

    @Test
    void deleteRecursivelyMustDeleteAllSubDirectoriesInDirectory() throws IOException {
        fsa.mkdirs(path);
        Path a = path.resolve("a");
        fsa.mkdirs(a);
        Path aa = a.resolve("a");
        fsa.write(aa).close();
        Path b = path.resolve("b");
        fsa.mkdirs(b);
        Path c = path.resolve("c");
        fsa.write(c).close();
        fsa.deleteRecursively(path);

        assertFalse(fsa.fileExists(a));
        assertFalse(fsa.fileExists(aa));
        assertFalse(fsa.fileExists(b));
        assertFalse(fsa.fileExists(c));
        assertFalse(fsa.fileExists(path));
        assertThrows(NoSuchFileException.class, () -> fsa.listFiles(path));
    }

    @Test
    void deleteRecursivelyMustNotDeleteSiblingDirectories() throws IOException {
        fsa.mkdirs(path);
        Path a = path.resolve("a");
        fsa.mkdirs(a);
        Path b = path.resolve("b");
        fsa.mkdirs(b);
        Path bb = b.resolve("b");
        fsa.write(bb).close();
        Path c = path.resolve("c");
        fsa.write(c).close();
        fsa.deleteRecursively(a);

        assertFalse(fsa.fileExists(a));
        assertTrue(fsa.fileExists(b));
        assertTrue(fsa.fileExists(bb));
        assertTrue(fsa.fileExists(c));
        assertTrue(fsa.fileExists(path));
    }

    @Test
    void fileWatcherCreation() throws IOException {
        try (FileWatcher fileWatcher = fsa.fileWatcher()) {
            assertNotNull(fileWatcher.watch(testDirectory.directory("testDirectory")));
        }
    }

    @Test
    void readAndWriteMustTakeBufferPositionIntoAccount() throws Exception {
        byte[] bytes = new byte[] {1, 2, 3, 4, 5};
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.position(1);

        fsa.mkdirs(path);
        Path file = path.resolve("file");
        try (StoreChannel channel = fsa.write(file)) {
            assertThat(channel.write(buf)).isEqualTo(4);
        }
        try (InputStream stream = fsa.openAsInputStream(file)) {
            assertThat(stream.read()).isEqualTo(2);
            assertThat(stream.read()).isEqualTo(3);
            assertThat(stream.read()).isEqualTo(4);
            assertThat(stream.read()).isEqualTo(5);
            assertThat(stream.read()).isEqualTo(-1);
        }
        Arrays.fill(bytes, (byte) 0);
        buf.position(1);
        try (StoreChannel channel = fsa.write(file)) {
            assertThat(channel.read(buf)).isEqualTo(4);
            buf.clear();
            assertThat(buf.get()).isEqualTo((byte) 0);
            assertThat(buf.get()).isEqualTo((byte) 2);
            assertThat(buf.get()).isEqualTo((byte) 3);
            assertThat(buf.get()).isEqualTo((byte) 4);
            assertThat(buf.get()).isEqualTo((byte) 5);
        }
    }

    @Test
    void streamFilesRecursiveMustBeEmptyForEmptyBaseDirectory() throws Exception {
        Path dir = existingDirectory("dir");
        assertThat(fsa.streamFilesRecursive(dir).count()).isEqualTo(0L);
    }

    @Test
    void streamFilesRecursiveMustListAllFilesInBaseDirectory() throws Exception {
        Path a = existingFile("a");
        Path b = existingFile("b");
        Path c = existingFile("c");
        Stream<FileHandle> stream = fsa.streamFilesRecursive(a.getParent());
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths)
                .contains(
                        a.toAbsolutePath().normalize(),
                        b.toAbsolutePath().normalize(),
                        c.toAbsolutePath().normalize());
    }

    @Test
    void streamFilesRecursiveMustListAllFilesInSubDirectories() throws Exception {
        Path sub1 = existingDirectory("sub1");
        Path sub2 = existingDirectory("sub2");
        Path a = existingFile("a");
        Path b = sub1.resolve("b");
        Path c = sub2.resolve("c");
        ensureExists(b);
        ensureExists(c);

        Stream<FileHandle> stream = fsa.streamFilesRecursive(a.getParent());
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths)
                .contains(
                        a.toAbsolutePath().normalize(),
                        b.toAbsolutePath().normalize(),
                        c.toAbsolutePath().normalize());
    }

    @Test
    void streamFilesRecursiveMustNotListSubDirectories() throws Exception {
        Path sub1 = existingDirectory("sub1");
        Path sub2 = existingDirectory("sub2");
        Path sub2sub1 = sub2.resolve("sub1");
        ensureDirectoryExists(sub2sub1);
        existingDirectory("sub3"); // must not be observed in the stream
        Path a = existingFile("a");
        Path b = sub1.resolve("b");
        Path c = sub2.resolve("c");
        ensureExists(b);
        ensureExists(c);

        Stream<FileHandle> stream = fsa.streamFilesRecursive(a.getParent());
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths)
                .containsOnly(
                        a.toAbsolutePath().normalize(),
                        b.toAbsolutePath().normalize(),
                        c.toAbsolutePath().normalize());
    }

    @Test
    void streamFilesRecursiveIncludingDirectoriesMustListSubDirectories() throws Exception {
        Path sub1 = existingDirectory("sub1");
        Path sub2 = existingDirectory("sub2");
        Path sub2sub1 = sub2.resolve("sub1");
        ensureDirectoryExists(sub2sub1);
        Path sub3 = existingDirectory("sub3");
        Path a = existingFile("a");
        Path b = sub1.resolve("b");
        Path c = sub2.resolve("c");
        ensureExists(b);
        ensureExists(c);

        Stream<FileHandle> stream = fsa.streamFilesRecursive(a.getParent(), true);
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths)
                .containsOnly(
                        path.toAbsolutePath().normalize(),
                        sub1.toAbsolutePath().normalize(),
                        sub2.toAbsolutePath().normalize(),
                        sub2sub1.toAbsolutePath().normalize(),
                        sub3.toAbsolutePath().normalize(),
                        a.toAbsolutePath().normalize(),
                        b.toAbsolutePath().normalize(),
                        c.toAbsolutePath().normalize());
    }

    @Test
    void streamFilesRecursiveFilePathsMustBeCanonical() throws Exception {
        Path sub = existingDirectory("sub");
        Path a = sub.resolve("..").resolve("sub").resolve("a");
        ensureExists(a);

        Stream<FileHandle> stream = fsa.streamFilesRecursive(sub.getParent());
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths).contains(a.toAbsolutePath().normalize()); // file in our sub directory
    }

    @Test
    void streamFilesRecursiveMustBeAbleToGivePathRelativeToBase() throws Exception {
        Path sub = existingDirectory("sub");
        Path a = existingFile("a");
        Path b = sub.resolve("b");
        ensureExists(b);
        Path base = a.getParent();
        Set<Path> set =
                fsa.streamFilesRecursive(base).map(FileHandle::getRelativePath).collect(toSet());
        assertThat(set).as("Files relative to base directory " + base).contains(Path.of("a"), Path.of("sub", "b"));
    }

    @Test
    void streamFilesRecursiveMustListSingleFileGivenAsBase() throws Exception {
        existingDirectory("sub"); // must not be observed
        existingFile("sub/x"); // must not be observed
        Path a = existingFile("a");

        Stream<FileHandle> stream = fsa.streamFilesRecursive(a);
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths).contains(a); // note that we don't go into 'sub'
    }

    @Test
    void streamFilesRecursiveListedSingleFileMustHaveCanonicalPath() throws Exception {
        Path sub = existingDirectory("sub");
        existingFile("sub/x"); // we query specifically for 'a', so this must not be listed
        Path a = existingFile("a");
        Path queryForA = sub.resolve("..").resolve("a");

        Stream<FileHandle> stream = fsa.streamFilesRecursive(queryForA);
        List<Path> filepaths = stream.map(FileHandle::getPath).toList();
        assertThat(filepaths).contains(a.toAbsolutePath().normalize()); // note that we don't go into 'sub'
    }

    @Test
    void streamFilesRecursiveMustReturnEmptyStreamForNonExistingBasePath() throws Exception {
        Path nonExisting = Path.of("nonExisting");
        assertFalse(fsa.streamFilesRecursive(nonExisting).anyMatch(Predicates.alwaysTrue()));
    }

    @Test
    void streamFilesRecursiveMustRenameFiles() throws Exception {
        Path a = existingFile("a");
        Path b = nonExistingFile("b"); // does not yet exist
        Path base = a.getParent();
        fsa.streamFilesRecursive(base).forEach(handleRename(b));
        List<Path> filepaths =
                fsa.streamFilesRecursive(base).map(FileHandle::getPath).toList();
        assertThat(filepaths).contains(b.toAbsolutePath().normalize());
    }

    @Test
    void streamFilesRecursiveMustDeleteFiles() throws Exception {
        Path a = existingFile("a");
        Path b = existingFile("b");
        Path c = existingFile("c");

        Path base = a.getParent();
        fsa.streamFilesRecursive(base).forEach(HANDLE_DELETE);

        assertFalse(fsa.fileExists(a));
        assertFalse(fsa.fileExists(b));
        assertFalse(fsa.fileExists(c));
    }

    @Test
    void streamFilesRecursiveMustThrowWhenDeletingNonExistingFile() throws Exception {
        Path a = existingFile("a");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        fsa.deleteFile(a);
        assertThrows(NoSuchFileException.class, handle::delete);
    }

    @Test
    void streamFilesRecursiveMustThrowWhenTargetFileOfRenameAlreadyExists() throws Exception {
        Path a = existingFile("a");
        Path b = existingFile("b");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        assertThrows(FileAlreadyExistsException.class, () -> handle.rename(b));
    }

    @Test
    void streamFilesRecursiveMustNotThrowWhenTargetFileOfRenameAlreadyExistsAndUsingReplaceExisting() throws Exception {
        Path a = existingFile("a");
        Path b = existingFile("b");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b, StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileRename() throws Exception {
        Path sub = existingDirectory("sub");
        Path x = sub.resolve("x");
        ensureExists(x);
        Path target = nonExistingFile("target");

        fsa.streamFilesRecursive(sub).forEach(handleRename(target));

        assertFalse(fsa.isDirectory(sub));
        assertFalse(fsa.fileExists(sub));
    }

    @Test
    void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByRename() throws Exception {
        Path sub = existingDirectory("sub");
        Path subsub = sub.resolve("subsub");
        ensureDirectoryExists(subsub);
        Path x = subsub.resolve("x");
        ensureExists(x);
        Path target = nonExistingFile("target");

        fsa.streamFilesRecursive(sub).forEach(handleRename(target));

        assertFalse(fsa.isDirectory(subsub));
        assertFalse(fsa.fileExists(subsub));
        assertFalse(fsa.isDirectory(sub));
        assertFalse(fsa.fileExists(sub));
    }

    @Test
    void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByRename() throws Exception {
        Path sub = existingDirectory("sub");
        Path subsub = sub.resolve("subsub");
        Path subsubsub = subsub.resolve("subsubsub");
        ensureDirectoryExists(subsub);
        ensureDirectoryExists(subsubsub);
        Path x = subsubsub.resolve("x");
        ensureExists(x);
        Path target = nonExistingFile("target");

        fsa.streamFilesRecursive(subsub).forEach(handleRename(target));

        assertFalse(fsa.fileExists(subsubsub));
        assertFalse(fsa.isDirectory(subsubsub));
        assertFalse(fsa.fileExists(subsub));
        assertFalse(fsa.isDirectory(subsub));
        assertTrue(fsa.fileExists(sub));
        assertTrue(fsa.isDirectory(sub));
    }

    @Test
    void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileDelete() throws Exception {
        Path sub = existingDirectory("sub");
        Path x = sub.resolve("x");
        ensureExists(x);

        fsa.streamFilesRecursive(sub).forEach(HANDLE_DELETE);

        assertFalse(fsa.isDirectory(sub));
        assertFalse(fsa.fileExists(sub));
    }

    @Test
    void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByDelete() throws Exception {
        Path sub = existingDirectory("sub");
        Path subsub = sub.resolve("subsub");
        ensureDirectoryExists(subsub);
        Path x = subsub.resolve("x");
        ensureExists(x);

        fsa.streamFilesRecursive(sub).forEach(HANDLE_DELETE);

        assertFalse(fsa.isDirectory(subsub));
        assertFalse(fsa.fileExists(subsub));
        assertFalse(fsa.isDirectory(sub));
        assertFalse(fsa.fileExists(sub));
    }

    @Test
    void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByDelete() throws Exception {
        Path sub = existingDirectory("sub");
        Path subsub = sub.resolve("subsub");
        Path subsubsub = subsub.resolve("subsubsub");
        ensureDirectoryExists(subsub);
        ensureDirectoryExists(subsubsub);
        Path x = subsubsub.resolve("x");
        ensureExists(x);

        fsa.streamFilesRecursive(subsub).forEach(HANDLE_DELETE);

        assertFalse(fsa.fileExists(subsubsub));
        assertFalse(fsa.isDirectory(subsubsub));
        assertFalse(fsa.fileExists(subsub));
        assertFalse(fsa.isDirectory(subsub));
        assertTrue(fsa.fileExists(sub));
        assertTrue(fsa.isDirectory(sub));
    }

    @Test
    void streamFilesRecursiveMustCreateMissingPathDirectoriesImpliedByFileRename() throws Exception {
        Path a = existingFile("a");
        Path sub = path.resolve("sub"); // does not exist
        Path target = sub.resolve("b");

        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(target);

        assertTrue(fsa.isDirectory(sub));
        assertTrue(fsa.fileExists(target));
    }

    @Test
    void streamFilesRecursiveMustNotSeeFilesLaterCreatedBaseDirectory() throws Exception {
        Path a = existingFile("a");
        Stream<FileHandle> stream = fsa.streamFilesRecursive(a.getParent());
        Path b = existingFile("b");
        Set<Path> files = stream.map(FileHandle::getPath).collect(toSet());
        assertThat(files).containsExactly(a);
        assertThat(files).doesNotContain(b);
    }

    @Test
    void streamFilesRecursiveMustNotSeeFilesRenamedIntoBaseDirectory() throws Exception {
        Path a = existingFile("a");
        Path sub = existingDirectory("sub");
        Path x = sub.resolve("x");
        ensureExists(x);
        Path target = nonExistingFile("target");
        Set<Path> observedFiles = new HashSet<>();
        fsa.streamFilesRecursive(a.getParent()).forEach(fh -> {
            Path file = fh.getPath();
            observedFiles.add(file);
            if (file.equals(x)) {
                handleRename(target).accept(fh);
            }
        });
        assertThat(observedFiles).contains(a, x);
    }

    @Test
    void streamFilesRecursiveMustNotSeeFilesRenamedIntoSubDirectory() throws Exception {
        Path a = existingFile("a");
        Path sub = existingDirectory("sub");
        Path target = sub.resolve("target");
        Set<Path> observedFiles = new HashSet<>();
        fsa.streamFilesRecursive(a.getParent()).forEach(fh -> {
            Path file = fh.getPath();
            observedFiles.add(file);
            if (file.equals(a)) {
                handleRename(target).accept(fh);
            }
        });
        assertThat(observedFiles).contains(a);
    }

    @Test
    void streamFilesRecursiveRenameMustCanonicaliseSourceFile() throws Exception {
        // Path 'a' should canonicalise from 'a/poke/..' to 'a', which is a file that exists.
        // Thus, this should not throw a NoSuchFileException.
        Path a = existingFile("a").resolve("poke").resolve("..");
        Path b = nonExistingFile("b");

        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b); // must not throw
    }

    @Test
    void streamFilesRecursiveRenameMustCanonicaliseTargetFile() throws Exception {
        // Path 'b' should canonicalise from 'b/poke/..' to 'b', which is a file that doesn't exists.
        // Thus, this should not throw a NoSuchFileException for the 'poke' directory.
        Path a = existingFile("a");
        Path b = path.resolve("b").resolve("poke").resolve("..");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b);
    }

    @Test
    void streamFilesRecursiveRenameTargetFileMustBeRenamed() throws Exception {
        Path a = existingFile("a");
        Path b = nonExistingFile("b");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b);
        assertTrue(fsa.fileExists(b));
    }

    @Test
    void streamFilesRecursiveSourceFileMustNotBeMappableAfterRename() throws Exception {
        Path a = existingFile("a");
        Path b = nonExistingFile("b");
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b);
        assertFalse(fsa.fileExists(a));
    }

    @Test
    void streamFilesRecursiveRenameMustNotChangeSourceFileContents() throws Exception {
        Path a = existingFile("a");
        Path b = nonExistingFile("b");
        generateFileWithRecords(a, recordCount);
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b);
        verifyRecordsInFile(b, recordCount);
    }

    @Test
    void streamFilesRecursiveRenameMustNotChangeSourceFileContentsWithReplaceExisting() throws Exception {
        Path a = existingFile("a");
        Path b = existingFile("b");
        generateFileWithRecords(a, recordCount);
        generateFileWithRecords(b, recordCount + recordsPerFilePage);

        // Fill 'b' with random data
        try (StoreChannel channel = fsa.write(b)) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int fileSize = (int) channel.size();
            ByteBuffer buffer = ByteBuffers.allocate(fileSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            for (int i = 0; i < fileSize; i++) {

                buffer.put(i, (byte) rng.nextInt());
            }
            buffer.rewind();
            channel.writeAll(buffer);
        }

        // Do the rename
        FileHandle handle = fsa.streamFilesRecursive(a).findAny().orElseThrow();
        handle.rename(b, REPLACE_EXISTING);

        // Then verify that the old random data we put in 'b' has been replaced with the contents of 'a'
        verifyRecordsInFile(b, recordCount);
    }

    @Test
    void shouldHandlePathThatLooksVeryDifferentWhenCanonicalized() throws Exception {
        Path dir = existingDirectory("./././home/.././././home/././.././././././././././././././././././home/././");
        Path a = existingFile("./home/a");

        List<Path> filepaths =
                fsa.streamFilesRecursive(dir).map(FileHandle::getRelativePath).toList();
        assertThat(filepaths).contains(a.getFileName());
    }

    @Test
    void truncationMustReduceFileSize() throws Exception {
        Path a = existingFile("a");
        try (StoreChannel channel = fsa.write(a)) {
            channel.position(0);
            byte[] data = {
                1, 2, 3, 4,
                5, 6, 7, 8
            };
            channel.writeAll(ByteBuffer.wrap(data));
            channel.truncate(4);
            assertThat(channel.size()).isEqualTo(4);
            ByteBuffer buf = ByteBuffers.allocate(data.length, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            channel.position(0);
            int read = channel.read(buf);
            assertThat(read).isEqualTo(4);
            buf.flip();
            assertThat(buf.remaining()).isEqualTo(4);
            assertThat(buf.array()).containsExactly(1, 2, 3, 4, 0, 0, 0, 0);
        }
    }

    @Test
    void deleteNonExistingFiles() {
        assertDoesNotThrow(() -> fsa.deleteRecursively(path.resolve("a")));
        assertDoesNotThrow(() -> fsa.deleteFile(path.resolve("b")));
        assertDoesNotThrow(() -> fsa.delete(path.resolve("c")));
    }

    private void generateFileWithRecords(Path file, int recordCount) throws IOException {
        try (StoreChannel channel = fsa.write(file)) {
            ByteBuffer buf = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            for (int i = 0; i < recordCount; i++) {
                generateRecordForId(i, buf);
                int rem = buf.remaining();
                do {
                    rem -= channel.write(buf);
                } while (rem > 0);
            }
        }
    }

    private void verifyRecordsInFile(Path file, int recordCount) throws IOException {
        try (StoreChannel channel = fsa.write(file)) {
            ByteBuffer buf = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            ByteBuffer observation = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            for (int i = 0; i < recordCount; i++) {
                generateRecordForId(i, buf);
                observation.position(0);
                channel.read(observation);
                assertRecord(i, observation, buf);
            }
        }
    }

    private static void assertRecord(long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents) {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
        int estimatedPageId = estimateId(actualBytes);
        assertThat(actualBytes)
                .as("Page id: " + pageId + " " + "(based on record data, it should have been " + estimatedPageId
                        + ", a difference of " + Math.abs(pageId - estimatedPageId) + ")")
                .containsExactly(expectedBytes);
    }

    private static int estimateId(byte[] record) {
        return ByteBuffer.wrap(record).getInt() - 1;
    }

    private static void generateRecordForId(long id, ByteBuffer buf) {
        buf.position(0);
        int x = (int) (id + 1);
        buf.putInt(x);
        while (buf.position() < buf.limit()) {
            x++;
            buf.put((byte) (x & 0xFF));
        }
        buf.position(0);
    }

    private Path existingFile(String fileName) throws IOException {
        Path file = path.resolve(fileName);
        fsa.mkdirs(path);
        fsa.write(file).close();
        return file;
    }

    private Path nonExistingFile(String fileName) {
        return path.resolve(fileName);
    }

    private Path existingDirectory(String dir) throws IOException {
        Path directory = path.resolve(dir);
        fsa.mkdirs(directory);
        return directory;
    }

    private void ensureExists(Path file) throws IOException {
        fsa.mkdirs(file.getParent());
        fsa.write(file).close();
    }

    private void ensureDirectoryExists(Path directory) throws IOException {
        fsa.mkdirs(directory);
    }

    private void writeIntegerIntoFile(Path targetFile) throws IOException {
        StoreChannel storeChannel = fsa.write(targetFile);
        ByteBuffer byteBuffer = ByteBuffers.allocate(Integer.SIZE, ByteOrder.LITTLE_ENDIAN, INSTANCE)
                .putInt(7);
        byteBuffer.flip();
        storeChannel.writeAll(byteBuffer);
        storeChannel.close();
    }
}
