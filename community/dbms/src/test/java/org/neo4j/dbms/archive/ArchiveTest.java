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
package org.neo4j.dbms.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.function.Predicates.alwaysFalse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class ArchiveTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction filesystem;

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripAnEmptyDirectory(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");

        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripASingleFile(StandardCompressionFormat compressionFormat) throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        filesystem.mkdirs(directory);
        write(directory.resolve("a-file"), "text");

        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripAnEmptyFile(StandardCompressionFormat compressionFormat) throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        filesystem.mkdirs(directory);
        touch(directory.resolve("a-file"));

        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripFilesWithDifferentContent(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        filesystem.mkdirs(directory);
        write(directory.resolve("a-file"), "text");
        write(directory.resolve("another-file"), "some-different-text");

        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripEmptyDirectories(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        Path subdir = directory.resolve("a-subdirectory");
        filesystem.mkdirs(subdir);
        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldRoundTripFilesInDirectories(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        Path subdir = directory.resolve("a-subdirectory");
        filesystem.mkdirs(subdir);
        write(subdir.resolve("a-file"), "text");
        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldCopeWithLongPaths(StandardCompressionFormat compressionFormat) throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        Path subdir = directory.resolve("a/very/long/path/which/is/not/realistic/for/a/database/today/but/which"
                + "/ensures/that/we/dont/get/caught/out/at/in/the/future/the/point/being/that/there/are/multiple/tar"
                + "/formats/some/of/which/do/not/cope/with/long/paths");
        filesystem.mkdirs(subdir);
        write(subdir.resolve("a-file"), "text");
        assertRoundTrips(directory, compressionFormat);
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldExcludeFilesMatchedByTheExclusionPredicate(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        filesystem.mkdirs(directory);
        touch(directory.resolve("a-file"));
        touch(directory.resolve("another-file"));

        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem);
        dumper.dump(directory, directory, dumper.openForDump(archive), compressionFormat, path -> path.getFileName()
                .toString()
                .equals("another-file"));
        Path txRootDirectory = testDirectory.directory("tx-root_directory");
        DatabaseLayout databaseLayout = layoutWithCustomTxRoot(txRootDirectory, "the-new-directory");
        Loader loader = new Loader(testDirectory.getFileSystem());
        loader.load(databaseLayout, archive);

        Path expectedOutput = testDirectory.directory("expected-output");
        filesystem.mkdirs(expectedOutput);
        touch(expectedOutput.resolve("a-file"));

        assertEquals(describeRecursively(expectedOutput), describeRecursively(databaseLayout.databaseDirectory()));
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void shouldExcludeWholeDirectoriesMatchedByTheExclusionPredicate(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path directory = testDirectory.directory("a-directory");
        Path subdir = directory.resolve("subdir");
        filesystem.mkdirs(subdir);
        touch(subdir.resolve("a-file"));

        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem);
        dumper.dump(directory, directory, dumper.openForDump(archive), compressionFormat, path -> path.getFileName()
                .toString()
                .equals("subdir"));
        Path txLogsRoot = testDirectory.directory("txLogsRoot");
        DatabaseLayout databaseLayout = layoutWithCustomTxRoot(txLogsRoot, "the-new-directory");

        Loader loader = new Loader(testDirectory.getFileSystem());
        loader.load(databaseLayout, archive);

        Path expectedOutput = testDirectory.directory("expected-output");
        filesystem.mkdirs(expectedOutput);

        assertEquals(describeRecursively(expectedOutput), describeRecursively(databaseLayout.databaseDirectory()));
    }

    @ParameterizedTest
    @EnumSource(StandardCompressionFormat.class)
    void dumpAndLoadTransactionLogsFromCustomLocations(StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path txLogsRoot = testDirectory.directory("txLogsRoot");
        DatabaseLayout testDatabaseLayout = layoutWithCustomTxRoot(txLogsRoot, "testDatabase");
        filesystem.mkdirs(testDatabaseLayout.databaseDirectory());
        Path txLogsDirectory = testDatabaseLayout.getTransactionLogsDirectory();
        filesystem.mkdirs(txLogsDirectory);
        touch(testDatabaseLayout.databaseDirectory().resolve("dbfile"));
        touch(txLogsDirectory.resolve(TransactionLogFilesHelper.DEFAULT_NAME + ".0"));

        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem);
        dumper.dump(
                testDatabaseLayout.databaseDirectory(),
                txLogsDirectory,
                dumper.openForDump(archive),
                compressionFormat,
                alwaysFalse());

        Path newTxLogsRoot = testDirectory.directory("newTxLogsRoot");
        DatabaseLayout newDatabaseLayout = layoutWithCustomTxRoot(newTxLogsRoot, "the-new-database");

        Loader loader = new Loader(testDirectory.getFileSystem());
        loader.load(newDatabaseLayout, archive);

        Path expectedOutput = testDirectory.directory("expected-output");
        touch(expectedOutput.resolve("dbfile"));

        Path expectedTxLogs = testDirectory.directory("expectedTxLogs");
        touch(expectedTxLogs.resolve(TransactionLogFilesHelper.DEFAULT_NAME + ".0"));

        assertEquals(describeRecursively(expectedOutput), describeRecursively(newDatabaseLayout.databaseDirectory()));
        assertEquals(
                describeRecursively(expectedTxLogs),
                describeRecursively(newDatabaseLayout.getTransactionLogsDirectory()));
    }

    private void write(Path file, String data) throws IOException {
        try (var outputStream = filesystem.openAsOutputStream(file, false)) {
            outputStream.write(data.getBytes());
        }
    }

    private void touch(Path file) throws IOException {
        try (var write = filesystem.write(file)) {
            write.force(true);
        }
    }

    private DatabaseLayout layoutWithCustomTxRoot(Path txLogsRoot, String databaseName) {
        Config config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, txLogsRoot.toAbsolutePath())
                .set(initial_default_database, databaseName)
                .build();
        return DatabaseLayout.of(config);
    }

    private void assertRoundTrips(Path oldDirectory, StandardCompressionFormat compressionFormat)
            throws IOException, IncorrectFormat {
        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem);
        dumper.dump(oldDirectory, oldDirectory, dumper.openForDump(archive), compressionFormat, alwaysFalse());
        Path newDirectory = testDirectory.file("the-new-directory");
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(newDirectory);
        Loader loader = new Loader(testDirectory.getFileSystem());
        loader.load(databaseLayout, archive);

        assertEquals(describeRecursively(oldDirectory), describeRecursively(newDirectory));
    }

    private Map<Path, Description> describeRecursively(Path directory) throws IOException {
        try (var filetree = filesystem.streamFilesRecursive(directory, true)) {
            return filetree.collect(
                    HashMap::new,
                    (map, handle) -> map.put(handle.getRelativePath(), describe(handle.getPath())),
                    HashMap::putAll);
        }
    }

    private Description describe(Path file) {
        try {
            return filesystem.isDirectory(file) ? new DirectoryDescription() : new FileDescription(readAllBytes(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] readAllBytes(Path file) throws IOException {
        try (var channel = filesystem.read(file)) {
            final var size = channel.size();
            if (size > (long) Integer.MAX_VALUE) {
                throw new OutOfMemoryError("Required array size too large");
            }
            final var data = new byte[(int) size];
            channel.readAll(ByteBuffer.wrap(data));
            return data;
        }
    }

    private interface Description {}

    private static class DirectoryDescription implements Description {
        @Override
        public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass());
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    private static class FileDescription implements Description {
        private final byte[] bytes;

        FileDescription(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileDescription that = (FileDescription) o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
