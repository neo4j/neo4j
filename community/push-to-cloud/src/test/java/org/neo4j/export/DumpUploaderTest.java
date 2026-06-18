/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.export;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.archive.DumpGzipFormatV1;
import org.neo4j.dbms.archive.DumpGzipFormatVLegacy;
import org.neo4j.dbms.archive.DumpZstdFormatV1;
import org.neo4j.dbms.archive.DumpZstdFormatVLegacy;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Dumper.DumpFormat;
import org.neo4j.dbms.archive.Dumper.FileOutput;
import org.neo4j.dbms.archive.Manifest;
import org.neo4j.dbms.archive.StandardCompressionFormat;
import org.neo4j.dbms.archive.Tarball;
import org.neo4j.dbms.archive.backup.BackupCompressionFormat;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupTarFormatV1;
import org.neo4j.dbms.archive.backup.BackupTarFormatV2;
import org.neo4j.dbms.archive.backup.BackupZstdFormatV1;
import org.neo4j.dbms.archive.backup.BackupZstdFormatV2;
import org.neo4j.dbms.archive.backup.BackupZstdFormatV3;
import org.neo4j.function.Predicates;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DumpUploaderTest {

    static Stream<DumpFormat> availableFormats() {
        return Stream.of(
                new DumpGzipFormatVLegacy(),
                new DumpZstdFormatVLegacy(),
                new DumpGzipFormatV1(),
                new DumpZstdFormatV1(),
                new BackupTarFormatV1(),
                new BackupTarFormatV2(),
                new BackupZstdFormatV1(),
                new BackupZstdFormatV2(),
                new BackupZstdFormatV3());
    }

    static final String TAR_ARCHIVE = "GDS-monstrosity.tar";
    static final String DATABASE = "foo";

    static final byte[] CONTENT = "Some awesome content!".getBytes(StandardCharsets.UTF_8);

    // Only the Zstd-streams provides content length from getMetadata().
    static final Map<Class<?>, Boolean> FORMAT_HAS_METADATA_SIZE = Map.of(
            DumpGzipFormatVLegacy.class, false,
            DumpZstdFormatVLegacy.class, true,
            DumpGzipFormatV1.class, false,
            DumpZstdFormatV1.class, true,
            BackupTarFormatV1.class, false,
            BackupTarFormatV2.class, false,
            BackupZstdFormatV1.class, true,
            BackupZstdFormatV2.class, true,
            BackupZstdFormatV3.class, true);

    @Inject
    TestDirectory directory;

    @ParameterizedTest
    @MethodSource("availableFormats")
    void testArchiveSize(DumpFormat format) throws IOException {
        ExecutionContext ctx = context();

        // Transfer the artifact to the test directory
        String artifact = getArtifactName(format);

        Path pth = directory.file(artifact);
        relocateArtifactTo(pth, artifact);

        long actual = DumpUploader.archiveSize(pth, DATABASE, ctx, false);
        long expected = FORMAT_HAS_METADATA_SIZE.get(format.getClass()) ? CONTENT.length : Files.size(pth);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testArchiveSizeForTarArchive() throws IOException {
        ExecutionContext ctx = context();

        // Transfer the artifact to the test directory
        Path pth = directory.file(TAR_ARCHIVE);
        relocateArtifactTo(pth, TAR_ARCHIVE);

        long actual = DumpUploader.archiveSize(pth, DATABASE, ctx, false);
        long expected = CONTENT.length;

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testArchiveSizeThrowsWhenNotBackupOrDump() throws IOException {
        ExecutionContext ctx = context();
        Path pth = directory.file("content");
        relocateArtifactTo(pth, "content");

        assertThatExceptionOfType(CommandFailedException.class)
                .isThrownBy(() -> DumpUploader.archiveSize(pth, "foo", ctx, false))
                .withMessageContainingAll("Detected invalid file format at in file", pth.toString());
    }

    private void relocateArtifactTo(Path pth, String artifact) throws IOException {
        try (var is = DumpUploaderTest.class.getResourceAsStream(artifact)) {
            Files.write(pth, is.readAllBytes());
        }
    }

    private ExecutionContext context() {
        var outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        return new ExecutionContext(
                directory.homePath(),
                directory.homePath(),
                printStream,
                mock(PrintStream.class),
                directory.getFileSystem());
    }

    // #######################################################
    // #### Methods for generation of test artifacts below ###
    // #######################################################
    private static int generateTestArtifacts(boolean force) throws IOException {
        var fs = new DefaultFileSystemAbstraction();
        Path base = fs.createTempDirectory("content");
        int wrote = 0;

        // Force re-generation if CONTENT is changed.
        boolean mismatch = contentMismatch();
        boolean tar_archive_missing = DumpUploaderTest.class.getResource(TAR_ARCHIVE) == null;

        List<DumpFormat> formatsToRegenerate = new ArrayList<>();
        for (var fmt : availableFormats().toList()) {
            String artifactName = getArtifactName(fmt);
            if (DumpUploaderTest.class.getResource(artifactName) == null || mismatch || force) {
                formatsToRegenerate.add(fmt);
            }
        }

        Path content = null;
        if (!formatsToRegenerate.isEmpty() || tar_archive_missing || mismatch || force) {
            content = base.resolve("content");
            Files.write(content, CONTENT);
        }

        for (var fmt : formatsToRegenerate) {
            wrote = 1;
            String formatName = getArtifactName(fmt);
            Path output = base.resolve(formatName);
            System.out.println(generateArtifact(fs, base, fmt, content, output));
        }

        if (tar_archive_missing || mismatch || force) {
            wrote = 1;
            System.out.println(generateGdsArtifact(fs, base, content));
        }

        return wrote;
    }

    private static boolean contentMismatch() throws IOException {
        try (var is = DumpUploaderTest.class.getResourceAsStream("content")) {
            var actual = is.readAllBytes();
            return !Arrays.equals(actual, CONTENT);
        }
    }

    private static Path generateArtifact(
            FileSystemAbstraction fs, Path base, DumpFormat format, Path content, Path output) throws IOException {
        Manifest manifest = Manifest.builder().add(content).build();

        // Dump backup formats
        var description = new BackupDescription(
                "foo", StoreId.UNKNOWN, DatabaseId.SYSTEM_DATABASE_ID, LocalDateTime.MIN, false, false, true, 0, 0);

        Dumper dumper = new Dumper(fs);
        if (format instanceof BackupCompressionFormat backup) {
            backup.setMetadata(description);
        }

        dumper.dump(FileOutput.of(fs, output), format, manifest);
        return output;
    }

    private static Path generateGdsArtifact(FileSystemAbstraction fs, Path base, Path content) throws IOException {
        Path dump = base.resolve(DATABASE + ".dump");
        generateArtifact(fs, base, new DumpZstdFormatV1(), content, dump);

        // Create a
        Tarball.tarball(
                base, TAR_ARCHIVE, StandardCompressionFormat.GZIP, Predicates.alwaysTrue(), null, content, dump);
        return base.resolve(TAR_ARCHIVE);
    }

    private static String getFormatName(DumpFormat fmt) {
        StringBuilder sb = new StringBuilder();
        var chars = fmt.getClass().getSimpleName().toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (Character.isUpperCase(ch)) {
                if (i > 0) {
                    sb.append("-");
                }
                sb.append(Character.toLowerCase(ch));
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static String getArtifactName(DumpFormat fmt) {
        String suffix;
        if (fmt instanceof BackupCompressionFormat) {
            suffix = ".backup";
        } else if (fmt instanceof DumpFormat) {
            suffix = ".dump";
        } else {
            throw new IllegalArgumentException("Unknown fmt: " + fmt);
        }
        return getFormatName(fmt) + suffix;
    }

    public static void main(String[] args) throws IOException {
        boolean force = true;
        System.exit(generateTestArtifacts(force));
    }
}
