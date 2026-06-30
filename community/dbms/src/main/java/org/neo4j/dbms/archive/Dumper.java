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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static org.neo4j.dbms.archive.LoggingArchiveProgressPrinter.createProgressPrinter;
import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.archive.Manifest.FileRecord;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.SplittingOutputStream;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.util.Preconditions;

public class Dumper {
    public static final String DUMP_EXTENSION = ".dump";
    public static final String TAR_EXTENSION = ".tar";

    private final FileSystemAbstraction fs;
    private final ArchiveProgressPrinter progressPrinter;
    private final boolean deleteAfterCopy;

    public Dumper(FileSystemAbstraction fs) {
        this(fs, ProgressPrinters.emptyPrinter(), false);
    }

    public Dumper(FileSystemAbstraction fs, PrintStream output) {
        this(fs, ProgressPrinters.printStreamPrinter(output), false);
    }

    public Dumper(FileSystemAbstraction fs, InternalLogProvider logProvider) {
        this(fs, ProgressPrinters.logProviderPrinter(logProvider.getLog(Dumper.class)), false);
    }

    public Dumper(FileSystemAbstraction fs, InternalLogProvider logProvider, boolean deleteAfterCopy) {
        this(fs, ProgressPrinters.logProviderPrinter(logProvider.getLog(Dumper.class)), deleteAfterCopy);
    }

    private Dumper(FileSystemAbstraction fs, OutputProgressPrinter progressPrinter, boolean deleteAfterCopy) {
        this(fs, createProgressPrinter(progressPrinter), deleteAfterCopy);
    }

    public Dumper(FileSystemAbstraction fs, ArchiveProgressPrinter progressPrinter) {
        this(fs, progressPrinter, false);
    }

    public Dumper(FileSystemAbstraction fs, ArchiveProgressPrinter progressPrinter, boolean deleteAfterCopy) {
        this.fs = requireNonNull(fs);
        this.progressPrinter = requireNonNull(progressPrinter);
        this.deleteAfterCopy = deleteAfterCopy;
    }

    /**
     * Tells whether a given path is a valid dump (based on file extension)
     * @param dump should be a dump
     * @return <code>true</code> if the provided path is a dump
     */
    public static boolean isDumpFile(Path dump) {
        return dump.toString().endsWith(DUMP_EXTENSION);
    }

    public static Manifest collectManifest(Path dbPath) throws IOException {
        return Manifest.builder().addContentsOf(dbPath).build();
    }

    public static Manifest collectManifest(Path dbPath, Path transactionalLogsPath, Predicate<Path> exclude)
            throws IOException {
        Manifest.Builder mfb = Manifest.builder();
        if (Util.isSameOrChildFile(dbPath, transactionalLogsPath)) {
            mfb.addContentsOf(Predicate.not(exclude), dbPath);
        } else {
            mfb.addContentsOf(Predicate.not(exclude), dbPath);
            mfb.addContentsOf(Predicate.not(exclude), transactionalLogsPath);
        }
        return mfb.build();
    }

    /**
     * @param dot    Dump output type
     * @param format compression format
     * @throws IOException in case of error
     */
    public void dump(DumpOutput dot, DumpFormat format, Manifest mf) throws IOException {
        progressPrinter.reset();
        long numFiles = 0;
        long numBytes = 0;
        for (Manifest.ManifestRecord record : mf.files()) {
            if (record instanceof FileRecord fileRecord) {
                numFiles += 1;
                numBytes += fileRecord.size();
            }
        }
        progressPrinter.maxBytes(numBytes);
        progressPrinter.maxFiles(numFiles);

        try (OutputStream compress = format.compress(dot.stream())) {
            // Add enough archive meta-data that the load command can print a meaningful progress indicator.
            if (StandardCompressionFormat.ZSTD.isFormat(compress)) {
                writeArchiveMetadata(compress, numFiles, numBytes);
            }

            Tarball.Writer progressWriter = (pth, os) -> {
                try (var is = fs.openAsInputStream(pth)) {
                    Utils.copy(is, os, progressPrinter);
                }
                if (deleteAfterCopy) {
                    fs.deleteFile(pth);
                }
            };

            try (Resource ignore = progressPrinter.startPrinting()) {
                Tarball.create(compress, null, progressWriter, mf);
            }
        }
    }

    /**
     * @see Loader#readArchiveSizeMetadata(InputStream)
     */
    void writeArchiveMetadata(OutputStream stream, long numFiles, long numBytes) throws IOException {
        DataOutputStream metadata = new DataOutputStream(stream); // Unbuffered. No need for flushing.
        metadata.writeInt(1); // Archive format version. Increment whenever the metadata format changes.
        metadata.writeLong(numFiles);
        metadata.writeLong(numBytes);
    }

    public interface DumpFormat extends CompressionFormat {}

    public interface DumpOutput {
        OutputStream stream() throws IOException;

        String description();
    }

    public record FileOutput(FileSystemAbstraction fs, Path path) implements DumpOutput {
        public FileOutput {
            Preconditions.checkState(
                    !fs.isDirectory(path), "FileOutput must target a file, not a directory: %s".formatted(path));
        }

        public OutputStream stream() throws IOException {
            // Always create the file to be sure that we are the owner.
            checkWritableDirectory(path.getParent());
            return fs.openAsOutputStream(path, Set.of(WRITE, CREATE_NEW));
        }

        @Override
        public String description() {
            return path.toString();
        }

        public static FileOutput of(FileSystemAbstraction fs, Path path) {
            return new FileOutput(fs, path);
        }
    }

    public record StdoutOutput(ExecutionContext ctx) implements DumpOutput {

        @Override
        public OutputStream stream() throws IOException {
            return CloseShieldOutputStream.wrap(ctx.out());
        }

        @Override
        public String description() {
            return "writing to stdout";
        }
    }

    public record SplitFileOutput(FileSystemAbstraction fs, Path baseArtifact, long maxArtifactSize)
            implements DumpOutput {
        public static final MagicSignature MAGIC_MANIFEST_HEADER =
                MagicSignature.of(ArchiveFormat.SPLIT_FILE_PREFIX + "MV1");
        public static final MagicSignature MAGIC_DATA_HEADER =
                MagicSignature.of(ArchiveFormat.SPLIT_FILE_PREFIX + "DV1");
        public static final int HEADER_SIZE = ArchiveFormat.MAGIC_PREFIX_LENGTH + 4 + 16; // header + index + uuid
        private static final long MIN_SPLIT_ARTIFACT_SIZE = ByteUnit.gibiBytes(1);

        public static long determineSplitArtifactSize(Config config, long overrideArchiveSplitSize) {
            long splitSize = config.get(GraphDatabaseInternalSettings.split_archive_file_size);
            if (overrideArchiveSplitSize > 0) {
                splitSize = overrideArchiveSplitSize;
            }
            if (splitSize > 0
                    && splitSize < MIN_SPLIT_ARTIFACT_SIZE
                    && !config.get(GraphDatabaseInternalSettings.allow_small_split_archive_size)) {
                throw new IllegalArgumentException(
                        "Can't split archive in sizes smaller than " + ByteUnit.bytesToString(MIN_SPLIT_ARTIFACT_SIZE));
            }
            return splitSize;
        }

        public static SplitFileOutput of(FileSystemAbstraction fs, Path baseArtifact, long maxArtifactSize) {
            return new SplitFileOutput(fs, baseArtifact, maxArtifactSize);
        }

        @Override
        public OutputStream stream() throws IOException {
            checkWritableDirectory(baseArtifact.getParent());
            SplitFileGenerator fileGenerator = new SplitFileGenerator(baseArtifact, fs);
            Runnable onClose = () -> {
                try {
                    fileGenerator.writeCountToFirstFile();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            return new SplittingOutputStream(fileGenerator, maxArtifactSize - HEADER_SIZE, onClose);
        }

        @Override
        public String description() {
            return "split archive: " + baseArtifact;
        }

        private static final class SplitFileGenerator implements Iterator<OutputStream> {
            private final FileSystemAbstraction fs;
            private final SequentialFileNameHelper fileNameHelper;
            private final Path baseArtifact;
            private final UUID id;
            private int count = 0;

            SplitFileGenerator(Path basePath, FileSystemAbstraction fs) {
                this.fs = fs;
                this.fileNameHelper = new SequentialFileNameHelper(
                        basePath.getParent(), basePath.getFileName().toString());
                this.baseArtifact = basePath;
                this.id = UUID.randomUUID();
            }

            @Override
            public boolean hasNext() {
                return count < Integer.MAX_VALUE;
            }

            @Override
            public OutputStream next() {
                OutputStream stream = null;
                try {
                    count++;
                    Path p = fileNameHelper.getFileForVersion(count);
                    stream = fs.openAsOutputStream(p, Set.of(WRITE, CREATE_NEW));
                    stream.write(MAGIC_DATA_HEADER.getBytes()); // header to signal split artifact
                    stream.write(intToByteArray(count)); // index of this file in the split artifact, starting from 1
                    stream.write(uuidBytes()); // unique id of the split artifacts
                    return stream;
                } catch (IOException e) {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (IOException ex) {
                            e.addSuppressed(ex);
                        }
                    }
                    throw new UncheckedIOException(e);
                }
            }

            private byte[] uuidBytes() {
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(id.getMostSignificantBits());
                buffer.putLong(id.getLeastSignificantBits());
                return buffer.array();
            }

            private byte[] intToByteArray(int value) {
                return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
            }

            void writeCountToFirstFile() throws IOException {
                // The first file contains header, total artifact count and uuid
                try (var stream = fs.openAsOutputStream(baseArtifact, Set.of(WRITE, CREATE_NEW))) {
                    stream.write(MAGIC_MANIFEST_HEADER.getBytes());
                    stream.write(intToByteArray(count));
                    stream.write(uuidBytes());
                }
            }
        }
    }
}
