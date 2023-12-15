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

import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.emptyPrinter;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.printStreamPrinter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.neo4j.commandline.dbms.StoreVersionLoader;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.VisibleForTesting;

public class Loader {
    private final FileSystemAbstraction filesystem;
    private final ArchiveProgressPrinter progressPrinter;

    @VisibleForTesting
    public Loader(FileSystemAbstraction filesystem) {
        this(filesystem, emptyPrinter());
    }

    public Loader(FileSystemAbstraction filesystem, PrintStream output) {
        this(filesystem, output != null ? printStreamPrinter(output) : emptyPrinter());
    }

    public Loader(FileSystemAbstraction filesystem, OutputProgressPrinter progressPrinter) {
        this.filesystem = filesystem;
        this.progressPrinter = new ArchiveProgressPrinter(progressPrinter, Instant::now);
    }

    public Loader(FileSystemAbstraction filesystem, InternalLogProvider logProvider) {
        this(filesystem, ProgressPrinters.logProviderPrinter(logProvider.getLog(Loader.class)));
    }

    public void load(DatabaseLayout databaseLayout, ThrowingSupplier<InputStream, IOException> streamSupplier)
            throws IOException, IncorrectFormat {
        load(databaseLayout, streamSupplier, "");
    }

    public void load(
            DatabaseLayout databaseLayout, ThrowingSupplier<InputStream, IOException> streamSupplier, String inputName)
            throws IOException, IncorrectFormat {
        load(databaseLayout, false, true, DumpFormatSelector::decompress, streamSupplier, inputName);
    }

    public void load(
            Path archive,
            DatabaseLayout databaseLayout,
            boolean validateDatabaseExistence,
            boolean validateLogsExistence,
            DecompressionSelector selector)
            throws IOException, IncorrectFormat {
        load(
                databaseLayout,
                validateDatabaseExistence,
                validateLogsExistence,
                selector,
                () -> filesystem.openAsInputStream(archive),
                archive.toString());
    }

    public void load(
            DatabaseLayout databaseLayout,
            boolean validateDatabaseExistence,
            boolean validateLogsExistence,
            DecompressionSelector selector,
            ThrowingSupplier<InputStream, IOException> streamSupplier,
            String inputName)
            throws IOException, IncorrectFormat {
        Path databaseDestination = databaseLayout.databaseDirectory();
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();

        validatePath(filesystem, databaseDestination, validateDatabaseExistence);
        validatePath(filesystem, transactionLogsDirectory, validateLogsExistence);

        createDestination(filesystem, databaseDestination);
        createDestination(filesystem, transactionLogsDirectory);

        checkDatabasePresence(filesystem, databaseLayout);

        try (ArchiveInputStream stream = openArchiveIn(selector, streamSupplier, inputName);
                Resource ignore = progressPrinter.startPrinting()) {
            ArchiveEntry entry;
            while ((entry = nextEntry(stream, inputName)) != null) {
                Path destination = determineEntryDestination(entry, databaseDestination, transactionLogsDirectory);
                loadEntry(destination, stream, entry);
            }
        }
    }

    public StoreVersionLoader.Result getStoreVersion(
            FileSystemAbstraction fs,
            Config config,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory) {
        try (StoreVersionLoader stl = new StoreVersionLoader(fs, config, contextFactory)) {
            return stl.loadStoreVersionAndCheckDowngrade(databaseLayout);
        }
    }

    public DumpMetaData getMetaData(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        try (InputStream decompressor = DumpFormatSelector.decompress(streamSupplier)) {
            String format = "TAR+GZIP.";
            String files = "?";
            String bytes = "?";
            if (StandardCompressionFormat.ZSTD.isFormat(decompressor)) {
                format = "Neo4j ZSTD Dump.";
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata(decompressor);
                files = String.valueOf(progressPrinter.maxFiles());
                bytes = String.valueOf(progressPrinter.maxBytes());
            }
            return new DumpMetaData(format, files, bytes);
        }
    }

    private static void checkDatabasePresence(FileSystemAbstraction filesystem, DatabaseLayout databaseLayout)
            throws FileAlreadyExistsException {
        if (StorageEngineFactory.selectStorageEngine(filesystem, databaseLayout).isPresent()) {
            throw new FileAlreadyExistsException(
                    "Database already exists at location: " + databaseLayout.databaseDirectory());
        }
    }

    private static void createDestination(FileSystemAbstraction filesystem, Path destination) throws IOException {
        if (!filesystem.fileExists(destination)) {
            filesystem.mkdirs(destination);
        }
    }

    private static void validatePath(FileSystemAbstraction filesystem, Path path, boolean validateExistence)
            throws FileSystemException {
        if (validateExistence && filesystem.fileExists(path)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        checkWritableDirectory(path.getParent());
    }

    private static Path determineEntryDestination(
            ArchiveEntry entry, Path databaseDestination, Path transactionLogsDirectory) {
        Path entryName = Path.of(entry.getName()).getFileName();
        try {
            return TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept(entryName)
                    ? transactionLogsDirectory
                    : databaseDestination;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ArchiveEntry nextEntry(ArchiveInputStream stream, String inputName) throws IncorrectFormat {
        try {
            return stream.getNextEntry();
        } catch (IOException e) {
            throw new IncorrectFormat(inputName, e);
        }
    }

    private void loadEntry(Path destination, ArchiveInputStream stream, ArchiveEntry entry) throws IOException {
        Path file = destination.resolve(entry.getName().replace('\\', '/'));
        var normalizedFile = file.normalize();
        if (!normalizedFile.startsWith(destination)) {
            throw new InvalidDumpEntryException(entry.getName());
        }

        if (entry.isDirectory()) {
            filesystem.mkdirs(normalizedFile);
        } else {
            filesystem.mkdirs(normalizedFile.getParent());
            try (OutputStream output = filesystem.openAsOutputStream(normalizedFile, false)) {
                Utils.copy(stream, output, progressPrinter);
            }
        }
    }

    private ArchiveInputStream openArchiveIn(
            DecompressionSelector selector, ThrowingSupplier<InputStream, IOException> streamSupplier, String inputName)
            throws IOException, IncorrectFormat {
        try {
            InputStream decompressor = selector.decompress(streamSupplier);

            if (StandardCompressionFormat.ZSTD.isFormat(decompressor)) {
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata(decompressor);
            }

            return new TarArchiveInputStream(decompressor);
        } catch (NoSuchFileException ioe) {
            throw ioe;
        } catch (IOException e) {
            throw new IncorrectFormat(inputName, e);
        }
    }

    /**
     * @see Dumper#writeArchiveMetadata(OutputStream)
     */
    void readArchiveMetadata(InputStream stream) throws IOException {
        DataInputStream metadata =
                new DataInputStream(stream); // Unbuffered. Will not play naughty tricks with the file position.
        int version = metadata.readInt();
        if (version == 1) {
            progressPrinter.maxFiles(metadata.readLong());
            progressPrinter.maxBytes(metadata.readLong());
        } else {
            throw new IOException(
                    "Cannot read archive meta-data. I don't recognise this archive version: " + version + ".");
        }
    }

    public record DumpMetaData(String format, String fileCount, String byteCount) {}
}
