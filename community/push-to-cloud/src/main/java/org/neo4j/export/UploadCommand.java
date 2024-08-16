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

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.dbms.archive.Loader.SizeMeta;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupFormatSelector;
import org.neo4j.export.aura.AuraClient;
import org.neo4j.export.aura.AuraConsole;
import org.neo4j.export.aura.AuraJsonMapper.SignedURIBodyResponse;
import org.neo4j.export.aura.AuraJsonMapper.UploadStatusResponse;
import org.neo4j.export.aura.AuraURLFactory;
import org.neo4j.export.providers.SignedUpload;
import org.neo4j.export.providers.SignedUploadURLFactory;
import org.neo4j.export.util.IOCommon;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.memory.EmptyMemoryTracker;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "upload",
        description = "Push a local database to a Neo4j Aura instance. "
                + "The target location is a Neo4j Aura Bolt URI. If Neo4j Cloud username and password are not provided "
                + "either as a command option or as an environment variable, they will be requested interactively ")
public class UploadCommand extends AbstractAdminCommand {
    private static final long CRC32_BUFFER_SIZE = ByteUnit.mebiBytes(4);
    private static final String DEV_MODE_VAR_NAME = "P2C_DEV_MODE";
    private static final String ENV_NEO4J_USERNAME = "NEO4J_USERNAME";
    private static final String ENV_NEO4J_PASSWORD = "NEO4J_PASSWORD";
    private static final String TO_PASSWORD = "--to-password";
    private static final String BACKUP_EXTENSION = ".backup";
    private final PushToCloudCLI pushToCloudCLI;
    private final AuraClient.AuraClientBuilder clientBuilder;
    private final AuraURLFactory auraURLFactory;
    private final UploadURLFactory uploadURLFactory;

    @Parameters(
            paramLabel = "<database>",
            description = "Name of the database that should be uploaded. The name is used to select a file "
                    + "which is expected to be named <database>.dump or <database>.backup.",
            converter = Converters.DatabaseNameConverter.class)
    private NormalizedDatabaseName database;

    @Option(
            names = "--from-path",
            paramLabel = "<path>",
            description =
                    "'/path/to/directory-containing-dump-or-backup' Path to a directory containing a database dump or backup file to upload.",
            required = true)
    private Path archiveDirectory;

    @Option(
            names = "--to-uri",
            paramLabel = "<uri>",
            arity = "1",
            required = true,
            description = "'neo4j://mydatabaseid.databases.neo4j.io' Bolt URI of the target database.")
    private String boltURI;

    @Option(
            names = "--to-user",
            defaultValue = "${" + ENV_NEO4J_USERNAME + "}",
            description =
                    "Username of the target database to push this database to. Prompt will ask for a username if not provided. "
                            + "%nDefault:  The value of the " + ENV_NEO4J_USERNAME + " environment variable.")
    private String username;

    @Option(
            names = TO_PASSWORD,
            defaultValue = "${" + ENV_NEO4J_PASSWORD + "}",
            description =
                    "Password of the target database to push this database to. Prompt will ask for a password if not provided. "
                            + "%nDefault:  The value of the " + ENV_NEO4J_PASSWORD + " environment variable.")
    private String password;

    @Option(
            names = "--overwrite-destination",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
            description = "Overwrite the data in the target database.")
    private boolean overwrite;

    @Option(
            names = "--to",
            paramLabel = "<destination>",
            description = "The destination for the upload.",
            defaultValue = "aura",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String to;

    public UploadCommand(
            ExecutionContext ctx,
            AuraClient.AuraClientBuilder clientBuilder,
            AuraURLFactory auraURLFactory,
            UploadURLFactory uploadURLFactory,
            PushToCloudCLI pushToCloudCLI) {
        super(ctx);
        this.clientBuilder = clientBuilder;
        this.pushToCloudCLI = pushToCloudCLI;
        this.auraURLFactory = auraURLFactory;
        this.uploadURLFactory = uploadURLFactory;
    }

    public static long readSizeFromArchiveMetaData(ExecutionContext ctx, Path backup) {
        try {
            final var fileSystem = ctx.fs();

            Loader.DumpMetaData metaData = new Loader(fileSystem, System.out)
                    .getMetaData(
                            () -> fileSystem.openAsInputStream(backup),
                            streamSupplier -> DumpFormatSelector.decompressWithBackupSupport(streamSupplier, bd -> {}));

            SizeMeta sizeMeta = metaData.sizeMeta();
            if (sizeMeta != null) {
                return sizeMeta.bytes();
            }
            return fileSystem.getFileSize(backup);
        } catch (IOException e) {
            throw new CommandFailedException("Unable to check size of archive backup.", e);
        }
    }

    public static String sizeText(long size) {
        return format("%.1f GB", bytesToGibibytes(size));
    }

    public static double bytesToGibibytes(long sizeInBytes) {
        return sizeInBytes / (double) (1024 * 1024 * 1024);
    }

    public long readSizeFromTarMetaData(ExecutionContext ctx, Path tar, String dbName) {
        final var fileSystem = ctx.fs();

        try (TarArchiveInputStream tais = new TarArchiveInputStream(maybeGzipped(tar, fileSystem))) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                if (entry.getName().endsWith(dbName + Dumper.DUMP_EXTENSION)) {

                    Loader.DumpMetaData metaData =
                            new Loader(fileSystem, System.out).getMetaData(() -> tais, DumpFormatSelector::decompress);
                    SizeMeta sizeMeta = metaData.sizeMeta();
                    if (sizeMeta != null) {
                        return sizeMeta.bytes();
                    }
                    return fileSystem.getFileSize(tar);
                }
            }
            throw new CommandFailedException("TAR file " + tar + " does not contain dump for  database " + dbName);
        } catch (IOException e) {
            throw new CommandFailedException("Unable to check size of tar dump database.", e);
        }
    }

    private InputStream maybeGzipped(Path tar, final FileSystemAbstraction fileSystem) throws IOException {
        try {
            return new GZIPInputStream(fileSystem.openAsInputStream(tar));
        } catch (ZipException e) {
            return fileSystem.openAsInputStream(tar);
        }
    }

    @Override
    public void execute() {
        try {
            if (!"aura".equals(to)) {
                throw new CommandFailedException(
                        format("'%s' is not a supported destination. Supported destinations are: 'aura'", to));
            }

            if (isBlank(username)) {
                if (isBlank(username = pushToCloudCLI.readLine("%s", "Neo4j aura username (default: neo4j):"))) {
                    username = "neo4j";
                }
            }
            char[] pass;
            if (isBlank(password)) {
                if ((pass = pushToCloudCLI.readPassword("Neo4j aura password for %s:", username)).length == 0) {
                    throw new CommandFailedException(format(
                            "Please supply a password, either by '%s' parameter, '%s' environment variable, or prompt",
                            TO_PASSWORD, ENV_NEO4J_PASSWORD));
                }
            } else {
                pass = password.toCharArray();
            }

            boolean devMode = pushToCloudCLI.readDevMode(DEV_MODE_VAR_NAME);

            AuraConsole auraConsole = auraURLFactory.buildConsoleURI(boltURI, devMode);

            AuraClient auraClient = clientBuilder
                    .withAuraConsole(auraConsole)
                    .withUserName(username)
                    .withPassword(pass)
                    .withConsent(overwrite)
                    .withBoltURI(boltURI)
                    .withDefaults()
                    .build();

            Uploader uploader = makeDumpUploader(archiveDirectory, database.name());

            uploader.process(auraClient);
        } catch (Exception e) {
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    private void verbose(String format, Object... args) {
        if (verbose) {
            ctx.out().printf(format, args);
        }
    }

    private ArrayList<Path> getBackupFiles(Path archivePath, String database) {
        var result = new ArrayList<Path>();
        try {
            var pattern = new DatabaseNamePattern(database);
            for (Path file : ctx.fs().listFiles(archivePath)) {
                if (file.toString().endsWith(BACKUP_EXTENSION)) {
                    try (var inputStream = ctx.fs().openAsInputStream(file)) {
                        BackupDescription backupDescription = BackupFormatSelector.readDescription(inputStream);
                        String dbName = backupDescription.getDatabaseName();
                        if (pattern.matches(dbName) && backupDescription.isFull()) {
                            result.add(file);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new CommandFailedException(format("Failed to list archive files in %s ", archivePath), e);
        }

        return result;
    }

    public DumpUploader makeDumpUploader(Path archivePath, String database) {
        if (!ctx.fs().isDirectory(archivePath)) {
            throw new CommandFailedException(format("The provided source directory '%s' doesn't exist", archivePath));
        }
        Path dumpFile = archivePath.resolve(database + Dumper.DUMP_EXTENSION);

        if (ctx.fs().fileExists(dumpFile)) {
            ctx.out().println("Detected source dump file at: " + dumpFile);
            return new DumpUploader(new Source(ctx.fs(), dumpFile, archiveSize(dumpFile, database)));
        }

        ArrayList<Path> backupFiles = getBackupFiles(archivePath, database);
        if (backupFiles.size() > 1) {
            String files = backupFiles.stream().map(Path::toString).collect(Collectors.joining(", "));
            throw new CommandFailedException(format(
                    "Found %d backup files for database %s in %s: %s. Expected one.",
                    backupFiles.size(), database, archivePath, files));
        }

        if (!backupFiles.isEmpty() && ctx.fs().fileExists(backupFiles.get(0))) {
            Path backupFile = backupFiles.get(0);
            ctx.out().println("Detected source backup file at: " + backupFile);
            return new DumpUploader(new Source(ctx.fs(), backupFile, archiveSize(backupFile, database)));
        }

        Path tarFile = archivePath.resolve(database + Dumper.TAR_EXTENSION);
        if (!ctx.fs().fileExists(tarFile)) {
            throw new CommandFailedException("Could not find any archive files");
        }
        return new DumpUploader(new Source(ctx.fs(), tarFile, archiveSize(tarFile, database)));
    }

    private long archiveSize(Path archive, String database) {
        long sizeInBytes;
        String fileName = archive.getFileName().toString();
        if (fileName.endsWith(Dumper.DUMP_EXTENSION) || fileName.endsWith(BACKUP_EXTENSION)) {
            sizeInBytes = readSizeFromArchiveMetaData(ctx, archive);
        } else if (fileName.endsWith(Dumper.TAR_EXTENSION)) {
            sizeInBytes = readSizeFromTarMetaData(ctx, archive, database);
        } else {
            throw new CommandFailedException(format(
                    "Detected invalid file format at in file: %s. Expected Format to be one either %s,%s or %s",
                    archive, Dumper.DUMP_EXTENSION, BACKUP_EXTENSION, Dumper.TAR_EXTENSION));
        }
        verbose("Determined DumpSize=%d bytes from archive at %s\n", sizeInBytes, archive);
        return sizeInBytes;
    }

    abstract static class Uploader {
        protected final Source source;

        Uploader(Source source) {
            this.source = source;
        }

        long size() {
            return source.size();
        }

        abstract void process(AuraClient auraClient);
    }

    public record Source(FileSystemAbstraction fs, Path path, long size) {
        long crc32Sum() throws IOException {
            CRC32 crc = new CRC32();
            try (var channel = fs.read(path);
                    var buffer = new NativeScopedBuffer(
                            CRC32_BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
                var byteBuffer = buffer.getBuffer();
                while ((channel.read(byteBuffer)) != -1) {
                    byteBuffer.flip();
                    crc.update(byteBuffer);
                    byteBuffer.clear();
                }
            }
            return crc.getValue();
        }
    }

    class DumpUploader extends Uploader {
        DumpUploader(Source source) {
            super(source);
        }

        @Override
        void process(AuraClient auraClient) {
            // Check size of dump (reading actual database size from dump header)
            String consoleURL = auraClient.getAuraConsole().baseURL();
            verbose("Checking database size %s fits at %s\n", sizeText(size()), consoleURL);
            String bearerToken = auraClient.authenticate(verbose);
            auraClient.checkSize(verbose, size(), bearerToken);

            // Upload dumpFile
            verbose("Uploading data of %s to %s\n", sizeText(size()), consoleURL);

            ctx.out().println("Generating crc32 of archive, this may take some time...");
            long crc32Sum;
            try {
                crc32Sum = source.crc32Sum();

            } catch (IOException e) {
                throw new CommandFailedException("Failed to process archive file", e);
            }

            long archiveSize = IOCommon.getFileSize(source, ctx);
            SignedURIBodyResponse signedURIBodyResponse =
                    auraClient.initatePresignedUpload(crc32Sum, archiveSize, size(), bearerToken);
            SignedUpload signedUpload = uploadURLFactory.fromAuraResponse(signedURIBodyResponse, ctx, boltURI);
            signedUpload.copy(verbose, source);

            try {
                triggerImportForDB(auraClient, bearerToken, crc32Sum, signedURIBodyResponse);
                verbose("Polling status\n");
                auraClient.doStatusPolling(verbose, bearerToken, source.size());
            } catch (IOException e) {
                throw new CommandFailedException(
                        "Failed to trigger import, please contact Aura support at: https://support.neo4j.com", e);
            } catch (InterruptedException e) {
                throw new CommandFailedException("Command interrupted", e);
            }

            ctx.out().println("Dump successfully uploaded to Aura");
            ctx.out().println(String.format("Your archive at %s can now be deleted.", source.path()));
        }

        private void triggerImportForDB(
                AuraClient auraClient, String bearerToken, long crc32Sum, SignedURIBodyResponse signedURIBodyResponse)
                throws IOException {
            if (signedURIBodyResponse.Provider.equalsIgnoreCase(String.valueOf(SignedUploadURLFactory.Provider.AWS))) {
                UploadStatusResponse uploadStatusResponse =
                        auraClient.uploadStatus(verbose, crc32Sum, signedURIBodyResponse.UploadID, bearerToken);
                auraClient.triggerAWSImportProtocol(
                        verbose, source.path(), crc32Sum, bearerToken, uploadStatusResponse);
            } else {
                auraClient.triggerGCPImportProtocol(verbose, source.path(), crc32Sum, bearerToken);
            }
        }
    }
}
