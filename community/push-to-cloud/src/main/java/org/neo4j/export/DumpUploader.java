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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupFormatSelector;
import org.neo4j.export.aura.AuraClient;
import org.neo4j.export.aura.AuraJsonMapper;
import org.neo4j.export.providers.SignedUpload;
import org.neo4j.export.providers.SignedUploadURLFactory;
import org.neo4j.export.util.IOCommon;
import org.neo4j.util.VisibleForTesting;

public class DumpUploader extends Uploader {
    private static final String BACKUP_EXTENSION = ".backup";

    private final ExecutionContext ctx;
    private final boolean verbose;
    private final String boltURI;
    private final UploadURLFactory uploadURLFactory;

    private DumpUploader(
            Source source, ExecutionContext ctx, boolean verbose, String boltURI, UploadURLFactory uploadURLFactory) {
        super(source);
        this.ctx = ctx;
        this.verbose = verbose;
        this.boltURI = boltURI;
        this.uploadURLFactory = uploadURLFactory;
    }

    public static DumpUploader makeDumpUploader(
            Path archivePath,
            String database,
            ExecutionContext ctx,
            boolean verbose,
            String boltURI,
            UploadURLFactory uploadURLFactory) {
        if (!ctx.fs().isDirectory(archivePath)) {
            throw new CommandFailedException(format("The provided source directory '%s' doesn't exist", archivePath));
        }
        Path dumpFile = archivePath.resolve(database + Dumper.DUMP_EXTENSION);

        if (ctx.fs().fileExists(dumpFile)) {
            ctx.out().println("Detected source dump file at: " + dumpFile);
            return new DumpUploader(
                    new Source(ctx.fs(), dumpFile, archiveSize(dumpFile, database, ctx, verbose)),
                    ctx,
                    verbose,
                    boltURI,
                    uploadURLFactory);
        }

        ArrayList<Path> backupFiles = getBackupFiles(archivePath, database, ctx);
        if (backupFiles.size() > 1) {
            String files = backupFiles.stream().map(Path::toString).collect(Collectors.joining(", "));
            throw new CommandFailedException(format(
                    "Found %d backup files for database %s in %s: %s. Expected one.",
                    backupFiles.size(), database, archivePath, files));
        }

        if (!backupFiles.isEmpty() && ctx.fs().fileExists(backupFiles.get(0))) {
            Path backupFile = backupFiles.get(0);
            ctx.out().println("Detected source backup file at: " + backupFile);
            return new DumpUploader(
                    new Source(ctx.fs(), backupFile, archiveSize(backupFile, database, ctx, verbose)),
                    ctx,
                    verbose,
                    boltURI,
                    uploadURLFactory);
        }

        Path tarFile = archivePath.resolve(database + Dumper.TAR_EXTENSION);
        if (!ctx.fs().fileExists(tarFile)) {
            throw new CommandFailedException("Could not find any archive files");
        }
        return new DumpUploader(
                new Source(ctx.fs(), tarFile, archiveSize(tarFile, database, ctx, verbose)),
                ctx,
                verbose,
                boltURI,
                uploadURLFactory);
    }

    @Override
    public void process(AuraClient auraClient) {
        // Check size of dump (reading actual database size from dump header)
        String consoleURL = auraClient.getAuraConsole().baseURL();
        verbose("Checking database size %s fits at %s\n", IOCommon.sizeText(size()), consoleURL);
        String bearerToken = auraClient.authenticate(verbose);
        auraClient.checkSize(verbose, size(), bearerToken);

        // Upload dumpFile
        verbose("Uploading data of %s to %s\n", IOCommon.sizeText(size()), consoleURL);

        ctx.out().println("Generating crc32 of archive, this may take some time...");
        long crc32Sum;
        try {
            crc32Sum = source.crc32Sum();

        } catch (IOException e) {
            throw new CommandFailedException("Failed to process archive file", e);
        }

        long archiveSize = IOCommon.getFileSize(source, ctx);
        AuraJsonMapper.SignedURIBodyResponse signedURIBodyResponse =
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
            AuraClient auraClient,
            String bearerToken,
            long crc32Sum,
            AuraJsonMapper.SignedURIBodyResponse signedURIBodyResponse)
            throws IOException {
        if (signedURIBodyResponse.Provider.equalsIgnoreCase(String.valueOf(SignedUploadURLFactory.Provider.AWS))) {
            AuraJsonMapper.UploadStatusResponse uploadStatusResponse =
                    auraClient.uploadStatus(verbose, crc32Sum, signedURIBodyResponse.UploadID, bearerToken);
            auraClient.triggerAWSImportProtocol(verbose, source.path(), crc32Sum, bearerToken, uploadStatusResponse);
        } else {
            auraClient.triggerGCPImportProtocol(verbose, source.path(), crc32Sum, bearerToken);
        }
    }

    private void verbose(String format, Object... args) {
        if (verbose) {
            ctx.out().printf(format, args);
        }
    }

    private static ArrayList<Path> getBackupFiles(Path archivePath, String database, ExecutionContext ctx) {
        var result = new ArrayList<Path>();
        try {
            var pattern = new DatabaseNamePattern(database);
            for (Path file : ctx.fs().listFiles(archivePath)) {
                if (file.toString().endsWith(BACKUP_EXTENSION)) {
                    BackupDescription backupDescription = BackupFormatSelector.readDescription(ctx.fs(), file);
                    String dbName = backupDescription.getDatabaseName();
                    if (pattern.matches(dbName) && backupDescription.isFull()) {
                        result.add(file);
                    }
                }
            }
        } catch (IOException e) {
            throw new CommandFailedException(format("Failed to list archive files in %s ", archivePath), e);
        }

        return result;
    }

    @VisibleForTesting
    static long archiveSize(Path archive, String database, ExecutionContext ctx, boolean verbose) {
        long sizeInBytes;
        String fileName = archive.getFileName().toString();
        if (fileName.endsWith(Dumper.DUMP_EXTENSION) || fileName.endsWith(BACKUP_EXTENSION)) {
            sizeInBytes = IOCommon.readSizeFromArchiveMetaData(ctx, archive);
        } else if (fileName.endsWith(Dumper.TAR_EXTENSION)) {
            sizeInBytes = IOCommon.readSizeFromTarMetaData(ctx, archive, database);
        } else {
            throw new CommandFailedException(format(
                    "Detected invalid file format at in file: %s. Expected Format to be one either %s,%s or %s",
                    archive, Dumper.DUMP_EXTENSION, BACKUP_EXTENSION, Dumper.TAR_EXTENSION));
        }
        if (verbose) {
            ctx.out().printf("Determined DumpSize=%d bytes from archive at %s\n", sizeInBytes, archive);
        }
        return sizeInBytes;
    }
}
