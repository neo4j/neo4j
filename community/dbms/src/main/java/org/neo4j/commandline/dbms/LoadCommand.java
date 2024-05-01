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
package org.neo4j.commandline.dbms;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static org.neo4j.commandline.dbms.LoadDumpExecutor.BACKUP_EXTENSION;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.archive.Dumper.DUMP_EXTENSION;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.dbms.archive.Loader.SizeMeta;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupFormatSelector;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

@Command(
        name = "load",
        header = "Load a database from an archive created with the dump command or from full Neo4j Enterprise backup.",
        description = "Load a database from an archive. <archive-path> must be a directory containing an archive(s). "
                + "Archive can be a database dump created with the dump command, or can be a full backup artifact "
                + "created by the backup command from Neo4j Enterprise. "
                + "If neither --from-path or --from-stdin is supplied `server.directories.dumps.root` setting will "
                + "be searched for the archive. "
                + "Existing databases can be replaced "
                + "by specifying --overwrite-destination. It is not possible to replace a database that is mounted "
                + "in a running Neo4j server. If --info is specified, then the database is not loaded, but information "
                + "(i.e. file count, byte count, and format of load file) about the archive is printed instead.")
public class LoadCommand extends AbstractAdminCommand {

    @Parameters(
            arity = "1",
            description = "Name of the database to load. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @ArgGroup()
    private SourceOption source = new SourceOption();

    private static class SourceOption {

        @Option(names = "--from-path", paramLabel = "<path>", description = "Path to directory containing archive(s).")
        private String path;

        @Option(names = "--from-stdin", description = "Read archive from standard input.")
        private boolean stdIn;
    }

    @Option(
            names = "--overwrite-destination",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = ALWAYS,
            description = "If an existing database should be replaced.")
    private boolean force;

    @Option(
            names = "--info",
            fallbackValue = "true",
            description =
                    "Print meta-data information about the archive file, instead of loading the contained database.")
    private boolean info;

    static final String SYSTEM_ERR_MESSAGE = "WARNING! You are loading a dump of Neo4j's internal system database.%n"
            + "This system database dump may contain unwanted metadata for the DBMS it was taken from;%n"
            + "Loading it should only be done after consulting the Neo4j Operations Manual.%n";

    public static final String FULL_BACKUP_DESCRIPTION = "Neo4j Full Backup";
    public static final String DIFFERENTIAL_BACKUP_DESCRIPTION = "Neo4j Differential Backup";
    public static final String ZSTD_DUMP_DESCRIPTION = "Neo4j ZSTD Dump.";
    public static final String GZIP_DUMP_DESCRIPTION = "TAR+GZIP.";
    public static final String UNKNOWN_COUNT = "?";

    public LoadCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected Optional<String> commandConfigName() {
        return Optional.of("database-load");
    }

    protected Loader createLoader(FileSystemAbstraction fs) {
        return new Loader(fs, ctx.err());
    }

    @Override
    public void execute() {
        Config config = buildConfig();

        try (var logProvider = Util.configuredLogProvider(ctx.out(), verbose);
                var fs = new SchemeFileSystemAbstraction(ctx.fs(), config, logProvider)) {
            Path sourcePath = null;
            if (source.path != null) {
                sourcePath = fs.resolve(source.path);
                if (!fs.isDirectory(sourcePath)) {
                    throw new CommandFailedException(source.path + " is not an existing directory");
                }
            }

            if (database.containsPattern() && source.stdIn) {
                throw new CommandFailedException(
                        "Globbing in database name can not be used in combination with standard input. "
                                + "Specify a directory as source or a single target database");
            }

            if (sourcePath == null && !source.stdIn) {
                Path defaultDumpsPath = config.get(GraphDatabaseSettings.database_dumps_root_path);
                if (!fs.isDirectory(defaultDumpsPath)) {
                    throw new CommandFailedException("The root location for storing dumps ('"
                            + GraphDatabaseSettings.database_dumps_root_path.name() + "'=" + defaultDumpsPath
                            + ") doesn't contain any dumps yet. Specify another directory with --from-path.");
                }
                sourcePath = fs.resolve(defaultDumpsPath.toString());
            }
            if (info) {
                inspectDump(fs, sourcePath);
            } else {
                loadDump(fs, sourcePath, config);
            }
        } catch (IOException e) {
            Util.wrapIOException(e);
        }
    }

    private void inspectDump(FileSystemAbstraction fs, Path sourcePath) {
        Set<DumpInfo> dbNames = getDbNames(fs, sourcePath, true);
        var loader = createLoader(fs);

        List<FailedLoad> failedLoads = new ArrayList<>();
        for (DumpInfo dumpInfo : dbNames) {
            if (dumpInfo.stdIn) {
                inspectOne(dumpInfo.dbName, ctx::in, loader, failedLoads, "reading from stdin");
            } else {
                for (Path path : dumpInfo.archives) {
                    inspectOne(dumpInfo.dbName, streamSupplierFor(fs, path), loader, failedLoads, path.toString());
                }
            }
        }
        checkFailure(failedLoads, "Print metadata failed for databases: '");
    }

    private void inspectOne(
            String dbName,
            ThrowingSupplier<InputStream, IOException> archiveInputStreamSupplier,
            Loader loader,
            List<FailedLoad> failedLoads,
            String streamDescription) {
        try {
            MutableBoolean backup = new MutableBoolean(false);
            MutableBoolean fullBackup = new MutableBoolean(false);
            Loader.DumpMetaData metaData = loader.getMetaData(
                    archiveInputStreamSupplier,
                    streamSupplier -> DumpFormatSelector.decompressWithBackupSupport(streamSupplier, bd -> {
                        backup.setTrue();
                        fullBackup.setValue(bd.isFull());
                    }));
            String archiveFormat =
                    getArchiveFormat(backup.booleanValue(), fullBackup.booleanValue(), metaData.compressed());
            SizeMeta sizeMeta = metaData.sizeMeta();
            printArchiveInfo(dbName, archiveFormat, sizeMeta);
        } catch (Exception e) {
            ctx.err().printf("Failed to get metadata for archive '%s': %s%n", streamDescription, e.getMessage());
            failedLoads.add(new FailedLoad(dbName, e));
        }
    }

    private void printArchiveInfo(String dbName, String archiveFormat, SizeMeta sizeMeta) {
        ctx.out().println("Database: " + dbName);
        ctx.out().println("Format: " + archiveFormat);
        ctx.out().println("Files: " + (sizeMeta != null ? sizeMeta.files() : UNKNOWN_COUNT));
        ctx.out().println("Bytes: " + (sizeMeta != null ? sizeMeta.bytes() : UNKNOWN_COUNT));
        ctx.out().println();
    }

    private String getArchiveFormat(boolean backup, boolean fullBackup, boolean compressed) {
        if (backup) {
            if (fullBackup) {
                return FULL_BACKUP_DESCRIPTION;
            }
            return DIFFERENTIAL_BACKUP_DESCRIPTION;
        }
        if (compressed) {
            return ZSTD_DUMP_DESCRIPTION;
        }
        return GZIP_DUMP_DESCRIPTION;
    }

    private void loadDump(FileSystemAbstraction fs, Path sourcePath, Config config) throws IOException {
        Set<DumpInfo> dbNames = getDbNames(fs, sourcePath, false);
        loadDump(dbNames, config, fs);
    }

    protected void loadDump(Set<DumpInfo> dbNames, Config config, FileSystemAbstraction fs) throws IOException {
        LoadDumpExecutor loadDumpExecutor =
                new LoadDumpExecutor(config, fs, ctx.err(), createLoader(fs), LoadCommand::decompress);

        List<FailedLoad> failedLoads = new ArrayList<>();
        for (DumpInfo dbName : dbNames) {
            try {
                if (dbName.dbName.equals(SYSTEM_DATABASE_NAME)) {
                    ctx.err().printf(SYSTEM_ERR_MESSAGE);
                }
                Path dumpPath = null;
                if (!dbName.stdIn) {
                    if (dbName.archives.size() > 1) {
                        throw new CommandFailedException("Multiple archives match:\n"
                                + dbName.archives.stream().map(Path::toString).collect(joining("\n"))
                                + "\nRemove ambiguity by leaving only one of the above, or use --from-stdin option and pipe "
                                + "desired archive.");
                    }
                    if (dbName.archives.isEmpty()) {
                        throw new CommandFailedException("No matching archives found");
                    }
                    dumpPath = dbName.archives.get(0);
                    if (!fs.fileExists(dumpPath)) {
                        // fail early as loadDumpExecutor.execute will create directories
                        throw new CommandFailedException("Archive does not exist: " + dumpPath);
                    }
                }
                var dumpInputDescription = dbName.stdIn ? "reading from stdin" : dumpPath.toString();
                ThrowingSupplier<InputStream, IOException> dumpInputStreamSupplier =
                        dbName.stdIn ? ctx::in : streamSupplierFor(fs, dumpPath);
                loadDumpExecutor.execute(
                        new LoadDumpExecutor.DumpInput(dumpInputStreamSupplier, dumpInputDescription),
                        dbName.dbName,
                        force);
            } catch (Exception e) {
                ctx.err().printf("Failed to load database '%s': %s%n", dbName.dbName, e.getMessage());
                failedLoads.add(new FailedLoad(dbName.dbName, e));
            }
        }
        checkFailure(failedLoads, "Load failed for databases: '");
    }

    private static ThrowingSupplier<InputStream, IOException> streamSupplierFor(
            FileSystemAbstraction fs, Path dumpPath) {
        return () -> fs.openAsInputStream(dumpPath);
    }

    private void checkFailure(List<FailedLoad> failedLoads, String prefix) {
        if (!failedLoads.isEmpty()) {
            StringJoiner failedDbs = new StringJoiner("', '", prefix, "'");
            Exception exceptions = null;
            for (FailedLoad failedLoad : failedLoads) {
                failedDbs.add(failedLoad.dbName);
                exceptions = Exceptions.chain(exceptions, failedLoad.e);
            }
            ctx.err().println(failedDbs);
            throw new CommandFailedException(failedDbs.toString(), exceptions);
        }
    }

    record FailedLoad(String dbName, Exception e) {}

    protected record DumpInfo(String dbName, boolean stdIn, List<Path> archives) {
        public DumpInfo(Map.Entry<String, List<Path>> mapEntry) {
            this(mapEntry.getKey(), false, mapEntry.getValue());
        }
    }

    private Set<DumpInfo> getDbNames(FileSystemAbstraction fs, Path sourcePath, boolean includeDiff) {
        if (source.stdIn) {
            return Set.of(new DumpInfo(database.getDatabaseName(), true, emptyList()));
        }
        var dbsToArchives = listArchivesMatching(fs, sourcePath, database, includeDiff);
        if (!database.containsPattern()) {
            var archives = dbsToArchives.getOrDefault(database.getDatabaseName(), emptyList());
            return Set.of(new DumpInfo(database.getDatabaseName(), false, archives));
        }

        var dbNames = dbsToArchives.entrySet().stream().map(DumpInfo::new).collect(Collectors.toSet());
        if (dbNames.isEmpty()) {
            throw new CommandFailedException(
                    "Pattern '" + database.getDatabaseName() + "' did not match any archive file in " + sourcePath);
        }
        return dbNames;
    }

    private Map<String, List<Path>> listArchivesMatching(
            FileSystemAbstraction fs, Path sourcePath, DatabaseNamePattern pattern, boolean includeDiff) {
        try {
            var result = new HashMap<String, List<Path>>();
            for (Path path : fs.listFiles(sourcePath)) {
                String fileName = path.getFileName().toString();
                if (!fs.isDirectory(path)) {
                    if (fileName.endsWith(DUMP_EXTENSION)) {
                        String dbName = fileName.substring(0, fileName.length() - DUMP_EXTENSION.length());
                        if (pattern.matches(dbName)) {
                            result.computeIfAbsent(dbName, name -> new ArrayList<>())
                                    .add(path);
                        }
                    } else if (fileName.endsWith(BACKUP_EXTENSION)) {
                        BackupDescription backupDescription =
                                BackupFormatSelector.readDescription(fs.openAsInputStream(path));
                        String dbName = backupDescription.getDatabaseName();
                        if (pattern.matches(dbName) && (includeDiff || backupDescription.isFull())) {
                            result.computeIfAbsent(dbName, name -> new ArrayList<>())
                                    .add(path);
                        }
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new CommandFailedException("Failed to list archive files", e);
        }
    }

    protected Config buildConfig() {
        return createPrefilledConfigBuilder().build();
    }

    private static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier)
            throws IOException {
        return DumpFormatSelector.decompressWithBackupSupport(streamSupplier, bd -> {
            if (!bd.isFull()) {
                throw new CommandFailedException(
                        "Loading of differential Neo4j backup is not supported. Use restore database instead.");
            }
        });
    }
}
