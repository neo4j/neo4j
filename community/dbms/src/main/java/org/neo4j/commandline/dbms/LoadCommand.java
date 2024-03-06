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

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.archive.Dumper.DUMP_EXTENSION;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

@Command(
        name = "load",
        header = "Load a database from an archive created with the dump command.",
        description = "Load a database from an archive. <archive-path> must be a directory containing an archive(s) "
                + "created with the dump command. "
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
        @Option(
                names = "--from-path",
                paramLabel = "<path>",
                description = "Path to directory containing archive(s) created with the dump command.")
        private String path;

        @Option(names = "--from-stdin", description = "Read dump from standard input.")
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

    static final String SYSTEM_ERR_MESSAGE =
            String.format("WARNING! You are loading a dump of Neo4j's internal system database.%n"
                    + "This system database dump may contain unwanted metadata for the DBMS it was taken from;%n"
                    + "Loading it should only be done after consulting the Neo4j Operations Manual.%n");

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
            final var loader = createLoader(fs);

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
                if (!ctx.fs().isDirectory(defaultDumpsPath)) {
                    throw new CommandFailedException("The root location for storing dumps ('"
                            + GraphDatabaseSettings.database_dumps_root_path.name() + "'=" + defaultDumpsPath
                            + ") doesn't contain any dumps yet. Specify another directory with --from-path.");
                }
                sourcePath = fs.resolve(defaultDumpsPath.toString());
            }

            Set<DumpInfo> dbNames = getDbNames(fs, sourcePath);

            if (info) {
                inspectDump(loader, dbNames);
            } else {
                loadDump(loader, dbNames, config);
            }
        } catch (IOException e) {
            Util.wrapIOException(e);
        }
    }

    private void inspectDump(Loader loader, Set<DumpInfo> dbNames) {
        List<FailedLoad> failedLoads = new ArrayList<>();

        for (DumpInfo dbName : dbNames) {
            try {
                Loader.DumpMetaData metaData = loader.getMetaData(getArchiveInputStreamSupplier(dbName.dumpPath));
                ctx.out().println("Database: " + dbName.dbName);
                ctx.out().println("Format: " + metaData.format());
                ctx.out().println("Files: " + metaData.fileCount());
                ctx.out().println("Bytes: " + metaData.byteCount());
                ctx.out().println();
            } catch (Exception e) {
                ctx.err().printf("Failed to get metadata for dump '%s': %s%n", dbName.dumpPath, e.getMessage());
                failedLoads.add(new FailedLoad(dbName.dbName, e));
            }
        }

        checkFailure(failedLoads, "Print metadata failed for databases: '");
    }

    private ThrowingSupplier<InputStream, IOException> getArchiveInputStreamSupplier(Path path) {
        if (path != null) {
            return () -> Files.newInputStream(path);
        }
        return ctx::in;
    }

    protected void loadDump(Loader loader, Set<DumpInfo> dbNames, Config config) throws IOException {
        final var fs = ctx.fs();
        LoadDumpExecutor loadDumpExecutor = new LoadDumpExecutor(config, fs, ctx.err(), loader);

        List<FailedLoad> failedLoads = new ArrayList<>();
        for (DumpInfo dbName : dbNames) {
            try {
                if (dbName.dbName.equals(SYSTEM_DATABASE_NAME)) {
                    ctx.err().print(SYSTEM_ERR_MESSAGE);
                }

                var dumpInputDescription = dbName.dumpPath == null ? "reading from stdin" : dbName.dumpPath.toString();
                var dumpInputStreamSupplier = getArchiveInputStreamSupplier(dbName.dumpPath);

                if (dbName.dumpPath != null && !fs.fileExists(dbName.dumpPath)) {
                    // fail early as loadDumpExecutor.execute will create directories
                    throw new CommandFailedException("Archive does not exist: " + dbName.dumpPath);
                }

                loadDumpExecutor.execute(
                        new LoadDumpExecutor.DumpInput(
                                dumpInputStreamSupplier, Optional.ofNullable(dbName.dumpPath), dumpInputDescription),
                        dbName.dbName,
                        force);
            } catch (Exception e) {
                ctx.err().printf("Failed to load database '%s': %s%n", dbName.dbName, e.getMessage());
                failedLoads.add(new FailedLoad(dbName.dbName, e));
            }
        }
        checkFailure(failedLoads, "Load failed for databases: '");
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

    protected record DumpInfo(String dbName, Path dumpPath) {}

    private Set<DumpInfo> getDbNames(FileSystemAbstraction fs, Path sourcePath) {
        if (source.stdIn) {
            return Set.of(new DumpInfo(database.getDatabaseName(), null));
        }
        if (!database.containsPattern()) {
            return Set.of(new DumpInfo(
                    database.getDatabaseName(), sourcePath.resolve(database.getDatabaseName() + DUMP_EXTENSION)));
        } else {
            Set<DumpInfo> dbNames = MutableSetFactoryImpl.INSTANCE.empty();
            try {
                for (Path path : fs.listFiles(sourcePath)) {
                    String fileName = path.getFileName().toString();
                    if (!fs.isDirectory(path) && fileName.endsWith(DUMP_EXTENSION)) {
                        String dbName = fileName.substring(0, fileName.length() - DUMP_EXTENSION.length());
                        if (database.matches(dbName)) {
                            dbNames.add(new DumpInfo(dbName, path));
                        }
                    }
                }
            } catch (IOException e) {
                throw new CommandFailedException("Failed to list dump files", e);
            }
            if (dbNames.isEmpty()) {
                throw new CommandFailedException(
                        "Pattern '" + database.getDatabaseName() + "' did not match any dump file in " + sourcePath);
            }
            return dbNames;
        }
    }

    protected Config buildConfig() {
        return createPrefilledConfigBuilder().build();
    }
}
