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
package org.neo4j.importer;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.import_context_directory;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.import_detailed_reporting_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.INFO;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.DetailedProgressReport;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.UnsupportedFormatException;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExitCode;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.importer.FileImporter.CsvImportException;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction.PatternStyle;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LoggerTarget;
import picocli.CommandLine.ParameterException;

public class ImportContext extends Monitor.Delegate implements InternalLogProvider {

    private static final DateTimeFormatter SPACELESS_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss").withZone(ZoneId.systemDefault());
    private static final String DEFAULT_LOG_DIR_TEMPLATE = "%s-admin-import-%s";

    public static final String LOG_FILE_NAME = "import.log";
    public static final String PROGRESS_REPORTING_FILE_NAME = "progress.json.log";
    public static final String DEFAULT_REPORT_FILE_NAME = "report.json.log";
    public static final String CLI_ARGS_FILE_NAME = "cli-args";
    public static final String SUCCESS_FILE_NAME = "success";

    private final String dbName;

    private final String collectorPath;

    private final Config databaseConfig;

    private final FileSystemAbstraction fs;

    private final List<String> originalArgs;

    private final LazyIO<InternalLogProvider> logProvider;

    private final LazyIO<PrintStream> progressStream;

    private final LazyIO<OutputStream> collectorStream;

    private final Function<RetainCheck, Boolean> retainContextDir;

    private final ObjectMapper objectMapper;

    private boolean hasErrors;

    private ImportContext(
            String dbName,
            Config databaseConfig,
            FileSystemAbstraction fs,
            List<String> originalArgs,
            String collectorPath,
            Path collectorOutputPath,
            Function<RetainCheck, Boolean> retainContextDir,
            boolean includeUpdatesInProgress,
            boolean verbose) {
        super(Monitor.NO_MONITOR);
        this.dbName = dbName;
        this.databaseConfig = databaseConfig;
        this.fs = fs;
        this.originalArgs = originalArgs;
        this.collectorPath = collectorPath;
        this.logProvider = new LazyIO<>(
                () -> new Log4jLogProvider(new BufferedOutputStream(output(logPath())), verbose ? DEBUG : INFO));
        this.progressStream = new LazyIO<>(() -> new PrintStream(output(progressReportingPath()), true));
        this.collectorStream = new LazyIO<>(() -> output(collectorOutputPath));
        this.retainContextDir = retainContextDir;
        this.objectMapper = new ObjectMapper()
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
                .registerModule(new SimpleModule()
                        .addSerializer(new DurationSerializer())
                        .addSerializer(new StatsSerializer(includeUpdatesInProgress)));
    }

    public static ImportContext create(
            FileSystemAbstraction fs,
            NormalizedDatabaseName database,
            Config databaseConfig,
            Path collectorReporting,
            List<String> originalArgs,
            boolean includeUpdatesInProgress,
            boolean retainForInstrumentation,
            boolean verbose) {
        var baseDir = newContextDir(fs, databaseConfig.get(logs_directory).toAbsolutePath(), database.name());
        var collectorReportingIsInContextDir = collectorReporting == null;
        var resolvedCollectorPath =
                collectorReportingIsInContextDir ? baseDir.resolve(DEFAULT_REPORT_FILE_NAME) : collectorReporting;
        var retaining = verbose || retainForInstrumentation;
        return new ImportContext(
                database.name(),
                Config.newBuilder()
                        .fromConfig(databaseConfig)
                        .set(import_context_directory, baseDir)
                        .build(),
                fs,
                originalArgs,
                collectorReporting == null ? DEFAULT_REPORT_FILE_NAME : collectorReporting.toString(),
                resolvedCollectorPath,
                check -> {
                    if (check == RetainCheck.PREAMBLE) {
                        return retaining;
                    }
                    try {
                        return retaining
                                || (collectorReportingIsInContextDir
                                        && fs.fileExists(resolvedCollectorPath)
                                        && fs.getFileSize(resolvedCollectorPath) > 0);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                },
                includeUpdatesInProgress,
                verbose);
    }

    public void preamble(PrintStream out) {
        var baseDir = baseDir();
        out.printf("Starting to import, the following output will be saved in the directory: %s%n", baseDir);
        out.printf("  Logging information: %s%n", LOG_FILE_NAME);
        out.printf("  Detailed progress reporting (JSON formatted): %s%n", PROGRESS_REPORTING_FILE_NAME);
        out.printf("  Import data errors / violations (JSON formatted): %s%n", collectorPath);
        if (!retainContextDir.apply(RetainCheck.PREAMBLE)) {
            out.println();
            out.println("NOTE this directory will be cleared on the completion of a successful import.");
        }
        out.println();
    }

    public Config config() {
        return databaseConfig;
    }

    public Path baseDir() {
        return databaseConfig.get(import_context_directory);
    }

    public Path logPath() {
        return baseDir().resolve(LOG_FILE_NAME);
    }

    public Path progressReportingPath() {
        return baseDir().resolve(PROGRESS_REPORTING_FILE_NAME);
    }

    public OutputStream collectorOutputStream() {
        return collectorStream.get();
    }

    public Exception captureError(Exception error) {
        hasErrors = true;
        if (error instanceof ParameterException) {
            return error;
        } else if (error instanceof FileLockException) {
            return new CommandFailedException(
                    "The database is in use. Stop database '%s' and try again.".formatted(dbName),
                    error,
                    ExitCode.FAIL);
        } else if (error instanceof CannotWriteException) {
            return new CommandFailedException("You do not have permission to import.", error, ExitCode.NOPERM);
        } else if (error instanceof CsvImportException) {
            return new CommandFailedException("Error importing csv file.", error, ExitCode.SOFTWARE);
        } else if (error instanceof UnsupportedFormatException) {
            return new CommandFailedException("Unsupported format.", error, ExitCode.SOFTWARE);
        } else if (error instanceof UncheckedIOException ioEx) {
            return transformIOException(ioEx.getCause());
        } else if (error instanceof IOException ioEx) {
            return transformIOException(ioEx);
        }
        return error;
    }

    @Override
    public InternalLog getLog(Class<?> loggingClass) {
        return logProvider.get().getLog(loggingClass);
    }

    @Override
    public InternalLog getLog(String name) {
        return logProvider.get().getLog(name);
    }

    @Override
    public InternalLog getLog(LoggerTarget target) {
        return logProvider.get().getLog(target);
    }

    @Override
    public long detailedProgressReportIntervalMillis() {
        return databaseConfig.get(import_detailed_reporting_interval).toMillis();
    }

    @Override
    public void detailedProgressReport(DetailedProgressReport report) {
        try {
            var out = progressStream.get();
            objectMapper.writeValue(out, report);
            out.println();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        try {
            IOUtils.closeAllUnchecked(logProvider, progressStream, collectorStream);
        } finally {
            clearBaseDir();
        }
    }

    private void clearBaseDir() {
        if (!(hasErrors || retainContextDir.apply(RetainCheck.CLEARING))) {
            var baseDir = baseDir();
            try {
                allowDeletionOfWriteProtectedFiles(baseDir);
                FileUtils.deleteDirectory(baseDir);
            } catch (IOException e) {
                var error = new CommandFailedException(e, ExitCode.SOFTWARE);
                error.addSupplementaryMessage("Unable to fully clear the import context directory: " + baseDir);
                throw error;
            }
        }
    }

    /**
     * Windows refuses to delete a file carrying the read-only attribute, which is what {@link #writeProtected(Path,
     * String)} leaves behind. Best effort - the deletion itself reports whatever it could not remove.
     */
    private static void allowDeletionOfWriteProtectedFiles(Path baseDir) {
        try (var entries = Files.list(baseDir)) {
            entries.forEach(entry -> entry.toFile().setWritable(true));
        } catch (IOException | UncheckedIOException | UnsupportedOperationException e) {
            // nothing to do, the files that matter are the ones the deletion below complains about
        }
    }

    private static CommandFailedException transformIOException(IOException e) {
        var error = new CommandFailedException(e, ExitCode.SOFTWARE);
        if (e instanceof NoSuchFileException ex) {
            error.addSupplementaryMessage(
                    "Check that the file '%s' exists or is specified correctly.".formatted(ex.getFile()));
        } else if (e.getCause() instanceof ProviderMismatchException) {
            error.addSupplementaryMessage("The scheme of the provided URI is not currently supported - currently "
                    + "only 's3', 'gs' and 'azb' schemes are supported.");
        } else if (e.getCause() instanceof URISyntaxException) {
            error.addSupplementaryMessage("Please check that the syntax of the URI resource provided is correct.");
        }
        return error;
    }

    /**
     * The counter distinguishing attempts started within the same second is zero-padded to a fixed width so that the
     * directory names of a database's attempts sort lexicographically by recency - unpadded, '.10' would sort before
     * '.2'. The width leaves room for far more attempts within one second than any real setup produces.
     */
    private static Path newContextDir(FileSystemAbstraction fs, Path importsDir, String dbName) {
        var ts = SPACELESS_DATE_FORMATTER.format(Instant.now());
        var repeat = 0;
        Path contextDir;
        do {
            var suffix = ts + (repeat++ == 0 ? "" : format(".%03d", repeat));
            contextDir = importsDir.resolve(format(DEFAULT_LOG_DIR_TEMPLATE, dbName, suffix));
        } while (fs.fileExists(contextDir));
        return contextDir;
    }

    private OutputStream output(Path path) throws UncheckedIOException {
        try {
            fs.mkdirs(path.getParent());
            // NOTE collector needs to be appending when we switch to resumable imports
            return fs.openAsOutputStream(path, false);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Persists the CLI arguments the import was invoked with into the context directory, one per line - a single
     * argument (e.g. a file path) may itself contain whitespace, so joining/splitting on whitespace would corrupt it.
     */
    public void persistCliArgs() {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir().resolve(CLI_ARGS_FILE_NAME), String.join("\n", originalArgs));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Records that the import completed, so that a retained context directory can be told apart from one left behind
     * by an attempt that did not finish. A retained directory outlives a successful import (see
     * {@link #create(FileSystemAbstraction, NormalizedDatabaseName, Config, Path, List, boolean, boolean, boolean)}),
     * and without this there is nothing in it that says the import got all the way through.
     */
    public void markSuccessful() {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir().resolve(SUCCESS_FILE_NAME), "");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes a file that records what an import attempt did, and takes the write permission off it afterwards. What
     * these files say decides what a later '--resume' reruns and whether it is allowed to run at all, so editing one
     * by hand quietly changes the import - or, for {@link #SUCCESS_FILE_NAME}, lets a completed import be rerun over
     * the database it produced. Read-only is a guard against doing that by accident, not protection against someone
     * who means to: the owner can put the permission back.
     */
    private static void writeProtected(Path path, String content) throws IOException {
        Files.writeString(path, content);
        try {
            Files.setPosixFilePermissions(
                    path,
                    Set.of(
                            PosixFilePermission.OWNER_READ,
                            PosixFilePermission.GROUP_READ,
                            PosixFilePermission.OTHERS_READ));
        } catch (UnsupportedOperationException e) {
            // fallback for windows
            path.toFile().setReadOnly();
        }
    }

    /**
     * Whether the import that owned the given context directory completed, see {@link #markSuccessful()}.
     */
    public static boolean wasSuccessful(Path contextDir) {
        return Files.exists(contextDir.resolve(SUCCESS_FILE_NAME));
    }

    /**
     * Finds the context directory of the most recent import attempt for the given database, if any is still present
     * (context directories are cleared on successful completion unless retained, so a previous attempt is only found
     * here if it was retained or did not complete successfully).
     */
    public static Optional<Path> mostRecentContextDir(FileSystemAbstraction fs, Path logsDir, String dbName)
            throws IOException {
        if (!fs.fileExists(logsDir)) {
            return Optional.empty();
        }
        List<Path> candidates =
                fs.matchFiles(logsDir, PatternStyle.GLOB, format(DEFAULT_LOG_DIR_TEMPLATE, dbName, "*"));
        return candidates.stream()
                .max(Comparator.comparing(path -> path.getFileName().toString()));
    }

    /**
     * Reads back the CLI arguments persisted by {@link #persistCliArgs()} for a given context directory.
     */
    public static Optional<List<String>> readCliArgs(Path contextDir) throws IOException {
        Path cliArgsPath = contextDir.resolve(CLI_ARGS_FILE_NAME);
        if (!Files.exists(cliArgsPath)) {
            return Optional.empty();
        }
        String content = Files.readString(cliArgsPath);
        return Optional.of(content.isEmpty() ? List.of() : content.lines().toList());
    }

    private static class DurationSerializer extends StdSerializer<Duration> {

        private DurationSerializer() {
            super(Duration.class);
        }

        @Override
        public void serialize(Duration duration, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeNumber(duration.toMillis());
        }
    }

    private static class StatsSerializer extends StdSerializer<DetailedProgressReport.Stats> {

        private final boolean includeUpdatesInProgress;

        private StatsSerializer(boolean includeUpdatesInProgress) {
            super(DetailedProgressReport.Stats.class);
            this.includeUpdatesInProgress = includeUpdatesInProgress;
        }

        @Override
        public void serialize(
                DetailedProgressReport.Stats stats, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("processed", stats.processed());
            jsonGenerator.writeNumberField("created", stats.created());
            if (includeUpdatesInProgress) {
                jsonGenerator.writeNumberField("updated", stats.updated());
                jsonGenerator.writeNumberField("deleted", stats.deleted());
            }
            jsonGenerator.writeEndObject();
        }
    }

    private enum RetainCheck {
        PREAMBLE,
        CLEARING
    }

    private static class LazyIO<T extends Closeable> implements Supplier<T>, Closeable {

        private final Supplier<T> supplier;

        private T resource;

        private boolean initialized;

        private LazyIO(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T get() {
            if (resource == null) {
                synchronized (this) {
                    if (!initialized) {
                        initialized = true;
                        resource = supplier.get();
                    }
                }
            }

            return resource;
        }

        @Override
        public void close() {
            IOUtils.closeUnchecked(resource);
        }
    }
}
