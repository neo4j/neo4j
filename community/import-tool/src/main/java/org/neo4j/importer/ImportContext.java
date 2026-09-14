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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.import_detailed_reporting_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.APPEND_OPTIONS;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.TRUNCATE_OPTIONS;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.INFO;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.DetailedProgressReport;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.ResumableStateAccessor;
import org.neo4j.batchimport.api.UnsupportedFormatException;
import org.neo4j.batchimport.api.input.ResumableStateData;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExitCode;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.importer.FileImporter.CsvImportException;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction.PatternStyle;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LoggerTarget;
import org.neo4j.memory.EmptyMemoryTracker;
import picocli.CommandLine.ParameterException;

public class ImportContext extends Monitor.Delegate implements InternalLogProvider, ResumableStateAccessor {

    private static final DateTimeFormatter SPACELESS_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss").withZone(ZoneId.systemDefault());
    private static final String DEFAULT_LOG_DIR_TEMPLATE = "%s-admin-import-%s";

    public static final String LOG_FILE_NAME = "import.log";
    public static final String PROGRESS_REPORTING_FILE_NAME = "progress.json.log";
    public static final String DEFAULT_REPORT_FILE_NAME = "report.json.log";
    public static final String CLI_ARGS_FILE_NAME = "cli-args";
    public static final String NODES_PER_RANGE_FILE_NAME = "nodes-per-range";
    public static final String CONFIG_FILE_NAME = "config";
    public static final String SUCCESS_FILE_NAME = "success";
    public static final String TEMP_FILE_SUFFIX = ".tmp";
    public static final String CHECKPOINT_FILE_NAME = "checkpoint";
    public static final String INPUT_FILES_FILE_NAME = "input-files";

    private static final HashFunction INPUT_FILES_HASH = Hashing.murmur3_128();

    private static final long MISSING_FILE_SIZE = -1;

    private final String dbName;

    private final String collectorPath;

    private final Config databaseConfig;

    private final Path baseDir;

    private final FileSystemAbstraction fs;

    private final List<String> originalArgs;

    private final LazyIO<InternalLogProvider> logProvider;

    private final LazyIO<PrintStream> progressStream;

    private final LazyIO<StoreChannel> collectorChannel;

    private final Function<RetainCheck, Boolean> retainContextDir;

    private final ObjectMapper objectMapper;

    private final Duration resumableImportCheckpointInterval;

    private boolean hasErrors;

    private ImportContext(
            String dbName,
            Config databaseConfig,
            Path baseDir,
            FileSystemAbstraction fs,
            List<String> originalArgs,
            String collectorPath,
            Path collectorOutputPath,
            boolean resuming,
            Function<RetainCheck, Boolean> retainContextDir,
            boolean includeUpdatesInProgress,
            boolean verbose,
            Duration resumableImportCheckpointInterval) {
        super(Monitor.NO_MONITOR);
        this.dbName = dbName;
        this.databaseConfig = databaseConfig;
        this.baseDir = baseDir;
        this.fs = fs;
        this.originalArgs = originalArgs;
        this.collectorPath = collectorPath;
        this.logProvider = new LazyIO<>(
                () -> new Log4jLogProvider(new BufferedOutputStream(output(logPath())), verbose ? DEBUG : INFO));
        this.progressStream = new LazyIO<>(() -> new PrintStream(output(progressReportingPath()), true));
        this.collectorChannel = new LazyIO<>(() -> channel(collectorOutputPath, resuming));
        this.retainContextDir = retainContextDir;
        this.objectMapper = new ObjectMapper()
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
                .registerModule(new SimpleModule()
                        .addSerializer(new DurationSerializer())
                        .addSerializer(new StatsSerializer(includeUpdatesInProgress)));
        this.resumableImportCheckpointInterval = resumableImportCheckpointInterval;
    }

    /**
     * @param previousBaseDir the Path to the previous attempt's context directory, or {@code null} if '--resume' was not specified
     */
    public static ImportContext create(
            FileSystemAbstraction fs,
            NormalizedDatabaseName database,
            Path previousBaseDir,
            Config databaseConfig,
            Path collectorReporting,
            List<String> originalArgs,
            boolean includeUpdatesInProgress,
            boolean retainForInstrumentation,
            boolean verbose,
            Duration resumableImportCheckpointInterval) {
        Path baseDir = previousBaseDir != null
                ? previousBaseDir
                : newContextDir(fs, databaseConfig.get(logs_directory).toAbsolutePath(), database.name());
        var collectorReportingIsInContextDir = collectorReporting == null;
        var resolvedCollectorPath =
                collectorReportingIsInContextDir ? baseDir.resolve(DEFAULT_REPORT_FILE_NAME) : collectorReporting;
        var retaining = verbose || retainForInstrumentation;
        return new ImportContext(
                database.name(),
                databaseConfig,
                baseDir,
                fs,
                originalArgs,
                collectorReporting == null ? DEFAULT_REPORT_FILE_NAME : collectorReporting.toString(),
                resolvedCollectorPath,
                previousBaseDir != null,
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
                verbose,
                resumableImportCheckpointInterval);
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
        return baseDir;
    }

    public Path logPath() {
        return baseDir().resolve(LOG_FILE_NAME);
    }

    public Path progressReportingPath() {
        return baseDir().resolve(PROGRESS_REPORTING_FILE_NAME);
    }

    public StoreChannel collectorChannel() {
        return collectorChannel.get();
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
            IOUtils.closeAllUnchecked(logProvider, progressStream, collectorChannel);
        } finally {
            clearBaseDir();
        }
    }

    private void clearBaseDir() {
        if (!(hasErrors || retainContextDir.apply(RetainCheck.CLEARING))) {
            var baseDir = baseDir();
            try {
                FileUtils.deleteDirectory(baseDir);
            } catch (IOException e) {
                var error = new CommandFailedException(e, ExitCode.SOFTWARE);
                error.addSupplementaryMessage("Unable to fully clear the import context directory: " + baseDir);
                throw error;
            }
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
            Path pathParent = path.toAbsolutePath().getParent();
            if (pathParent != null) {
                fs.mkdirs(pathParent);
            }
            return fs.openAsOutputStream(path, true);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private void writeToTempFileAndReplaceAtomically(Path parentPath, String fileName, String content)
            throws IOException {
        writeToTempFileAndReplaceAtomically(parentPath, fileName, content.getBytes(UTF_8));
    }

    private void writeToTempFileAndReplaceAtomically(Path parentPath, String fileName, byte[] content)
            throws IOException {
        Path path = parentPath.resolve(fileName);
        Path tmpPath = parentPath.resolve(fileName + TEMP_FILE_SUFFIX);
        try (StoreChannel channel = fs.open(tmpPath, TRUNCATE_OPTIONS)) {
            channel.writeAll(ByteBuffer.wrap(content));
            channel.force(false);
        }
        fs.renameFile(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
        // the move is atomic, but only forcing the directory it happened in makes it durable.
        fs.tryForceDirectory(parentPath);
    }

    private StoreChannel channel(Path path, boolean append) throws UncheckedIOException {
        try {
            fs.mkdirs(path.getParent());
            return fs.open(path, append ? APPEND_OPTIONS : TRUNCATE_OPTIONS);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Persists the CLI arguments the import was invoked with into the context directory, one per line - a single
     * argument (e.g. a file path) may itself contain whitespace, so joining/splitting on whitespace would corrupt it.
     * Written {@link #writeProtected(Path, String, String) write-protected}, since a hand edit would quietly change what a
     * later '--resume' replays.
     */
    public void persistCliArgs() {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir(), CLI_ARGS_FILE_NAME, String.join("\n", originalArgs));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Persists the configuration the import actually ran with into the context directory, in {@code neo4j.conf}
     * format. Every declared setting with a value is written, not just the explicitly set ones, since a setting left
     * at its default can still resolve differently between attempts - several defaults are derived from the machine
     * (available memory, processors) or from the environment ({@code NEO4J_HOME} and the paths hanging off it).
     * <p>
     * A resumed import picks up state that the attempt it resumes laid out on disk (the partially written store, its
     * temporary intermediary data, the node id ranges the work was divided into), so it is only safe to continue if
     * the settings that shaped that state still hold the same values, which this makes it possible to tell.
     * <p>
     * Written {@link #writeProtected(Path, String, String) write-protected}, since a hand edit would decide whether a resume
     * is allowed to run at all.
     */
    public void persistConfig() {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir(), CONFIG_FILE_NAME, asConfigFile(configValuesStringMapping(databaseConfig)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Map<String, String> configValuesStringMapping(Config config) {
        HashMap<String, String> settingValueAsStringByName = new HashMap<>();
        for (Entry<String, Setting<Object>> entry : config.getDeclaredSettings().entrySet()) {
            String name = entry.getKey();
            Setting<Object> setting = entry.getValue();
            var value = config.get(setting);
            if (value != null) {
                settingValueAsStringByName.put(name, ((SettingImpl<Object>) setting).valueToString(value));
            }
        }
        return settingValueAsStringByName;
    }

    /**
     * Renders setting values as the file {@link Config.Builder#fromFile} reads back in
     * {@link #resumeSensitiveChanges}. Escaping is left to {@link MapUtil#store}, whose
     * {@link java.util.Properties} format is what that reader parses: the values are raw strings, so on Windows a path
     * value carries backslashes (e.g. {@code Z:\work\...}) that the reader would otherwise take for escape sequences,
     * rejecting a stray backslash-u with a "Malformed" unicode-encoding error.
     */
    static String asConfigFile(Map<String, String> configValues) throws IOException {
        var file = new StringWriter();
        MapUtil.store(configValues, file);
        return file.toString();
    }

    @Override
    public void persistNodesPerRange(long nodesPerRange) {
        try {
            fs.mkdirs(baseDir());
            writeToTempFileAndReplaceAtomically(baseDir(), NODES_PER_RANGE_FILE_NAME, Long.toString(nodesPerRange));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long lastNodesPerRange() {
        try {
            Path path = baseDir().resolve(NODES_PER_RANGE_FILE_NAME);
            if (!fs.fileExists(path)) {
                return NO_PREVIOUS_NODES_PER_RANGE;
            }
            try (var is = fs.openAsInputStream(path)) {
                return Long.parseLong(new String(is.readAllBytes(), UTF_8));
            } catch (NumberFormatException e) {
                throw new IOException("Invalid nodes-per-range value in " + path, e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void writeCheckpoint(ResumableStateData resumableStateData) throws IOException {
        fs.mkdirs(baseDir());
        writeToTempFileAndReplaceAtomically(baseDir(), CHECKPOINT_FILE_NAME, resumableStateData.asByteArray());
    }

    @Override
    public DataInputStream lastCheckpoint() {
        try {
            Path checkpoint = baseDir().resolve(CHECKPOINT_FILE_NAME);
            if (!fs.fileExists(checkpoint)) {
                return null;
            }
            return new DataInputStream(fs.openAsInputStream(checkpoint));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Duration resumableImportCheckpointInterval() {
        return resumableImportCheckpointInterval;
    }

    /**
     * Persists a 64-bit hash over the input files the import read - every file's name together with its size, in name
     * order, so that the same sizes shuffled between files hash differently, see {@link #inputFilesHash(ArrayList)}. A
     * best-effort guard only: it catches input swapped, truncated or regenerated between attempts, not a deliberate
     * forgery. Modification times are deliberately left out, since copying or checking out the very same input changes
     * them.
     * <p>
     * Written {@link #writeProtected write-protected}, like the other records of an attempt, since a
     * hand edit would decide whether a resume is allowed to run at all.
     */
    void persistInputFilesHash(ArrayList<Path> inputFiles) {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir(), INPUT_FILES_FILE_NAME, Long.toString(inputFilesHash(inputFiles)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Whether the input files this run would read are no longer the ones the attempt owning the given context
     * directory read - a file gone, added, renamed, resized, or the same sizes belonging to differently named files
     * than before. Where those files are kept is not part of it, so the same input reached over another path still
     * matches. Only says that something changed, not what: all that is kept of the input is the single hash
     * {@link #persistInputFilesHash persisted} for it.
     * <p>
     * An attempt that recorded no hash at all cannot be compared against and passes.
     */
    public boolean inputFilesChanged(ArrayList<Path> inputFiles) throws IOException {
        Path recorded = baseDir().resolve(INPUT_FILES_FILE_NAME);
        if (!fs.fileExists(recorded)) {
            return false;
        }
        try {
            String readBackString = FileSystemUtils.readString(fs, recorded, EmptyMemoryTracker.INSTANCE);
            long oldHash = (readBackString == null ? 0 : Long.parseLong(readBackString));
            return oldHash != inputFilesHash(inputFiles);
        } catch (NumberFormatException e) {
            return true;
        }
    }

    /**
     * Hashes each file's name together with its size, rather than its whole path, so that the same input kept somewhere
     * else still matches.
     * <p>
     * Sorted here, by file name, so that the same files hash the same however their file groups were laid out.
     */
    private long inputFilesHash(ArrayList<Path> inputFiles) throws IOException {
        inputFiles.sort(Comparator.comparing(ImportContext::fileNameOf));
        Hasher hasher = INPUT_FILES_HASH.newHasher();
        hasher.putInt(inputFiles.size());
        for (Path inputFile : inputFiles) {
            String name = fileNameOf(inputFile);
            hasher.putInt(name.length());
            hasher.putUnencodedChars(name);
            hasher.putLong(sizeOf(inputFile));
        }
        return hasher.hash().asLong();
    }

    private static String fileNameOf(Path file) {
        Path fileName = file.getFileName();
        return fileName == null ? file.toString() : fileName.toString();
    }

    /**
     * The size of a file that is there, and {@link #MISSING_FILE_SIZE} for one that is not: a file the import is about
     * to read fails the import on its own terms, whereas one gone missing since the attempt being resumed read it has
     * to hash differently than it did then.
     */
    private long sizeOf(Path file) throws IOException {
        try {
            return fs.getFileSize(file);
        } catch (NoSuchFileException e) {
            return MISSING_FILE_SIZE;
        } catch (UncheckedIOException e) {
            // a cloud storage path fetches its size when asked for it rather than when its attributes are read, so a
            // missing object surfaces here, wrapped, and as either of the two exceptions that stand for 'not there'
            if (e.getCause() instanceof NoSuchFileException || e.getCause() instanceof FileNotFoundException) {
                return MISSING_FILE_SIZE;
            }
            throw e;
        }
    }

    /**
     * A setting that holds a different value now than it did for the attempt being resumed. A value is null when the
     * setting had none at all on that side.
     */
    public record SettingChange(Setting<?> setting, Object previous, Object current) {}

    /**
     * Which declared settings the given configuration resolves differently than the attempt that owns the given context
     * directory did. Ordered by setting name.
     *
     * @param resumeSafe whether a change to the given setting leaves the attempt resumable regardless
     */
    public static List<SettingChange> resumeSensitiveChanges(
            Path contextDir, Config current, Predicate<Setting<?>> resumeSafe) {
        Path configPath = contextDir.resolve(CONFIG_FILE_NAME);
        if (!Files.exists(configPath)) {
            return List.of();
        }
        Config previous = Config.newBuilder().fromFile(configPath).build();
        return current.getDeclaredSettings().values().stream()
                .filter(setting -> !resumeSafe.test(setting))
                .filter(setting -> !Objects.equals(previous.get(setting), current.get(setting)))
                .sorted(Comparator.comparing(Setting::name))
                .map(setting -> new SettingChange(setting, previous.get(setting), current.get(setting)))
                .toList();
    }

    /**
     * Records that the import completed, so that a retained context directory can be told apart from one left behind
     * by an attempt that did not finish. A retained directory outlives a successful import (see
     * {@link #create(FileSystemAbstraction, NormalizedDatabaseName, Path, Config, Path, List, boolean, boolean, boolean, Duration)}),
     * and without this there is nothing in it that says the import got all the way through. Written
     * {@link #writeProtected(Path, String, String) write-protected}, like the other records of an attempt.
     */
    public void markSuccessful() {
        try {
            fs.mkdirs(baseDir());
            writeProtected(baseDir(), SUCCESS_FILE_NAME, "");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes a file and takes the write permission off it afterwards, leaving it removable. On POSIX that comes for
     * free - what may be removed from a directory is the directory's business, not the file's - whereas on Windows the
     * read-only attribute the permission maps to also refuses deletion, so write access is denied through the ACL
     * instead, which leaves deletion alone. A filesystem offering neither leaves the file writable rather than
     * undeletable. The owner can always put the permission back.
     */
    private void writeProtected(Path parentPath, String fileName, String content) throws IOException {
        Path path = parentPath.resolve(fileName);
        makeSureIsInBaseDir(path);
        writeUnprotect(path);
        writeToTempFileAndReplaceAtomically(parentPath, fileName, content);
        writeProtect(path);
    }

    /**
     * Permissions are the one thing {@link FileSystemAbstraction} does not cover, so the two steps around the write
     * reach for {@link Files} instead. That leaves them addressing whatever the default filesystem holds rather than
     * what {@link #fs} just wrote, hence the {@link Files#exists(Path, java.nio.file.LinkOption...)} guards: on an
     * abstraction that keeps its files elsewhere there is nothing to protect, and the attribute views would fail on a
     * path the default filesystem does not have.
     */
    private static void writeProtect(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        var posix = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posix != null) {
            posix.setPermissions(Set.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ));
            return;
        }
        var acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
        if (acl == null) {
            return;
        }
        var denyWrite = AclEntry.newBuilder()
                .setType(AclEntryType.DENY)
                .setPrincipal(acl.getOwner())
                .setPermissions(AclEntryPermission.WRITE_DATA, AclEntryPermission.APPEND_DATA)
                .build();
        var entries = new ArrayList<>(acl.getAcl());
        // a DENY entry only takes effect ahead of the entries granting what it takes away
        entries.addFirst(denyWrite);
        acl.setAcl(entries);
    }

    private static void writeUnprotect(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        var posix = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posix != null) {
            posix.setPermissions(Set.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ));
            return;
        }
        var acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
        if (acl == null) {
            return;
        }
        var entries = new ArrayList<>(acl.getAcl());
        var iterator = entries.iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.type() == AclEntryType.DENY
                    && entry.principal().equals(acl.getOwner())
                    && entry.permissions().contains(AclEntryPermission.WRITE_DATA)) {
                iterator.remove();
                break;
            }
        }
        acl.setAcl(entries);
    }

    private void makeSureIsInBaseDir(Path path) {
        if (!path.startsWith(baseDir())) {
            throw new IllegalArgumentException("Path " + path + " is not in the import context directory " + baseDir());
        }
    }

    /**
     * Whether the import that owned the given context directory completed, see {@link #markSuccessful()}.
     */
    public static boolean wasSuccessful(FileSystemAbstraction fs, Path contextDir) {
        return fs.fileExists(contextDir.resolve(SUCCESS_FILE_NAME));
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
    public static Optional<List<String>> readCliArgs(FileSystemAbstraction fs, Path contextDir) throws IOException {
        Path cliArgsPath = contextDir.resolve(CLI_ARGS_FILE_NAME);
        if (!fs.fileExists(cliArgsPath)) {
            return Optional.empty();
        }
        String content;
        try (var in = fs.openAsInputStream(cliArgsPath)) {
            content = new String(in.readAllBytes(), UTF_8);
        }
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
