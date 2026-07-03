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
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getThrowableList;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfType;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.duplication_user_messages;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.data;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.ImportValidationException;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.UnsupportedFormatException;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.cloud.storage.StorageUtils;
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.importer.ImportCommand.ShardingArguments;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.MissingRelationshipDataException;
import org.neo4j.internal.batchimport.input.ProblemReporters;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.CsvInput.PrintingMonitor;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.internal.batchimport.input.parquet.ParquetInput;
import org.neo4j.internal.batchimport.input.parquet.ParquetMonitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.PrefixedLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.token.TokenHolders;

public class FileImporter {

    private static final String MULTILINE_HINT = "Detected field which spanned multiple lines for an import where "
            + "--multiline-fields=false. If you know that your input data "
            + "include fields containing new-line characters then import with this option set to "
            + "true.";

    private final DatabaseLayout databaseLayout;
    private final Config databaseConfig;
    private final StorageEngineFactory storageEngineFactory;
    private final org.neo4j.csv.reader.Configuration csvConfig;
    private final Configuration importConfig;
    private final ThrowingSupplier<OutputStream, IOException> reportOutputStream;
    private final IdType defaultIdType;
    private final Charset inputEncoding;
    private final boolean ignoreExtraColumns;
    private final boolean skipBadRelationships;
    private final boolean skipDuplicateNodes;
    private final boolean skipBadEntriesLogging;
    private final long badTolerance;
    private final boolean normalizeTypes;
    private final boolean verbose;
    private final boolean autoSkipHeaders;
    private final Map<Set<String>, List<FileGroup>> nodeFiles;
    private final Map<String, List<FileGroup>> relationshipFiles;
    private final FileSystemAbstraction fileSystem;
    private final PrintStream stdOut;
    private final PrintStream stdErr;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final boolean force;
    private final InternalLogProvider logProvider;
    private final SchemaCommandSource schemaCommands;
    private final FileInputType fileInputType;
    private final ShardingArguments shardingArguments;
    private final Monitor monitor;

    private FileImporter(Builder b) {
        this.databaseLayout = requireNonNull(b.databaseLayout);
        this.databaseConfig = requireNonNull(b.databaseConfig);
        this.storageEngineFactory = requireNonNull(b.storageEngineFactory);
        this.csvConfig = requireNonNull(b.csvConfig);
        this.importConfig = requireNonNull(b.importConfig);
        this.reportOutputStream = requireNonNull(b.reportOutputStream);
        this.defaultIdType = requireNonNull(b.defaultIdType);
        this.inputEncoding = requireNonNull(b.inputEncoding);
        this.ignoreExtraColumns = b.ignoreExtraColumns;
        this.skipBadRelationships = b.skipBadRelationships;
        this.skipDuplicateNodes = b.skipDuplicateNodes;
        this.skipBadEntriesLogging = b.skipBadEntriesLogging;
        this.badTolerance = b.badTolerance;
        this.normalizeTypes = b.normalizeTypes;
        this.verbose = b.verbose;
        this.autoSkipHeaders = b.autoSkipHeaders;
        this.nodeFiles = requireNonNull(b.nodeFiles);
        this.relationshipFiles = requireNonNull(b.relationshipFiles);
        this.fileSystem = requireNonNull(b.fileSystem);
        this.pageCacheTracer = requireNonNull(b.pageCacheTracer);
        this.contextFactory = requireNonNull(b.contextFactory);
        this.memoryTracker = requireNonNull(b.memoryTracker);
        this.stdOut = requireNonNull(b.stdOut);
        this.stdErr = requireNonNull(b.stdErr);
        this.logProvider = requireNonNull(b.logProvider);
        this.force = b.force;
        this.schemaCommands = b.schemaCommands;
        this.fileInputType = b.fileInputType;
        this.shardingArguments = b.shardingArguments;
        this.monitor = b.monitor;
    }

    public FileInputType fileInputType() {
        return fileInputType;
    }

    public void dryRun(ImportCommand.Base type) throws IOException {
        printOverview(true);

        try (var input = importInput();
                var jobScheduler = createInitialisedScheduler()) {
            type.doDryRun(
                    input,
                    fileSystem,
                    databaseLayout,
                    databaseConfig,
                    storageEngineFactory,
                    jobScheduler,
                    contextFactory,
                    importConfig,
                    () -> new IndexProvidersAccess() {
                        @Override
                        public IndexProviderMap access(
                                PageCache pageCache,
                                DatabaseLayout layout,
                                DatabaseReadOnlyChecker readOnlyChecker,
                                MemoryTracker memoryTracker) {
                            return unsupported();
                        }

                        @Override
                        public IndexProviderMap access(
                                PageCache pageCache,
                                DatabaseLayout layout,
                                DatabaseReadOnlyChecker readOnlyChecker,
                                TokenHolders tokenHolders) {
                            return unsupported();
                        }

                        @Override
                        public void close() {}

                        private IndexProviderMap unsupported() {
                            throw new UnsupportedOperationException(
                                    "Indexes do not need to be accessed during a dry run");
                        }
                    },
                    stdOut,
                    verbose,
                    shardingArguments,
                    monitor);
        } catch (Exception ex) {
            throw csvImportExceptionWrapped(databaseLayout.getDatabaseName(), ex, type.importType());
        }
    }

    public void doImport(ImportCommand.Base type, boolean skidbladnir) throws IOException {
        if (force) {
            fileSystem.deleteRecursively(
                    databaseLayout.databaseDirectory(), path -> !path.equals(databaseLayout.databaseLockFile()));
            fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());
        }

        try (var badCollector = getBadCollector();
                var input = importInput()) {
            doImport(input, badCollector, type, skidbladnir);
        }
    }

    private Input importInput() {
        // extract the default time zone from the database configuration
        var dbTimeZone = databaseConfig.get(db_temporal_timezone);
        var nodeData = nodeData();
        var relationshipsData = relationshipData();
        return importInput(nodeData, relationshipsData, () -> dbTimeZone);
    }

    private void abortIfVectorsUnsupported(Input input) {
        if (!storageEngineFactory.supportsVectorData() && input.containsVectorData()) {
            throw new UnsupportedOperationException("Provided input is known to contain vector value data, "
                    + "which is not supported by the target storage engine.");
        }
    }

    private Input importInput(
            Iterable<DataFactory> nodeData, Iterable<DataFactory> relationshipsData, Supplier<ZoneId> defaultTimeZone) {
        return switch (fileInputType) {
            case CSV ->
                new CsvInput(
                        nodeData,
                        defaultFormatNodeFileHeader(defaultTimeZone, normalizeTypes),
                        relationshipsData,
                        defaultFormatRelationshipFileHeader(defaultTimeZone, normalizeTypes),
                        schemaCommands,
                        defaultIdType,
                        csvConfig,
                        autoSkipHeaders,
                        new PrintingMonitor(stdOut),
                        new Groups(),
                        memoryTracker);
            case PARQUET -> {
                var input = new ParquetInput(
                        nodeFiles,
                        relationshipFiles,
                        schemaCommands,
                        defaultIdType,
                        csvConfig,
                        new Groups(),
                        new ParquetMonitor(stdOut));
                // For Parquet: Abort if vectors are unsupported.
                abortIfVectorsUnsupported(input);
                yield input;
            }
            case NO_INPUT -> null;
        };
    }

    private void doImport(Input input, Collector badCollector, ImportCommand.Base type, boolean skidbladnir) {
        boolean success = false;

        printOverview(false);

        try (JobScheduler jobScheduler = createInitialisedScheduler();
                Lifespan life = new Lifespan()) {
            // Let the storage engine factory be configurable in the tool later on...
            var logService = new SimpleLogService(
                    NullLogProvider.getInstance(),
                    new PrefixedLogProvider(logProvider, databaseLayout.getDatabaseName()),
                    databaseConfig.get(duplication_user_messages));
            Supplier<IndexProvidersAccess> indexProviders = () -> new DefaultIndexProvidersAccess(
                    storageEngineFactory,
                    fileSystem,
                    databaseConfig,
                    jobScheduler,
                    new SimpleLogService(logProvider),
                    pageCacheTracer,
                    contextFactory);
            if (skidbladnir) {
                type.doSkidbladnirImport(
                        fileSystem,
                        databaseLayout,
                        force,
                        databaseConfig,
                        storageEngineFactory,
                        jobScheduler,
                        logProvider,
                        pageCacheTracer,
                        contextFactory,
                        importConfig,
                        logService,
                        stdOut,
                        stdErr,
                        verbose,
                        badCollector,
                        memoryTracker,
                        input,
                        inputEncoding,
                        nodeFiles,
                        indexProviders,
                        shardingArguments,
                        monitor);
            } else {
                type.doImport(
                        fileSystem,
                        databaseLayout,
                        force,
                        databaseConfig,
                        storageEngineFactory,
                        jobScheduler,
                        logProvider,
                        pageCacheTracer,
                        contextFactory,
                        importConfig,
                        logService,
                        stdOut,
                        stdErr,
                        verbose,
                        badCollector,
                        memoryTracker,
                        input,
                        indexProviders,
                        shardingArguments,
                        monitor);
            }
            success = true;
        } catch (Exception ex) {
            throw csvImportExceptionWrapped(databaseLayout.getDatabaseName(), ex, type.importType());
        } finally {
            long numberOfBadEntries = badCollector.badEntries();
            if (badTolerance != BadCollector.UNLIMITED_TOLERANCE && numberOfBadEntries > badTolerance) {
                stdOut.println("Neo4j-admin aborted the import because " + numberOfBadEntries + " bad entries were "
                        + "found, which exceeds the set fault tolerance ("
                        + badTolerance + "). Import is optimized to import fault-free data.");
                stdOut.println();
                if (skipBadEntriesLogging) {
                    stdOut.println("Bad entry logging is disabled, enable it using --skip-bad-entries-logging=false.");
                }
                stdOut.println();
                stdOut.println("We recommend that data should be cleaned before importing. The fault-tolerance can be "
                        + "increased using --bad-tolerance=<num>, however this will dramatically affect the tool’s"
                        + " performance.");
                stdOut.println();
            }
            if (!success) {
                stdErr.println("WARNING Import failed. The store files in "
                        + databaseLayout.databaseDirectory().toAbsolutePath()
                        + " are left as they are, although they are likely in an unusable state. Starting a database"
                        + " on these store files will likely fail or observe inconsistent records so start at your own"
                        + " risk or delete the store manually.");
                stdOut.println();
            }
        }
    }

    /**
     * Wraps the provided exception in a {@link CsvImportException} which provides additional error information and
     * traceability.
     * <p>
     * <b>Note:</b> Instances of {@link UnsupportedFormatException} are not wrapped, and are returned as is.
     *
     * @param databaseName the name of the database to receive the import data
     * @param e            the error that occurred
     * @param importType   the import type (ex. full/incremental/sharded)
     */
    private static RuntimeException csvImportExceptionWrapped(String databaseName, Exception e, String importType) {
        int relevantThrowableIndex;
        // List of common errors that can be explained to the user
        if (DuplicateInputIdException.class.equals(e.getClass())) {
            return new CsvImportException(
                    "Duplicate input ids that would otherwise clash can be put into separate id space.", e);
        } else if (MissingRelationshipDataException.class.equals(e.getClass())) {
            return new CsvImportException("Relationship missing mandatory field", e);
        } else if (DirectoryNotEmptyException.class.equals(e.getClass())) {
            return new CsvImportException(
                    "Database already exist. Re-run with `--overwrite-destination` to remove the database prior to import",
                    e);
        } else if (FileLockException.class.equals(e.getClass())) {
            String string =
                    "%s can only be run against a database which is offline. The current state of database '%s' is online."
                            .formatted(importType, databaseName);
            return new CsvImportException(string, e);
        } else if ((relevantThrowableIndex = indexOfType(e, InputException.class)) != -1) {
            InputException ie = (InputException) getThrowableList(e).get(relevantThrowableIndex);
            // Provide extra hint if causal chain contains IllegalMultilineFieldException (which is wrapped because it
            // comes from the csv component, which has no access to InputException).
            String message = indexOfThrowable(e, IllegalMultilineFieldException.class) != -1
                    ? format("%s%n%n%s", MULTILINE_HINT, ie.getMessage())
                    : ie.getMessage();
            return new CsvImportException(message, ie);
        } else if (e instanceof UnsupportedFormatException ufe) {
            return ufe;
        } else if (e instanceof ImportValidationException ive) {
            return new CsvImportException("The import failed some validation.", ive);
        }
        return new CsvImportException(e); // throw in order to have process exit with !0
    }

    static class CsvImportException extends RuntimeException {
        CsvImportException(Throwable cause) {
            super(cause);
        }

        CsvImportException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private void printOverview(boolean isDryRun) {
        stdOut.println("Neo4j version: " + Version.getNeo4jVersion());
        if (isDryRun) {
            stdOut.println("Checking the contents of the following files:");
        } else {
            stdOut.println("Importing the contents of these files into " + databaseLayout.databaseDirectory() + ":");
        }
        printInputFiles("Nodes", nodeFiles, stdOut);
        printInputFiles("Relationships", relationshipFiles, stdOut);
        stdOut.println();
        stdOut.println("Available resources:");
        printIndented("Total machine memory: " + bytesToString(OsBeanUtil.getTotalMemory()), stdOut);
        printIndented("Free machine memory: " + bytesToString(OsBeanUtil.getFreeMemory()), stdOut);
        printIndented("Max heap memory : " + bytesToString(Runtime.getRuntime().maxMemory()), stdOut);
        printIndented("Max worker threads: " + importConfig.maxNumberOfWorkerThreads(), stdOut);
        printIndented("Configured max memory: " + bytesToString(importConfig.maxOffHeapMemory()), stdOut);
        printIndented("High parallel IO: " + importConfig.highIO(), stdOut);
        stdOut.println();
    }

    private static void printInputFiles(
            String name, Map<?, List<FileGroup>> inputFileGroupsByAdditionalLabels, PrintStream out) {
        if (inputFileGroupsByAdditionalLabels.isEmpty()) {
            return;
        }

        out.println(name + ":");

        inputFileGroupsByAdditionalLabels.forEach((additionalLabels, fileGroups) -> {
            if (!isEmptyKey(additionalLabels)) {
                printIndented(additionalLabels + ":", out);
            }

            for (FileGroup fileGroup : fileGroups) {
                for (FileGroup.NumberedFile file : fileGroup.files()) {
                    printIndented(StorageUtils.toString(file.path()), out);
                }
            }
            out.println();
        });
    }

    private static boolean isEmptyKey(Object k) {
        return switch (k) {
            case null -> true;
            case String s -> s.isEmpty();
            case Set<?> set -> set.isEmpty();
            default -> false;
        };
    }

    private static void printIndented(Object value, PrintStream out) {
        out.println("  " + value);
    }

    private Iterable<DataFactory> relationshipData() {
        final var result = new ArrayList<DataFactory>();
        relationshipFiles.forEach((defaultTypeName, fileGroups) -> {
            final var decorator = defaultRelationshipType(defaultTypeName);
            for (FileGroup fileGroup : fileGroups) {
                final var data =
                        data(decorator, inputEncoding, fileGroup.streamPaths().toArray(Path[]::new));
                result.add(data);
            }
        });
        return result;
    }

    private Iterable<DataFactory> nodeData() {
        final var result = new ArrayList<DataFactory>();
        nodeFiles.forEach((labels, fileGroups) -> {
            final var decorator = labels.isEmpty() ? NO_DECORATOR : additiveLabels(labels.toArray(new String[0]));
            for (FileGroup fileGroup : fileGroups) {
                final var data =
                        data(decorator, inputEncoding, fileGroup.streamPaths().toArray(Path[]::new));
                result.add(data);
            }
        });
        return result;
    }

    private Collector getBadCollector() throws IOException {
        return BadCollector.create(
                ProblemReporters.jsonOutputProblemHandler(reportOutputStream.get()),
                badTolerance,
                BadCollector.collectFlag(
                        skipBadRelationships,
                        skipDuplicateNodes,
                        ignoreExtraColumns,
                        SchemaCommandSource.mayHaveCommands(schemaCommands)),
                skipBadEntriesLogging);
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum FileInputType {
        CSV,
        PARQUET,
        NO_INPUT
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class Builder {
        private DatabaseLayout databaseLayout;
        private Config databaseConfig;
        private StorageEngineFactory storageEngineFactory;
        private org.neo4j.csv.reader.Configuration csvConfig = org.neo4j.csv.reader.Configuration.COMMAS;
        private Configuration importConfig = Configuration.DEFAULT;
        private ThrowingSupplier<OutputStream, IOException> reportOutputStream;
        private IdType defaultIdType = IdType.STRING;
        private Charset inputEncoding = StandardCharsets.UTF_8;
        private boolean ignoreExtraColumns;
        private boolean skipBadRelationships;
        private boolean skipDuplicateNodes;
        private boolean skipBadEntriesLogging;
        private long badTolerance;
        private boolean normalizeTypes;
        private boolean verbose;
        private boolean autoSkipHeaders;
        private final Map<Set<String>, List<FileGroup>> nodeFiles = new HashMap<>();
        private final Map<String, List<FileGroup>> relationshipFiles = new HashMap<>();
        private FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        private CursorContextFactory contextFactory =
                new CursorContextFactory(pageCacheTracer, new FixedVersionContextSupplier(BASE_TX_ID));
        private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
        private PrintStream stdOut = System.out;
        private PrintStream stdErr = System.err;
        private boolean force;
        private InternalLogProvider logProvider = NullLogProvider.getInstance();
        private SchemaCommandSource schemaCommands = ResolvedSchemaCommands.of();
        private FileInputType fileInputType = FileInputType.CSV;
        private ShardingArguments shardingArguments;
        private Monitor monitor = Monitor.NO_MONITOR;
        private int globalFileIdCounter = 0;

        /**
         * Wraps a path array in a file group and assign each file in the group a global id.
         */
        public FileGroup toFileGroup(Path[] paths) {
            FileGroup.NumberedFile[] numberedFiles = new FileGroup.NumberedFile[paths.length];
            for (int i = 0; i < paths.length; i++) {
                numberedFiles[i] = new FileGroup.NumberedFile(globalFileIdCounter++, paths[i]);
            }
            return new FileGroup(numberedFiles);
        }

        public Builder withDatabaseLayout(DatabaseLayout databaseLayout) {
            this.databaseLayout = databaseLayout;
            return this;
        }

        public DatabaseLayout getDatabaseLayout() {
            return databaseLayout;
        }

        public Builder withDatabaseConfig(Config databaseConfig) {
            this.databaseConfig = databaseConfig;
            return this;
        }

        public Config getDatabaseConfig() {
            return databaseConfig;
        }

        public Builder withStorageEngineFactory(StorageEngineFactory storageEngineFactory) {
            this.storageEngineFactory = storageEngineFactory;
            return this;
        }

        public Builder withCsvConfig(org.neo4j.csv.reader.Configuration csvConfig) {
            this.csvConfig = csvConfig;
            return this;
        }

        public Builder withImportConfig(Configuration importConfig) {
            this.importConfig = importConfig;
            return this;
        }

        public Builder withReportOutputStream(ThrowingSupplier<OutputStream, IOException> reportOutputStream) {
            this.reportOutputStream = reportOutputStream;
            return this;
        }

        public Builder withReportFile(Path reportFile) {
            this.reportOutputStream = () -> fileSystem.openAsOutputStream(reportFile, false);
            return this;
        }

        public Builder withDefaultIdType(IdType idType) {
            this.defaultIdType = idType;
            return this;
        }

        public Builder withInputEncoding(Charset inputEncoding) {
            this.inputEncoding = inputEncoding;
            return this;
        }

        public Builder withIgnoreExtraColumns(boolean ignoreExtraColumns) {
            this.ignoreExtraColumns = ignoreExtraColumns;
            return this;
        }

        public Builder withSkipBadRelationships(boolean skipBadRelationships) {
            this.skipBadRelationships = skipBadRelationships;
            return this;
        }

        public Builder withSkipDuplicateNodes(boolean skipDuplicateNodes) {
            this.skipDuplicateNodes = skipDuplicateNodes;
            return this;
        }

        public Builder withSkipBadEntriesLogging(boolean skipBadEntriesLogging) {
            this.skipBadEntriesLogging = skipBadEntriesLogging;
            return this;
        }

        public Builder withBadTolerance(long badTolerance) {
            this.badTolerance = badTolerance;
            return this;
        }

        public Builder withNormalizeTypes(boolean normalizeTypes) {
            this.normalizeTypes = normalizeTypes;
            return this;
        }

        public Builder withVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public Builder withAutoSkipHeaders(boolean autoSkipHeaders) {
            this.autoSkipHeaders = autoSkipHeaders;
            return this;
        }

        public Builder addNodeFiles(Set<String> labels, FileGroup fileGroup) {
            final var list = nodeFiles.computeIfAbsent(labels, unused -> new ArrayList<>());
            list.add(fileGroup);
            return this;
        }

        public Builder addRelationshipFiles(String defaultRelType, FileGroup fileGroup) {
            final var list = relationshipFiles.computeIfAbsent(defaultRelType, unused -> new ArrayList<>());
            list.add(fileGroup);
            return this;
        }

        public Builder withFileSystem(FileSystemAbstraction fileSystem) {
            this.fileSystem = fileSystem;
            return this;
        }

        public FileSystemAbstraction getFileSystem() {
            return fileSystem;
        }

        public Builder withPageCacheTracer(PageCacheTracer pageCacheTracer) {
            this.pageCacheTracer = pageCacheTracer;
            return this;
        }

        public Builder withCursorContextFactory(CursorContextFactory contextFactory) {
            this.contextFactory = contextFactory;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder withMemoryTracker(MemoryTracker memoryTracker) {
            this.memoryTracker = memoryTracker;
            return this;
        }

        public Builder withStdOut(PrintStream stdOut) {
            this.stdOut = stdOut;
            return this;
        }

        public Builder withStdErr(PrintStream stdErr) {
            this.stdErr = stdErr;
            return this;
        }

        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        public boolean isForce() {
            return force;
        }

        public Builder withLogProvider(InternalLogProvider logProvider) {
            this.logProvider = logProvider;
            return this;
        }

        public Builder withSchemaCommands(SchemaCommandSource schemaCommands) {
            this.schemaCommands = requireNonNull(schemaCommands);
            return this;
        }

        public Builder withFileInputType(FileInputType fileInputType) {
            this.fileInputType = fileInputType;
            return this;
        }

        public Builder withShardingArguments(ShardingArguments shardingArguments) {
            this.shardingArguments = shardingArguments;
            return this;
        }

        public Builder withMonitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public FileImporter build() {
            return new FileImporter(this);
        }
    }
}
