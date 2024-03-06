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

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.duplication_user_messages;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.server_logging_config_path;
import static org.neo4j.internal.batchimport.input.Collectors.badCollector;
import static org.neo4j.internal.batchimport.input.Collectors.collect;
import static org.neo4j.internal.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.data;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;
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
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.MissingRelationshipDataException;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.PrefixedLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.Preconditions;

class CsvImporter implements Importer {
    static final String DEFAULT_REPORT_FILE_NAME = "import.report";

    private final DatabaseLayout databaseLayout;
    private final Config databaseConfig;
    private final org.neo4j.csv.reader.Configuration csvConfig;
    private final org.neo4j.internal.batchimport.Configuration importConfig;
    private final Path reportFile;
    private final IdType idType;
    private final Charset inputEncoding;
    private final boolean ignoreExtraColumns;
    private final boolean skipBadRelationships;
    private final boolean skipDuplicateNodes;
    private final boolean skipBadEntriesLogging;
    private final long badTolerance;
    private final boolean normalizeTypes;
    private final boolean verbose;
    private final boolean autoSkipHeaders;
    private final Map<Set<String>, List<Path[]>> nodeFiles;
    private final Map<String, List<Path[]>> relationshipFiles;
    private final FileSystemAbstraction fileSystem;
    private final PrintStream stdOut;
    private final PrintStream stdErr;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final boolean force;
    private final IncrementalStage incrementalStage;
    private final boolean incremental;
    private final InternalLogProvider logProvider;

    private CsvImporter(Builder b) {
        this.databaseLayout = requireNonNull(b.databaseLayout);
        this.databaseConfig = requireNonNull(b.databaseConfig);
        this.csvConfig = requireNonNull(b.csvConfig);
        this.importConfig = requireNonNull(b.importConfig);
        this.reportFile = requireNonNull(b.reportFile);
        this.idType = requireNonNull(b.idType);
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
        this.incremental = b.incremental;
        this.incrementalStage = b.incrementalStage;
    }

    @Override
    public void doImport() throws IOException {
        if (force) {
            fileSystem.deleteRecursively(
                    databaseLayout.databaseDirectory(), path -> !path.equals(databaseLayout.databaseLockFile()));
            fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());
        }

        try (OutputStream badOutput = fileSystem.openAsOutputStream(reportFile, false);
                Collector badCollector = getBadCollector(skipBadEntriesLogging, badOutput)) {
            // Extract the default time zone from the database configuration
            ZoneId dbTimeZone = databaseConfig.get(db_temporal_timezone);
            Supplier<ZoneId> defaultTimeZone = () -> dbTimeZone;

            final var nodeData = nodeData();
            final var relationshipsData = relationshipData();

            try (CsvInput input = new CsvInput(
                    nodeData,
                    defaultFormatNodeFileHeader(defaultTimeZone, normalizeTypes),
                    relationshipsData,
                    defaultFormatRelationshipFileHeader(defaultTimeZone, normalizeTypes),
                    idType,
                    csvConfig,
                    autoSkipHeaders,
                    new CsvInput.PrintingMonitor(stdOut),
                    memoryTracker)) {
                doImport(input, badCollector);
            }
        }
    }

    private void doImport(Input input, Collector badCollector) {
        boolean success = false;

        printOverview();

        try (JobScheduler jobScheduler = createInitialisedScheduler()) {
            // Let the storage engine factory be configurable in the tool later on...
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine(databaseConfig);
            var logService = new SimpleLogService(
                    NullLogProvider.getInstance(),
                    new PrefixedLogProvider(logProvider, databaseLayout.getDatabaseName()),
                    databaseConfig.get(duplication_user_messages));
            if (incremental) {
                try (Lifespan life = new Lifespan()) {
                    var indexProviders = life.add(new DefaultIndexProvidersAccess(
                            storageEngineFactory,
                            fileSystem,
                            databaseConfig,
                            jobScheduler,
                            new SimpleLogService(logProvider),
                            pageCacheTracer,
                            contextFactory));
                    var importer = storageEngineFactory.incrementalBatchImporter(
                            databaseLayout,
                            fileSystem,
                            pageCacheTracer,
                            importConfig,
                            logService,
                            stdOut,
                            verbose,
                            AdditionalInitialIds.EMPTY,
                            () -> readLogTailMetaData(storageEngineFactory),
                            databaseConfig,
                            new PrintingImportLogicMonitor(stdOut, stdErr),
                            jobScheduler,
                            badCollector,
                            TransactionLogInitializer.getLogFilesInitializer(),
                            new IndexImporterFactoryImpl(),
                            memoryTracker,
                            contextFactory,
                            indexProviders);
                    switch (incrementalStage) {
                        case prepare -> importer.prepare(input);
                        case build -> importer.build(input);
                        case merge -> importer.merge();
                        case all -> {
                            importer.prepare(input);
                            importer.build(input);
                            importer.merge();
                        }
                        default -> throw new IllegalArgumentException("Unknown import mode " + incrementalStage);
                    }
                }
            } else {
                BatchImporter importer = storageEngineFactory.batchImporter(
                        databaseLayout,
                        fileSystem,
                        pageCacheTracer,
                        importConfig,
                        logService,
                        stdOut,
                        verbose,
                        AdditionalInitialIds.EMPTY,
                        databaseConfig,
                        new PrintingImportLogicMonitor(stdOut, stdErr),
                        jobScheduler,
                        badCollector,
                        TransactionLogInitializer.getLogFilesInitializer(),
                        new IndexImporterFactoryImpl(),
                        memoryTracker,
                        contextFactory);

                importer.doImport(input);
            }
            success = true;
        } catch (Exception ex) {
            throw andPrintError(databaseLayout.getDatabaseName(), ex, incremental, stdErr);
        } finally {
            long numberOfBadEntries = badCollector.badEntries();
            if (badTolerance != BadCollector.UNLIMITED_TOLERANCE && numberOfBadEntries > badTolerance) {
                stdOut.println("Neo4j-admin aborted the import because " + numberOfBadEntries + " bad entries were "
                        + "found, which exceeds the set fault tolerance ("
                        + badTolerance + "). Import is optimized to import fault-free data.");
                stdOut.println();
                if (skipBadEntriesLogging) {
                    stdOut.println(
                            "Bad entry logging is disabled, enable it using --skip-bad-entries-logging=false" + ".");
                } else {
                    stdOut.println("Bad entries were logged to " + reportFile.toAbsolutePath() + ".");
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
                        + " are left as they are, although they are likely in an unusable state. "
                        + "Starting a database on these store files will likely fail or observe inconsistent records so "
                        + "start at your own risk or delete the store manually.");
                stdOut.println();
            }
        }
    }

    private LogTailMetadata readLogTailMetaData(StorageEngineFactory storageEngineFactory) throws IOException {
        return LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fileSystem)
                .withStorageEngineFactory(storageEngineFactory)
                .build()
                .getTailMetadata();
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     *
     * @param databaseName the name of the database to receive the import data
     * @param e            the error that occurred
     * @param incremental  whether the import is incremental
     * @param err          the error output stream
     */
    private static RuntimeException andPrintError(
            String databaseName, Exception e, boolean incremental, PrintStream err) {
        // List of common errors that can be explained to the user
        if (DuplicateInputIdException.class.equals(e.getClass())) {
            err.println("Duplicate input ids that would otherwise clash can be put into separate id space.");
        } else if (MissingRelationshipDataException.class.equals(e.getClass())) {
            err.println("Relationship missing mandatory field");
        } else if (DirectoryNotEmptyException.class.equals(e.getClass())) {
            err.println(
                    "Database already exist. Re-run with `--overwrite-destination` to remove the database prior to import");
        } else if (FileLockException.class.equals(e.getClass())) {
            String string =
                    "%s can only be run against a database which is offline. The current state of database '%s' is online."
                            .formatted(incremental ? "Incremental import" : "Import", databaseName);
            err.println(string);
        }
        // This type of exception is wrapped since our input code throws InputException consistently,
        // and so IllegalMultilineFieldException comes from the csv component, which has no access to InputException
        // therefore it's wrapped.
        else if (indexOfThrowable(e, IllegalMultilineFieldException.class) != -1) {
            err.println("Detected field which spanned multiple lines for an import where "
                    + "--multiline-fields=false. If you know that your input data "
                    + "include fields containing new-line characters then import with this option set to "
                    + "true.");
        } else if (indexOfThrowable(e, InputException.class) != -1) {
            err.println("Error in input data");
        }
        err.println();

        return new CsvImportException(e); // throw in order to have process exit with !0
    }

    static class CsvImportException extends RuntimeException {
        CsvImportException(Throwable cause) {
            super(cause);
        }
    }

    private void printOverview() {
        stdOut.println("Neo4j version: " + Version.getNeo4jVersion());
        stdOut.println("Importing the contents of these files into " + databaseLayout.databaseDirectory() + ":");
        if (incrementalStage != null) {
            stdOut.println("Import mode: " + incrementalStage);
        }
        printInputFiles("Nodes", nodeFiles, stdOut);
        printInputFiles("Relationships", relationshipFiles, stdOut);
        stdOut.println();
        stdOut.println("Available resources:");
        printIndented("Total machine memory: " + bytesToString(OsBeanUtil.getTotalPhysicalMemory()), stdOut);
        printIndented("Free machine memory: " + bytesToString(OsBeanUtil.getFreePhysicalMemory()), stdOut);
        printIndented("Max heap memory : " + bytesToString(Runtime.getRuntime().maxMemory()), stdOut);
        printIndented("Max worker threads: " + importConfig.maxNumberOfWorkerThreads(), stdOut);
        printIndented("Configured max memory: " + bytesToString(importConfig.maxOffHeapMemory()), stdOut);
        printIndented("High parallel IO: " + importConfig.highIO(), stdOut);
        stdOut.println();
    }

    private static void printInputFiles(String name, Map<?, List<Path[]>> inputFiles, PrintStream out) {
        if (inputFiles.isEmpty()) {
            return;
        }

        out.println(name + ":");

        inputFiles.forEach((k, files) -> {
            if (!isEmptyKey(k)) {
                printIndented(k + ":", out);
            }

            for (Path[] arr : files) {
                for (final Path file : arr) {
                    printIndented(file, out);
                }
            }
            out.println();
        });
    }

    private static boolean isEmptyKey(Object k) {
        if (k instanceof String) {
            return ((String) k).isEmpty();
        } else if (k instanceof Set) {
            return ((Set<?>) k).isEmpty();
        }
        return false;
    }

    private static void printIndented(Object value, PrintStream out) {
        out.println("  " + value);
    }

    private Iterable<DataFactory> relationshipData() {
        final var result = new ArrayList<DataFactory>();
        relationshipFiles.forEach((defaultTypeName, fileSets) -> {
            final var decorator = defaultRelationshipType(defaultTypeName);
            for (Path[] files : fileSets) {
                final var data = data(decorator, inputEncoding, files);
                result.add(data);
            }
        });
        return result;
    }

    private Iterable<DataFactory> nodeData() {
        final var result = new ArrayList<DataFactory>();
        nodeFiles.forEach((labels, fileSets) -> {
            final var decorator = labels.isEmpty() ? NO_DECORATOR : additiveLabels(labels.toArray(new String[0]));
            for (Path[] files : fileSets) {
                final var data = data(decorator, inputEncoding, files);
                result.add(data);
            }
        });
        return result;
    }

    private Collector getBadCollector(boolean skipBadEntriesLogging, OutputStream badOutput) {
        return skipBadEntriesLogging
                ? silentBadCollector(badTolerance)
                : badCollector(
                        badOutput, badTolerance, collect(skipBadRelationships, skipDuplicateNodes, ignoreExtraColumns));
    }

    static InternalLogProvider createLogProvider(FileSystemAbstraction fileSystem, Config databaseConfig) {
        return new Log4jLogProvider(createLoggerFromXmlConfig(
                fileSystem,
                databaseConfig.get(server_logging_config_path),
                !databaseConfig.isExplicitlySet(server_logging_config_path),
                databaseConfig::configStringLookup));
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private DatabaseLayout databaseLayout;
        private Config databaseConfig;
        private org.neo4j.csv.reader.Configuration csvConfig = org.neo4j.csv.reader.Configuration.COMMAS;
        private Configuration importConfig = Configuration.DEFAULT;
        private Path reportFile;
        private IdType idType = IdType.STRING;
        private Charset inputEncoding = StandardCharsets.UTF_8;
        private boolean ignoreExtraColumns;
        private boolean skipBadRelationships;
        private boolean skipDuplicateNodes;
        private boolean skipBadEntriesLogging;
        private long badTolerance;
        private boolean normalizeTypes;
        private boolean verbose;
        private boolean autoSkipHeaders;
        private final Map<Set<String>, List<Path[]>> nodeFiles = new HashMap<>();
        private final Map<String, List<Path[]>> relationshipFiles = new HashMap<>();
        private FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        private CursorContextFactory contextFactory =
                new CursorContextFactory(pageCacheTracer, new FixedVersionContextSupplier(BASE_TX_ID));
        private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
        private PrintStream stdOut = System.out;
        private PrintStream stdErr = System.err;
        private boolean force;
        private boolean incremental = false;
        private IncrementalStage incrementalStage = null;
        private InternalLogProvider logProvider = NullLogProvider.getInstance();

        Builder withDatabaseLayout(DatabaseLayout databaseLayout) {
            this.databaseLayout = databaseLayout;
            return this;
        }

        Builder withDatabaseConfig(Config databaseConfig) {
            this.databaseConfig = databaseConfig;
            return this;
        }

        Builder withCsvConfig(org.neo4j.csv.reader.Configuration csvConfig) {
            this.csvConfig = csvConfig;
            return this;
        }

        Builder withImportConfig(Configuration importConfig) {
            this.importConfig = importConfig;
            return this;
        }

        Builder withReportFile(Path reportFile) {
            this.reportFile = reportFile;
            return this;
        }

        Builder withIdType(IdType idType) {
            this.idType = idType;
            return this;
        }

        Builder withInputEncoding(Charset inputEncoding) {
            this.inputEncoding = inputEncoding;
            return this;
        }

        Builder withIgnoreExtraColumns(boolean ignoreExtraColumns) {
            this.ignoreExtraColumns = ignoreExtraColumns;
            return this;
        }

        Builder withSkipBadRelationships(boolean skipBadRelationships) {
            this.skipBadRelationships = skipBadRelationships;
            return this;
        }

        Builder withSkipDuplicateNodes(boolean skipDuplicateNodes) {
            this.skipDuplicateNodes = skipDuplicateNodes;
            return this;
        }

        Builder withSkipBadEntriesLogging(boolean skipBadEntriesLogging) {
            this.skipBadEntriesLogging = skipBadEntriesLogging;
            return this;
        }

        Builder withBadTolerance(long badTolerance) {
            this.badTolerance = badTolerance;
            return this;
        }

        Builder withNormalizeTypes(boolean normalizeTypes) {
            this.normalizeTypes = normalizeTypes;
            return this;
        }

        Builder withVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        Builder withAutoSkipHeaders(boolean autoSkipHeaders) {
            this.autoSkipHeaders = autoSkipHeaders;
            return this;
        }

        Builder addNodeFiles(Set<String> labels, Path[] files) {
            final var list = nodeFiles.computeIfAbsent(labels, unused -> new ArrayList<>());
            list.add(files);
            return this;
        }

        Builder addRelationshipFiles(String defaultRelType, Path[] files) {
            final var list = relationshipFiles.computeIfAbsent(defaultRelType, unused -> new ArrayList<>());
            list.add(files);
            return this;
        }

        Builder withFileSystem(FileSystemAbstraction fileSystem) {
            this.fileSystem = fileSystem;
            return this;
        }

        Builder withPageCacheTracer(PageCacheTracer pageCacheTracer) {
            this.pageCacheTracer = pageCacheTracer;
            return this;
        }

        Builder withCursorContextFactory(CursorContextFactory contextFactory) {
            this.contextFactory = contextFactory;
            return this;
        }

        Builder withMemoryTracker(MemoryTracker memoryTracker) {
            this.memoryTracker = memoryTracker;
            return this;
        }

        Builder withStdOut(PrintStream stdOut) {
            this.stdOut = stdOut;
            return this;
        }

        Builder withStdErr(PrintStream stdErr) {
            this.stdErr = stdErr;
            return this;
        }

        Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        Builder withIncremental(boolean incremental) {
            this.incremental = incremental;
            return this;
        }

        Builder withIncrementalStage(IncrementalStage mode) {
            this.incrementalStage = mode;
            return this;
        }

        Builder withLogProvider(InternalLogProvider logProvider) {
            this.logProvider = logProvider;
            return this;
        }

        CsvImporter build() {
            Preconditions.checkState(
                    !(force && incremental),
                    "--overwrite-destination doesn't work with incremental import",
                    incrementalStage);
            return new CsvImporter(this);
        }
    }

    enum IncrementalStage {
        /**
         * Prepares an incremental import. This requires target database to be offline.
         */
        prepare,
        /**
         * Builds the incremental import. The is disjoint from the target database state.
         */
        build,
        /**
         * Merges the incremental import into the target database. This requires target database to be offline.
         */
        merge,
        /**
         * Performs a full incremental import including all steps involved.
         */
        all
    }
}
