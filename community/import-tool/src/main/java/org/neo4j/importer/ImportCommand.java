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

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.importer.CsvImporter.DEFAULT_REPORT_FILE_NAME;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.storageengine.api.StorageEngineFactory.SELECTOR;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Help.Visibility.NEVER;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.ByteUnitConverter;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.Converters.MaxOffHeapMemoryConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.importer.CsvImporter.CsvImportException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "import",
        description = "High-speed import of data from CSV files, optimized for fault-free data.",
        subcommands = {ImportCommand.Full.class, CommandLine.HelpCommand.class})
@SuppressWarnings("FieldMayBeFinal")
public class ImportCommand {
    /**
     * Arguments and logic shared between Full and Incremental import commands.
     */
    protected abstract static class Base extends AbstractAdminCommand {
        /**
         * Delimiter used between files in an input group.
         */
        private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();

        private static final org.neo4j.csv.reader.Configuration DEFAULT_CSV_CONFIG = COMMAS;
        private static final Configuration DEFAULT_IMPORTER_CONFIG = DEFAULT;

        private enum OnOffAuto {
            ON,
            OFF,
            AUTO
        }

        static class OnOffAutoConverter implements ITypeConverter<OnOffAuto> {
            @Override
            public OnOffAuto convert(String value) throws Exception {
                return OnOffAuto.valueOf(value.toUpperCase(Locale.ROOT));
            }
        }

        @Parameters(
                index = "0",
                converter = DatabaseNameConverter.class,
                defaultValue = DEFAULT_DATABASE_NAME,
                description = "Name of the database to import.%n"
                        + "  If the database into which you import does not exist prior to importing,%n"
                        + "  you must create it subsequently using CREATE DATABASE.")
        private NormalizedDatabaseName database;

        @Option(
                names = "--report-file",
                paramLabel = "<path>",
                defaultValue = DEFAULT_REPORT_FILE_NAME,
                description = "File in which to store the report of the csv-import.")
        private Path reportFile = Path.of(DEFAULT_REPORT_FILE_NAME);

        @Option(
                names = "--id-type",
                paramLabel = "string|integer|actual",
                defaultValue = "string",
                description = "Each node must provide a unique ID. This is used to find the "
                        + "correct nodes when creating relationships. Possible values are:%n"
                        + "  string: arbitrary strings for identifying nodes,%n"
                        + "  integer: arbitrary integer values for identifying nodes,%n"
                        + "  actual: (advanced) actual node IDs.%n"
                        + "For more information on ID handling, please see the Neo4j Manual: "
                        + "https://neo4j.com/docs/operations-manual/current/tools/import/",
                converter = IdTypeConverter.class)
        IdType idType = IdType.STRING;

        @Option(
                names = "--input-encoding",
                paramLabel = "<character-set>",
                description = "Character set that input data is encoded in.")
        private Charset inputEncoding = StandardCharsets.UTF_8;

        @Option(
                names = "--ignore-extra-columns",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "If unspecified columns should be ignored during the import.")
        private boolean ignoreExtraColumns;

        private static final String MULTILINE_FIELDS = "--multiline-fields";

        @Option(
                names = MULTILINE_FIELDS,
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not fields from an input source can span multiple lines, i.e. contain newline characters. "
                                + "Setting " + MULTILINE_FIELDS + "=true can severely degrade the performance of "
                                + "the importer. Therefore, use it with care, especially with large imports.")
        private boolean multilineFields = DEFAULT_CSV_CONFIG.multilineFields();

        @Option(
                names = "--ignore-empty-strings",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null.")
        private boolean ignoreEmptyStrings = DEFAULT_CSV_CONFIG.emptyQuotedStringsAsNull();

        @Option(
                names = "--trim-strings",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Whether or not strings should be trimmed for whitespaces.")
        private boolean trimStrings = DEFAULT_CSV_CONFIG.trimStrings();

        @Option(
                names = "--legacy-style-quoting",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Whether or not a backslash-escaped quote e.g. \\\" is interpreted as an inner quote.")
        private boolean legacyStyleQuoting = DEFAULT_CSV_CONFIG.legacyStyleQuoting();

        @Option(
                names = "--delimiter",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description = "Delimiter character between values in CSV data. "
                        + "Also accepts 'TAB' and e.g. 'U+20AC' for specifying a character using Unicode.")
        private char delimiter = DEFAULT_CSV_CONFIG.delimiter();

        @Option(
                names = "--array-delimiter",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description = "Delimiter character between array elements within a value in CSV data. "
                        + "Also accepts 'TAB' and e.g. 'U+20AC' for specifying a character using Unicode.")
        private char arrayDelimiter = DEFAULT_CSV_CONFIG.arrayDelimiter();

        @Option(
                names = "--quote",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description =
                        "Character to treat as quotation character for values in CSV data. Quotes can be escaped as per RFC 4180 by doubling them, "
                                + "for example \"\" would be interpreted as a literal \". You cannot escape using \\.")
        private char quote = DEFAULT_CSV_CONFIG.quotationCharacter();

        @Option(
                names = "--read-buffer-size",
                paramLabel = "<size>",
                converter = ByteUnitConverter.class,
                description = "Size of each buffer for reading input data. "
                        + "It has to be at least large enough to hold the biggest single value in the input data. "
                        + "The value can be a plain number or a byte units string, e.g. 128k, 1m.")
        private long bufferSize = DEFAULT_CSV_CONFIG.bufferSize();

        @Option(
                names = "--max-off-heap-memory",
                paramLabel = "<size>",
                defaultValue = "90%",
                converter = MaxOffHeapMemoryConverter.class,
                description =
                        "Maximum memory that neo4j-admin can use for various data structures and caching to improve performance. "
                                + "Values can be plain numbers, such as 10000000, or 20G for 20 gigabytes. "
                                + "It can also be specified as a percentage of the available memory, for example 70%%.")
        private long maxOffHeapMemory;

        @Option(
                names = "--high-parallel-io",
                showDefaultValue = ALWAYS,
                paramLabel = "on|off|auto",
                defaultValue = "auto",
                converter = OnOffAutoConverter.class,
                description =
                        "Ignore environment-based heuristics and indicate if the target storage subsystem can support parallel IO with high throughput or auto detect. "
                                + " Typically this is on for SSDs, large raid arrays, and network-attached storage.")
        private OnOffAuto highIo;

        @Option(
                names = "--threads",
                paramLabel = "<num>",
                description =
                        "(advanced) Max number of worker threads used by the importer. Defaults to the number of available processors reported by the JVM. "
                                + "There is a certain amount of minimum threads needed so for that reason there is no lower bound for this "
                                + "value. For optimal performance, this value should not be greater than the number of available processors.")
        private int threads = DEFAULT_IMPORTER_CONFIG.maxNumberOfWorkerThreads();

        private static final String BAD_TOLERANCE_OPTION = "--bad-tolerance";

        @Option(
                names = BAD_TOLERANCE_OPTION,
                paramLabel = "<num>",
                description =
                        "Number of bad entries before the import is aborted. The import process is optimized for error-free data. "
                                + "Therefore, cleaning the data before importing it is highly recommended. If you encounter any bad entries during "
                                + "the import process, you can set the number of bad entries to a specific value that suits your needs. "
                                + "However, setting a high value may affect the performance of the tool.")
        private long badTolerance = 1000;

        public static final String SKIP_BAD_ENTRIES_LOGGING = "--skip-bad-entries-logging";

        @Option(
                names = SKIP_BAD_ENTRIES_LOGGING,
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "When set to `true`, the details of bad entries are not written in the log. Disabling logging "
                                + "can improve performance when the data contains lots of faults. Cleaning the data before importing "
                                + "it is highly recommended because faults dramatically affect the tool's performance even without "
                                + "logging.")
        private boolean skipBadEntriesLogging;

        @Option(
                names = "--skip-bad-relationships",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not to skip importing relationships that refer to missing node IDs, i.e. either start or end node ID/group referring "
                                + "to a node that was not specified by the node input data. Skipped relationships will be logged, containing at most the number of entities "
                                + "specified by " + BAD_TOLERANCE_OPTION + ", unless otherwise specified by the "
                                + SKIP_BAD_ENTRIES_LOGGING + " option.")
        private boolean skipBadRelationships;

        @Option(
                names = "--strict",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                description =
                        "Whether or not the lookup of nodes referred to from relationships needs to be checked strict. "
                                + "If disabled, most but not all relationships referring to non-existent nodes will be detected. "
                                + "If enabled all those relationships will be found but at the cost of lower performance.")
        private boolean strict = false;

        @Option(
                names = "--skip-duplicate-nodes",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not to skip importing nodes that have the same ID/group. In the event of multiple nodes within the same group having "
                                + "the same ID, the first encountered will be imported, whereas consecutive such nodes will be skipped. Skipped nodes will be logged, "
                                + "containing at most the number of entities specified by " + BAD_TOLERANCE_OPTION
                                + ", unless otherwise specified by the " + SKIP_BAD_ENTRIES_LOGGING + " option.")
        private boolean skipDuplicateNodes;

        @Option(
                names = "--normalize-types",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "When `true`, non-array property values are converted to their equivalent Cypher types. "
                        + "For example, all integer values will be converted to 64-bit long integers.")
        private boolean normalizeTypes = true;

        @Option(
                names = "--nodes",
                required = true,
                arity = "1..*",
                converter = NodeFilesConverter.class,
                paramLabel = "[<label>[:<label>]...=]<files>",
                description =
                        "Node CSV header and data. Multiple files will be logically seen as one big file from the perspective of the importer. The first "
                                + "line must contain the header. Multiple data sources like these can be specified in one import, where each data source has its "
                                + "own header. Files can also be specified using regular expressions.")
        private List<NodeFilesGroup> nodes;

        @Option(
                names = "--relationships",
                arity = "1..*",
                converter = RelationshipFilesConverter.class,
                showDefaultValue = NEVER,
                paramLabel = "[<type>=]<files>",
                description =
                        "Relationship CSV header and data. Multiple files will be logically seen as one big file from the perspective of the importer. "
                                + "The first line must contain the header. Multiple data sources like these can be specified in one import, where each data source has "
                                + "its own header. Files can also be specified using regular expressions.")
        private List<RelationshipFilesGroup> relationships = new ArrayList<>();

        @Option(
                names = "--auto-skip-subsequent-headers",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Automatically skip accidental header lines in subsequent files in file groups with more than one file.")
        private boolean autoSkipHeaders;

        protected Base(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected Optional<String> commandConfigName() {
            return Optional.of("database-import");
        }

        @FunctionalInterface
        protected interface MaybeLocker {
            Closeable maybeCheckLock(DatabaseLayout databaseLayout) throws CannotWriteException, IOException;
        }

        protected void doExecute(
                ImportType importType, String format, boolean overwriteDestination, Base.MaybeLocker maybeLockChecker) {
            try {
                final var databaseConfig = loadNeo4jConfig(format);
                DatabaseLayout databaseLayout = Neo4jLayout.of(databaseConfig).databaseLayout(database.name());

                try (var ignore = maybeLockChecker.maybeCheckLock(databaseLayout);
                        var logProvider = CsvImporter.createLogProvider(ctx.fs(), databaseConfig);
                        var fileSystem = new SchemeFileSystemAbstraction(ctx.fs(), databaseConfig, logProvider)) {
                    final var csvConfig = csvConfiguration();
                    final var importConfig = importConfiguration();

                    final var importerBuilder = CsvImporter.builder()
                            .withDatabaseLayout(databaseLayout)
                            .withDatabaseConfig(databaseConfig)
                            .withFileSystem(fileSystem)
                            .withStdOut(ctx.out())
                            .withStdErr(ctx.err())
                            .withCsvConfig(csvConfig)
                            .withImportConfig(importConfig)
                            .withIdType(idType)
                            .withInputEncoding(inputEncoding)
                            .withReportFile(reportFile.toAbsolutePath())
                            .withIgnoreExtraColumns(ignoreExtraColumns)
                            .withBadTolerance(badTolerance)
                            .withSkipBadRelationships(skipBadRelationships)
                            .withSkipDuplicateNodes(skipDuplicateNodes)
                            .withSkipBadEntriesLogging(skipBadEntriesLogging)
                            .withSkipBadRelationships(skipBadRelationships)
                            .withNormalizeTypes(normalizeTypes)
                            .withVerbose(verbose)
                            .withAutoSkipHeaders(autoSkipHeaders)
                            .withForce(overwriteDestination)
                            .withImportType(importType)
                            .withLogProvider(logProvider);
                    CursorContextFactory cursorContextFactory;
                    if (importType == ImportType.incremental) {
                        cursorContextFactory = new CursorContextFactory(
                                PageCacheTracer.NULL,
                                new FixedVersionContextSupplier(getLogTail(fileSystem, databaseLayout, databaseConfig)
                                        .getLastCommittedTransaction()
                                        .id()));
                    } else {
                        cursorContextFactory = new CursorContextFactory(
                                PageCacheTracer.NULL, new FixedVersionContextSupplier(BASE_TX_ID));
                    }
                    importerBuilder.withCursorContextFactory(cursorContextFactory);

                    for (var n : nodes) {
                        importerBuilder.addNodeFiles(n.key, n.toPaths(fileSystem));
                    }

                    for (var r : relationships) {
                        importerBuilder.addRelationshipFiles(r.key, r.toPaths(fileSystem));
                    }

                    importerBuilder.build().doImport(this);
                } catch (FileLockException e) {
                    throw new CommandFailedException(
                            "The database is in use. Stop database '%s' and try again."
                                    .formatted(databaseLayout.getDatabaseName()),
                            e,
                            ExitCode.FAIL);
                } catch (CannotWriteException e) {
                    throw new CommandFailedException("You do not have permission to import.", e, ExitCode.NOPERM);
                } catch (CsvImportException e) {
                    throw new CommandFailedException("Error importing csv file.", e, ExitCode.SOFTWARE);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        protected abstract void doImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                Config databaseConfig,
                JobScheduler jobScheduler,
                InternalLogProvider logProvider,
                PageCacheTracer pageCacheTracer,
                CursorContextFactory contextFactory,
                Configuration importConfig,
                LogService logService,
                PrintStream stdOut,
                PrintStream stdErr,
                boolean verbose,
                Collector badCollector,
                MemoryTracker memoryTracker,
                Input input)
                throws IOException;

        private LogTailMetadata getLogTail(
                FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config databaseConfig) throws IOException {
            Optional<StorageEngineFactory> storageEngineFactory = SELECTOR.selectStorageEngine(fs, databaseLayout);
            return getLogTail(fs, databaseLayout, databaseConfig, storageEngineFactory.orElseThrow());
        }

        private LogTailMetadata getLogTail(
                FileSystemAbstraction fs,
                DatabaseLayout databaseLayout,
                Config config,
                StorageEngineFactory storageEngineFactory)
                throws IOException {
            LogTailExtractor logTailExtractor = new LogTailExtractor(fs, config, storageEngineFactory, EMPTY);
            return logTailExtractor.getTailMetadata(databaseLayout, EmptyMemoryTracker.INSTANCE);
        }

        @VisibleForTesting
        Config loadNeo4jConfig(String format) {
            Config.Builder builder = createPrefilledConfigBuilder();
            if (StringUtils.isNotEmpty(format)) {
                builder.set(GraphDatabaseSettings.db_format, format);
            }
            return builder.build();
        }

        LogTailMetadata readLogTailMetaData(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                StorageEngineFactory storageEngineFactory)
                throws IOException {
            return LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fileSystem)
                    .withStorageEngineFactory(storageEngineFactory)
                    .build()
                    .getTailMetadata();
        }

        private org.neo4j.csv.reader.Configuration csvConfiguration() {
            return DEFAULT_CSV_CONFIG.toBuilder()
                    .withDelimiter(delimiter)
                    .withArrayDelimiter(arrayDelimiter)
                    .withQuotationCharacter(quote)
                    .withMultilineFields(multilineFields)
                    .withEmptyQuotedStringsAsNull(ignoreEmptyStrings)
                    .withTrimStrings(trimStrings)
                    .withLegacyStyleQuoting(legacyStyleQuoting)
                    .withBufferSize(toIntExact(bufferSize))
                    .build();
        }

        private org.neo4j.internal.batchimport.Configuration importConfiguration() {
            return new Configuration.Overridden(Configuration.defaultConfiguration()) {
                @Override
                public int maxNumberOfWorkerThreads() {
                    return threads;
                }

                @Override
                public long maxOffHeapMemory() {
                    return maxOffHeapMemory;
                }

                @Override
                public boolean highIO() {
                    // super.highIO will look at the device and make a decision
                    return highIo == OnOffAuto.AUTO ? super.highIO() : highIo == OnOffAuto.ON;
                }

                @Override
                public IndexConfig indexConfig() {
                    return IndexConfig.create().withLabelIndex().withRelationshipTypeIndex();
                }

                @Override
                public boolean strictNodeCheck() {
                    return strict;
                }
            };
        }

        static class EscapedCharacterConverter implements ITypeConverter<Character> {
            @Override
            public Character convert(String value) {
                return CHARACTER_CONVERTER.apply(value);
            }
        }

        static class NodeFilesConverter implements ITypeConverter<NodeFilesGroup> {
            @Override
            public NodeFilesGroup convert(String value) {
                try {
                    return parseNodeFilesGroup(value);
                } catch (Exception e) {
                    throw new CommandLine.TypeConversionException(format("Invalid nodes file: %s (%s)", value, e));
                }
            }
        }

        static class RelationshipFilesConverter implements ITypeConverter<InputFilesGroup<String>> {
            @Override
            public InputFilesGroup<String> convert(String value) {
                try {
                    return parseRelationshipFilesGroup(value);
                } catch (Exception e) {
                    throw new CommandLine.TypeConversionException(
                            format("Invalid relationships file: %s (%s)", value, e));
                }
            }
        }

        static class IdTypeConverter implements CommandLine.ITypeConverter<IdType> {
            @Override
            public IdType convert(String in) {
                try {
                    return IdType.valueOf(in.toUpperCase(Locale.ROOT));
                } catch (Exception e) {
                    throw new CommandLine.TypeConversionException(format("Invalid id type: %s (%s)", in, e));
                }
            }
        }
    }

    @Command(
            name = "full",
            description = "High-speed initial import of fault-free data from CSV files into a non-existent or empty "
                    + "database.")
    public static class Full extends Base {
        @Option(
                names = "--format",
                showDefaultValue = NEVER,
                required = false,
                description = "Name of database format. The imported database will be created in the specified format "
                        + "or use the format set in the configuration. Valid formats are `standard`, `aligned`, "
                        + "`high_limit`, and `block`.")
        private String format;

        // Was force
        @Option(
                names = "--overwrite-destination",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Delete any existing database files prior to the import.")
        private boolean overwriteDestination;

        public Full(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        public void execute() throws Exception {
            doExecute(ImportType.full, format, overwriteDestination, databaseLayout -> {
                // Create the db folder if it doesn't exist, to be able to create and lock the lockfile.
                ctx.fs().mkdirs(databaseLayout.databaseDirectory());
                return LockChecker.checkDatabaseLock(databaseLayout);
            });
        }

        @Override
        protected void doImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                Config databaseConfig,
                JobScheduler jobScheduler,
                InternalLogProvider logProvider,
                PageCacheTracer pageCacheTracer,
                CursorContextFactory contextFactory,
                Configuration importConfig,
                LogService logService,
                PrintStream stdOut,
                PrintStream stdErr,
                boolean verbose,
                Collector badCollector,
                MemoryTracker memoryTracker,
                Input input)
                throws IOException {
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine(databaseConfig);
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
    }

    @Command(name = "incremental", description = "Incremental import into an existing database.")
    public static class Incremental extends Base {
        @Option(
                names = "--stage",
                paramLabel = "all|prepare|build|merge",
                description = "Stage of incremental import. "
                        + "For incremental import into an existing database use 'all' (which requires "
                        + "the database to be stopped). For semi-online incremental import run 'prepare' (on "
                        + "a stopped database) followed by 'build' (on a potentially running database) and "
                        + "finally 'merge' (on a stopped database).",
                converter = StageConverter.class)
        IncrementalStage stage = IncrementalStage.all;

        @Option(names = "--force", required = true, description = "Confirm incremental import by setting this flag.")
        boolean forced;

        public Incremental(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        public void execute() throws Exception {
            if (!forced) {
                System.err.println(
                        "ERROR: Incremental import needs to be used with care. Please confirm by specifying --force.");
                throw new IllegalArgumentException("Missing force");
            }
            doExecute(
                    ImportType.incremental,
                    null,
                    false,
                    (layout) -> () -> {} /* locking handled in the specific steps */);
        }

        @Override
        protected void doImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                Config databaseConfig,
                JobScheduler jobScheduler,
                InternalLogProvider logProvider,
                PageCacheTracer pageCacheTracer,
                CursorContextFactory contextFactory,
                Configuration importConfig,
                LogService logService,
                PrintStream stdOut,
                PrintStream stdErr,
                boolean verbose,
                Collector badCollector,
                MemoryTracker memoryTracker,
                Input input)
                throws IOException {
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine(
                            fileSystem, databaseLayout)
                    .orElseThrow();
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
                        () -> readLogTailMetaData(fileSystem, databaseLayout, storageEngineFactory),
                        databaseConfig,
                        new PrintingImportLogicMonitor(stdOut, stdErr),
                        jobScheduler,
                        badCollector,
                        TransactionLogInitializer.getLogFilesInitializer(),
                        new IndexImporterFactoryImpl(),
                        memoryTracker,
                        contextFactory,
                        indexProviders);
                switch (stage) {
                    case prepare -> importer.prepare(input);
                    case build -> importer.build(input);
                    case merge -> importer.merge();
                    case all -> importer.doImport(input);
                    default -> throw new IllegalArgumentException("Unknown import mode " + stage);
                }
            }
        }

        static class StageConverter implements CommandLine.ITypeConverter<IncrementalStage> {
            @Override
            public IncrementalStage convert(String in) {
                in = switch (in) {
                    case "1" -> "prepare";
                    case "2" -> "build";
                    case "3" -> "merge";
                    default -> in.toLowerCase(Locale.ROOT);
                };
                try {
                    return IncrementalStage.valueOf(in);

                } catch (Exception e) {
                    throw new CommandLine.TypeConversionException(format("Invalid stage: %s (%s)", in, e));
                }
            }
        }
    }

    private static final String MULTI_FILE_DELIMITER = ",";

    static class NodeFilesGroup extends InputFilesGroup<Set<String>> {
        NodeFilesGroup(Set<String> key, String files) {
            super(key, files);
        }
    }

    static class RelationshipFilesGroup extends InputFilesGroup<String> {
        RelationshipFilesGroup(String key, String files) {
            super(key, files);
        }
    }

    abstract static class InputFilesGroup<T> {
        final T key;
        final String files;

        InputFilesGroup(T key, String files) {
            this.key = key;
            this.files = files;
        }

        Path[] toPaths(FileSystemAbstraction fs) {
            return parseFilesList(fs, files);
        }
    }

    @VisibleForTesting
    static RelationshipFilesGroup parseRelationshipFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, String::trim);
        return new RelationshipFilesGroup(p.getOne(), p.getTwo());
    }

    @VisibleForTesting
    static NodeFilesGroup parseNodeFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, s -> stream(s.split(":"))
                .map(String::trim)
                .filter(x -> !x.isEmpty())
                .collect(toSet()));
        return new NodeFilesGroup(p.getOne(), p.getTwo());
    }

    private static <T> Pair<T, String> parseInputFilesGroup(String str, Function<String, ? extends T> keyParser) {
        final var i = str.indexOf('=');
        if (i < 0) {
            return pair(keyParser.apply(""), str);
        }
        if (i == 0 || i == str.length() - 1) {
            throw new IllegalArgumentException("illegal `=` position: " + str);
        }
        final var keyStr = str.substring(0, i);
        return pair(keyParser.apply(keyStr), str.substring(i + 1));
    }

    private static Path[] parseFilesList(FileSystemAbstraction fs, String str) {
        return Converters.toFiles(MULTI_FILE_DELIMITER, Converters.regexFiles(fs, true))
                .apply(str);
    }

    @Option(
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Show this help message and exit.")
    private boolean helpRequested;

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

    protected enum ImportType {
        full("Full import"),
        incremental("Incremental import"),
        spd("SPD property data sharded import");

        private final String description;

        ImportType(String description) {
            this.description = description;
        }

        String description() {
            return description;
        }
    }
}
