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
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.importer.FileImporter.FileInputType.NO_INPUT;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Help.Visibility.NEVER;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.batchimport.api.BatchImporter.HardwareValidation;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexConfig;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.ByteUnitConverter;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.Converters.MaxOffHeapMemoryConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.cloud.storage.StorageUtils;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.importer.FileImporter.FileInputType;
import org.neo4j.importer.SchemaCommandReader.ReaderConfig;
import org.neo4j.importer.SchemaCommandSource.DeferredSchemaCommands;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.DefaultAdditionalIds;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction.PatternStyle;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.api.exceptions.ConsoleFriendlyException;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.files.LogTailMetadataFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.DeprecatedFormatWarning;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
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
    public abstract static class Base extends AbstractAdminCommand {
        /**
         * Delimiter used between files in an input group.
         */
        private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();

        public static final org.neo4j.csv.reader.Configuration DEFAULT_CSV_CONFIG = COMMAS;
        private static final Configuration DEFAULT_IMPORTER_CONFIG = DEFAULT;

        enum OnOffAuto {
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

        static class PatternStyleConverter implements ITypeConverter<PatternStyle> {
            @Override
            public PatternStyle convert(String value) throws Exception {
                return PatternStyle.valueOf(value.toUpperCase(Locale.ROOT));
            }
        }

        enum MultilineFormat {
            /**
             * Format for the legacy multiline parsing in single threaded mode
             */
            V1,
            /**
             * Format for the new, reverse scan of multiline CSV documents. This has the restriction that at least one
             * text field MUST be quoted per row.
             */
            V2
        }

        static class MultilineFormatConverter implements ITypeConverter<MultilineFormat> {
            @Override
            public MultilineFormat convert(String value) throws Exception {
                return MultilineFormat.valueOf(value.toUpperCase(Locale.ROOT));
            }
        }

        @Option(
                names = "--dry-run",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Flag used to indicate that a dry run of the import should be performed, i.e. no data "
                        + "will actually be imported, only the validation of the various arguments and estimation of "
                        + "size of the import will be performed and reported.")
        private boolean dryRun;

        @Option(
                names = "--skidbladnir",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Zzzing",
                hidden = true)
        private boolean skidbladnir;

        @Option(
                names = "--schema",
                paramLabel = "<path>",
                description =
                        "Path to the file containing the Cypher commands for creating indexes and constraints during"
                                + " data import.\n"
                                + "It is possible to load commands from AWS S3 buckets, Google Cloud storage buckets, and"
                                + " Azure buckets using the appropriate URI as the path.")
        private String schemaCommands;

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
                description = "File in which to store the report of the csv-import.")
        private Path reportFile;

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
        IdType defaultIdType = IdType.STRING;

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

        @Option(
                names = "--path-pattern-style",
                showDefaultValue = ALWAYS,
                paramLabel = "regex|glob|none",
                converter = PatternStyleConverter.class,
                defaultValue = "regex",
                description = "Pattern style to use for matching --nodes and --relationships files.")
        private PatternStyle patternStyle;

        private static final String MULTILINE_FIELDS = "--multiline-fields";
        private static final String MULTILINE_FIELDS_FORMAT = MULTILINE_FIELDS + "-format";

        @ArgGroup(exclusive = false)
        private MultilineFieldOptions multilineFieldOptions;

        static class MultilineFieldOptions {
            @Option(
                    names = MULTILINE_FIELDS,
                    required = true,
                    showDefaultValue = ALWAYS,
                    paramLabel = "true|false|<path>[,<path>]",
                    fallbackValue = "true",
                    description =
                            "In v1, whether or not fields from an input source can span multiple lines, i.e. contain "
                                    + "newline characters. Setting " + MULTILINE_FIELDS
                                    + "=true can severely degrade the performance of the importer. Therefore, use it"
                                    + " with care, especially with large imports. In v2, this option will specify the"
                                    + " list of files that contain multiline fields. Files can also be specified using"
                                    + " regular expressions.")
            private String multilineFields;

            @Option(
                    names = MULTILINE_FIELDS_FORMAT,
                    converter = MultilineFormatConverter.class,
                    showDefaultValue = ALWAYS,
                    paramLabel = "v1|v2",
                    description = "Controls the parsing of input source that can span multiple lines, i.e. contain "
                            + "newline characters. When set to v1, the value for " + MULTILINE_FIELDS + " can only be "
                            + "true or false. When set to v2, the value for " + MULTILINE_FIELDS
                            + " should be the list of files that contain multiline fields.")
            private MultilineFormat multilineFormat = MultilineFormat.V1;
        }

        @Option(
                names = "--ignore-empty-strings",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as"
                                + " null.")
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

        private static final String DELIMITER_WARNING_CSV =
                "For CSV data, this value must be different from the one specified in the `--delimiter` option.";

        @Option(
                names = "--delimiter",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description = "Delimiter character between values in CSV data. "
                        + "Also accepts 'TAB' and e.g. 'U+002A' for specifying a character using Unicode. Note that "
                        + "the delimiter character must be a single byte character in UTF-8.")
        private char delimiter = DEFAULT_CSV_CONFIG.delimiter();

        @Option(
                names = "--accept-multibyte-delimiter",
                hidden = true,
                arity = "0..1",
                fallbackValue = "true",
                description = "Allow multibyte delimiter character, this will have performance impact and this option"
                        + " is only here to allow legacy delimiter characters")
        private boolean allowMultibyteDelimiter = false;

        @Option(
                names = "--array-delimiter",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description = "Delimiter character between array elements within a value in CSV data. "
                        + "Also accepts 'TAB' and e.g. 'U+20AC' for specifying a character using Unicode. "
                        + DELIMITER_WARNING_CSV
                        + "For Parquet data, this is only needed if the array is encoded as a string.")
        private char arrayDelimiter = DEFAULT_CSV_CONFIG.arrayDelimiter();

        @Option(
                names = "--vector-delimiter",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description = "Delimiter character between vector coordinates within a value in CSV data. "
                        + "Also accepts 'TAB' and e.g. 'U+20AC' for specifying a character using Unicode. "
                        + DELIMITER_WARNING_CSV
                        + "For Parquet data, this is only needed if the vector is encoded as a string.")
        private char vectorDelimiter = DEFAULT_CSV_CONFIG.vectorDelimiter();

        @Option(
                names = "--quote",
                paramLabel = "<char>",
                converter = EscapedCharacterConverter.class,
                description =
                        "Character to treat as a quotation mark for values in CSV data. For example, quotes can be escaped as per RFC 4180 by doubling them. "
                                + "Thus \"\" would be interpreted as a literal \". You cannot escape using \\. "
                                + DELIMITER_WARNING_CSV)
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
                description = MaxOffHeapMemoryConverter.DESCRIPTION)
        private long maxOffHeapMemory;

        @Option(
                names = "--high-parallel-io",
                showDefaultValue = ALWAYS,
                paramLabel = "on|off|auto",
                defaultValue = "auto",
                converter = OnOffAutoConverter.class,
                description =
                        "Ignore environment-based heuristics and indicate if the target storage subsystem can support"
                                + " parallel IO with high throughput or auto detect.  Typically this is on for SSDs, large"
                                + " raid arrays, and network-attached storage.")
        private OnOffAuto highIo;

        private static final String THREADS = "--threads";

        @Option(
                names = THREADS,
                paramLabel = "<num>",
                description = "(advanced) Max number of worker threads used by the importer. Defaults to the number of"
                        + " available processors reported by the JVM. There is a certain amount of minimum threads"
                        + " needed so for that reason there is no lower bound for this value. For optimal"
                        + " performance, this value should not be greater than the number of available"
                        + " processors.")
        private int threads = DEFAULT_IMPORTER_CONFIG.maxNumberOfWorkerThreads();

        protected static final String BAD_TOLERANCE_OPTION = "--bad-tolerance";

        @Option(
                names = BAD_TOLERANCE_OPTION,
                paramLabel = "<num>",
                description = "Number of bad entries before the import is aborted. The import process is optimized for"
                        + " error-free data. Therefore, cleaning the data before importing it is highly"
                        + " recommended. If you encounter any bad entries during the import process, you can set"
                        + " the number of bad entries to a specific value that suits your needs. However, setting"
                        + " a high value may affect the performance of the tool.")
        private long badTolerance = BadCollector.UNLIMITED_TOLERANCE;

        public static final String SKIP_BAD_ENTRIES_LOGGING = "--skip-bad-entries-logging";

        @Option(
                names = SKIP_BAD_ENTRIES_LOGGING,
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "When set to `true`, the details of bad entries are not written in the log. Disabling logging"
                                + " can improve performance when the data contains lots of faults. Cleaning the data"
                                + " before importing it is highly recommended because faults dramatically affect the"
                                + " tool's performance even without logging.")
        private boolean skipBadEntriesLogging;

        @Option(
                names = "--skip-bad-relationships",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not to skip importing relationships that refer to missing node IDs, i.e. either"
                                + " start or end node ID/group referring to a node that was not specified by the node"
                                + " input data. Skipped relationships will be logged if they are within the limit of"
                                + " entities specified by " + BAD_TOLERANCE_OPTION + " and the "
                                + SKIP_BAD_ENTRIES_LOGGING
                                + " option is disabled.")
        private boolean skipBadRelationships;

        @Option(
                names = "--strict",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                description =
                        "Whether or not the lookup of nodes referred to from relationships needs to be checked strict."
                                + " If disabled, most but not all relationships referring to non-existent nodes will be"
                                + " detected. If enabled all those relationships will be found but at the cost of lower"
                                + " performance.")
        private boolean strict = false;

        @Option(
                names = "--skip-duplicate-nodes",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Whether or not to skip importing nodes that have the same ID/group. In the event of multiple"
                                + " nodes within the same group having the same ID, the first encountered will be"
                                + " imported, whereas consecutive such nodes will be skipped. Skipped nodes will be logged"
                                + " if they are within the limit of entities specified by " + BAD_TOLERANCE_OPTION
                                + " and the " + SKIP_BAD_ENTRIES_LOGGING + " option is disabled.")
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
                arity = "1..*",
                converter = NodeFilesConverter.class,
                paramLabel = "[<label>[:<label>]...=]<files>",
                description = "Node CSV header and data. Multiple files will be logically seen as one big file from the"
                        + " perspective of the importer. The first line must contain the header. Multiple data"
                        + " sources like these can be specified in one import, where each data source has its own"
                        + " header. Files can also be specified using regular expressions.\n"
                        + "It is possible to import files from AWS S3 buckets, Google Cloud storage buckets, and"
                        + " Azure buckets using the appropriate URI as the path.")
        private List<NodeFilesGroup> nodes;

        @Option(
                names = "--relationships",
                arity = "1..*",
                converter = RelationshipFilesConverter.class,
                showDefaultValue = NEVER,
                paramLabel = "[<type>=]<files>",
                description =
                        "Relationship CSV header and data. Multiple files will be logically seen as one big file from"
                                + " the perspective of the importer. The first line must contain the header. Multiple data"
                                + " sources like these can be specified in one import, where each data source has its own"
                                + " header. Files can also be specified using regular expressions.\n"
                                + "It is possible to import files from AWS S3 buckets, Google Cloud storage buckets, and"
                                + " Azure buckets using the appropriate URI as the path.")
        private List<RelationshipFilesGroup> relationships = new ArrayList<>();

        @Option(
                names = "--auto-skip-subsequent-headers",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description =
                        "Automatically skip accidental header lines in subsequent files in file groups with more than"
                                + " one file.")
        private boolean autoSkipHeaders;

        @Option(
                names = "--ranges",
                hidden = true,
                defaultValue = "-1",
                description =
                        "(advanced) override the number of ranges the relationship data is split into during import."
                                + " Number of ranges relates to reducing unnecessary page faults during import and"
                                + " typically the number of ranges is automatically and optimally calculated for best"
                                + " performance. However, if it turns out that this calculation may not be optimal, this"
                                + " option can override this calculation")
        private int overrideNumRanges;

        @Option(
                names = "--input-type",
                paramLabel = "csv|parquet",
                description = "File type to import from. Can be csv or parquet. Defaults to csv.",
                converter = FileInputTypeConverter.class)
        FileImporter.FileInputType fileInputType;

        @Option(
                names = "--temp-path",
                paramLabel = "<path>",
                description =
                        "Provide a path where to store temporary files that are created and deleted during import. If"
                                + " not specifically provided, the default temp path will be created inside the database"
                                + " directory of the imported database.")
        private Path tempPath;

        @Option(
                names = "--disable-instrumentation",
                hidden = true,
                defaultValue = "false",
                fallbackValue = "true",
                description = "Disable import performance instrumentation.")
        private boolean disableInstrumentation;

        @Option(
                names = "--capture-java-flight-recordings",
                hidden = true,
                arity = "0..1",
                fallbackValue = "true",
                defaultValue = "true",
                description = "Enable import performance instrumentation to take java flight recordings when "
                        + "significant performance issues are found.")
        private boolean captureJFRs;

        @Option(
                names = "--capture-thread-dumps",
                hidden = true,
                arity = "0..1",
                fallbackValue = "true",
                defaultValue = "false",
                description = "Enable import performance instrumentation to take thread dumps when "
                        + "significant performance issues are found.")
        private boolean captureThreadDumps;

        @Option(
                names = "--profile",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                defaultValue = "false",
                description = "Capture a java flight recording for the entire duration of the import.")
        private boolean captureProfile;

        @Option(
                names = "--profile-results-path",
                paramLabel = "<path>",
                description = "Provide a path where to store java flight recordings captured with the --profile "
                        + "option. Requires --profile or --profile=true to be set to have an effect.")
        private Path captureProfileResultPath = null;

        private Monitor monitor;

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

        /**
         * @deprecated Please use {@link Base#decorateImportContext(Monitor)} instead to decorate the import context
         * Available since it's otherwise difficult to do this by means of PicoCLI.
         */
        @Deprecated(forRemoval = true, since = "2026.03")
        public void setMonitor(Monitor monitor) {
            this.monitor = monitor;
        }

        /**
         * (Optionally) decorates the import context monitor to receive callbacks about the various stages of the
         * import process.
         *
         * @param contextMonitor the import context monitor to decorate
         * @return the monitor to be used during the import process
         */
        public Monitor decorateImportContext(Monitor contextMonitor) {
            return contextMonitor;
        }

        @Override
        public void execute() throws Exception {
            var format = importFormat();
            if (format != null && StorageEngineFactory.isFormatDeprecated(format)) {
                printf("WARNING: %s%n", DeprecatedFormatWarning.getTargetFormatWarning(format));
            }

            try (var importContext = ImportContext.create(
                    ctx.fs(),
                    database,
                    loadNeo4jConfig(format),
                    reportFile,
                    includeUpdatesInProgress(),
                    !disableInstrumentation && captureProfile && captureProfileResultPath == null,
                    verbose)) {
                var databaseConfig = importContext.config();
                var databaseLayout = Neo4jLayout.of(databaseConfig).databaseLayout(database.name());
                try (var fileSystem = new SchemeFileSystemAbstraction(ctx.fs(), databaseConfig, importContext)) {
                    preImportValidation(fileSystem, databaseConfig.get(GraphDatabaseSettings.db_format));

                    final var importerBuilder = configureFileImporterBuilder(FileImporter.builder()
                            .withCsvConfig(csvConfiguration(fileSystem))
                            .withImportConfig(importConfiguration(databaseConfig))
                            .withDatabaseLayout(databaseLayout)
                            .withDatabaseConfig(databaseConfig)
                            .withFileSystem(fileSystem)
                            .withStdOut(ctx.out())
                            .withStdErr(ctx.err())
                            .withDefaultIdType(defaultIdType)
                            .withInputEncoding(inputEncoding)
                            .withIgnoreExtraColumns(ignoreExtraColumns)
                            .withBadTolerance(badTolerance)
                            .withSkipBadRelationships(skipBadRelationships)
                            .withSkipDuplicateNodes(skipDuplicateNodes)
                            .withSkipBadEntriesLogging(skipBadEntriesLogging)
                            .withSkipBadRelationships(skipBadRelationships)
                            .withNormalizeTypes(normalizeTypes)
                            .withVerbose(verbose)
                            .withAutoSkipHeaders(autoSkipHeaders)
                            .withSchemaCommands(parseSchemaCommands(fileSystem, databaseConfig))
                            .withReportOutputStream(importContext::collectorOutputStream)
                            .withLogProvider(importContext)
                            .withMonitor(monitor == null ? decorateImportContext(importContext) : monitor));

                    FileImporter importer;
                    if (isDistributedPropShard()) {
                        // faking this because input will be ignored anyway since input SHOULD come from the graph shard
                        importer = importerBuilder.withFileInputType(NO_INPUT).build();
                    } else {
                        importer = addInputData(fileSystem, importerBuilder).build();
                    }

                    validateInputType(importer.fileInputType());

                    if (dryRun) {
                        importer.dryRun(this);
                    } else {
                        try (var ignore = maybeLockChecker().maybeCheckLock(databaseLayout)) {
                            importContext.preamble(ctx.out());
                            importer.doImport(this, skidbladnir);
                            postImport(fileSystem, databaseConfig, importContext, databaseLayout);
                        }
                    }
                } catch (Exception e) {
                    throw importContext.captureError(e);
                }
            }
        }

        protected boolean isDryRun() {
            return dryRun;
        }

        protected boolean isSkidbladnir() {
            return skidbladnir;
        }

        /**
         * @param resolvedDbFormat the format that is either specified in the command line or resolved from the database config
         */
        protected void preImportValidation(SchemeFileSystemAbstraction fs, String resolvedDbFormat)
                throws CommandFailedException {
            if (requiresNodeParameter()) {
                if (nodes == null) {
                    throw new ParameterException(spec.commandLine(), "Missing required option: '--nodes'");
                }
            }

            if (isDistributedGraphShard() && isDistributedPropShard()) {
                throw new ParameterException(
                        spec.commandLine(), "Both distributed graph and property shard options have been specified");
            }

            if (threads > DEFAULT_IMPORTER_CONFIG.maxNumberOfWorkerThreads()) {
                printf(
                        "WARNING: '%s' is set to %d but the total number of cores on this machine is only %d"
                                + " which could severely impact performance.",
                        THREADS, threads, DEFAULT_IMPORTER_CONFIG.maxNumberOfWorkerThreads());
            }

            if (badTolerance == BadCollector.UNLIMITED_TOLERANCE && skipBadEntriesLogging) {
                printf(
                        "WARNING: '%s' is set to 'true' but the import process will not be stopped upon a bad entry"
                                + " being encountered (due to '%s' being set to unlimited). You will be unable to identify"
                                + "  these bad entries upon completion of the import.",
                        SKIP_BAD_ENTRIES_LOGGING, BAD_TOLERANCE_OPTION);
            }
            // Check if the provided delimiter is multibyte, disallow this
            if (!allowMultibyteDelimiter && String.valueOf(delimiter).getBytes(StandardCharsets.UTF_8).length > 1) {
                throw new ParameterException(
                        spec.commandLine(), "Delimiter must be a single byte character (In UTF-8)");
            }
        }

        protected void validateInputType(FileInputType inputType) {
            if (isSkidbladnir() && inputType != FileInputType.CSV) {
                throw new ParameterException(
                        spec.commandLine(), "ERROR: Skidbladnir import is only supported for CSV input type");
            }
        }

        protected void postImport(
                SchemeFileSystemAbstraction fileSystem,
                Config config,
                InternalLogProvider logProvider,
                DatabaseLayout databaseLayout)
                throws IOException {}

        public abstract String importType();

        public abstract boolean isDistributedGraphShard();

        public abstract boolean isDistributedPropShard();

        public boolean isDistributed() {
            return isDistributedPropShard() || isDistributedGraphShard();
        }

        protected abstract boolean includeUpdatesInProgress();

        protected abstract boolean requiresNodeParameter();

        protected abstract String importFormat();

        protected abstract FileImporter.Builder configureFileImporterBuilder(FileImporter.Builder builder)
                throws IOException;

        protected abstract MaybeLocker maybeLockChecker();

        protected abstract void doDryRun(
                Input input,
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
                JobScheduler jobScheduler,
                CursorContextFactory contextFactory,
                Configuration importConfig,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                PrintStream stdOut,
                boolean verbose,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException;

        protected abstract void doImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                boolean force,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
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
                Input input,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException;

        protected abstract void doSkidbladnirImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                boolean force,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
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
                Input input,
                Charset encoding,
                Map<Set<String>, List<FileGroup>> nodeFileGroupsByAdditionalLabels,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException;

        protected IndexConfig customiseIndexConfig(Config databaseConfig, IndexConfig indexConfig) {
            return setupIndexConfigForImport(indexConfig);
        }

        private FileImporter.Builder addInputData(FileSystemAbstraction fs, FileImporter.Builder importerBuilder) {
            var actualInputType = fileInputType;
            for (NodeFilesGroup n : nodes) {
                Path[] paths = n.toPathArray(fs, patternStyle);
                FileGroup fileGroup = importerBuilder.toFileGroup(paths);
                FileInputType inputType = resolveFileInputType(fileInputType, fileGroup);
                if (actualInputType == null) {
                    actualInputType = inputType;
                } else if (inputType != actualInputType) {
                    throw unexpectedInputType(actualInputType, inputType, fileInputType != null, true, fileGroup);
                }
                importerBuilder.addNodeFiles(n.key, fileGroup);
            }
            for (RelationshipFilesGroup r : relationships) {
                Path[] paths = r.toPathArray(fs, patternStyle);
                FileGroup fileGroup = importerBuilder.toFileGroup(paths);
                FileInputType inputType = resolveFileInputType(fileInputType, fileGroup);
                if (inputType != actualInputType) {
                    throw unexpectedInputType(actualInputType, inputType, fileInputType != null, false, fileGroup);
                }
                importerBuilder.addRelationshipFiles(r.key, fileGroup);
            }

            return importerBuilder.withFileInputType(actualInputType);
        }

        private ConsoleFriendlyException unexpectedInputType(
                FileInputType expectedType,
                FileInputType unexpectedType,
                boolean configuredInCLI,
                boolean inNodes,
                FileGroup fileGroup) {
            var entity = inNodes ? "node" : "relationship";
            String message;
            if (configuredInCLI) {
                message = "The %s files contain a %s file but only files of type %s should be provided."
                        .formatted(entity, unexpectedType, expectedType);
            } else {
                message = "The %s files contain a mixture of both %s and %s files."
                        .formatted(entity, expectedType, unexpectedType);
            }

            return new CommandFailedException(message, ExitCode.USAGE)
                    .addSupplementaryMessage(
                            "A mixture of both %s and %s files in the import is not currently supported [%s]."
                                    .formatted(
                                            expectedType,
                                            unexpectedType,
                                            fileGroup
                                                    .streamPaths()
                                                    .map(Path::getFileName)
                                                    .map(Objects::toString)
                                                    .collect(Collectors.joining(","))));
        }

        private SchemaCommandSource parseSchemaCommands(SchemeFileSystemAbstraction fileSystem, Config config)
                throws IOException {
            if (schemaCommands == null) {
                return ResolvedSchemaCommands.of();
            }
            final var schemaPath = schemaCommandsPath(fileSystem);
            Preconditions.checkState(
                    fileSystem.fileExists(schemaPath), "The path to the Cypher schema commands must exist");

            String cypherText;
            try {
                cypherText = FileSystemUtils.readString(fileSystem, schemaPath, EmptyMemoryTracker.INSTANCE);
            } catch (IOException ex) {
                throw new CommandFailedException("Unable to read schema commands", ex);
            }
            if (cypherText == null || cypherText.isEmpty()) {
                return ResolvedSchemaCommands.of();
            }

            return new DeferredSchemaCommands((adaptor, tokens) -> {
                final var reader = schemaCommandReader(fileSystem, config, adaptor, tokens);
                try {
                    return reader.parse(cypherText);
                } catch (SchemaCommandReaderException ex) {
                    throw new CommandFailedException("Error parsing schema commands", ex);
                }
            });
        }

        protected abstract SchemaCommandReader schemaCommandReader(
                SchemeFileSystemAbstraction fileSystem,
                Config config,
                DeferredSchemaCommands.Adaptor adaptor,
                TokenHolders tokenHolders);

        private Path schemaCommandsPath(SchemeFileSystemAbstraction fileSystem) throws IOException {
            assert schemaCommands != null;

            String commandPath;
            if (schemaCommands.startsWith("'") && schemaCommands.endsWith("'")) {
                commandPath = schemaCommands.substring(1, schemaCommands.length() - 1);
            } else {
                commandPath = schemaCommands;
            }

            final var schemaPath = fileSystem.resolve(commandPath);
            if (!fileSystem.fileExists(schemaPath)) {
                throw new CommandFailedException("The provided schema commands file does not exist.", ExitCode.IOERR);
            }

            if (fileSystem.isDirectory(schemaPath)) {
                throw new CommandFailedException(
                        "The provided schema commands file is not a regular file.", ExitCode.IOERR);
            }

            return schemaPath;
        }

        @VisibleForTesting
        Config loadNeo4jConfig(String format) {
            Config.Builder builder = createPrefilledConfigBuilder();
            if (StringUtils.isNotEmpty(format)) {
                builder.set(GraphDatabaseSettings.db_format, format);
            }
            return builder.build();
        }

        private org.neo4j.csv.reader.Configuration csvConfiguration(SchemeFileSystemAbstraction fs) {
            final var builder = DEFAULT_CSV_CONFIG.toBuilder()
                    .withDelimiter(delimiter)
                    .withArrayDelimiter(arrayDelimiter)
                    .withVectorDelimiter(vectorDelimiter)
                    .withQuotationCharacter(quote)
                    .withEmptyQuotedStringsAsNull(ignoreEmptyStrings)
                    .withTrimStrings(trimStrings)
                    .withLegacyStyleQuoting(legacyStyleQuoting);

            if (bufferSize == DEFAULT_CSV_CONFIG.bufferSize()) {
                // Use default value
                if (skidbladnir) {
                    builder.withBufferSize(
                            org.neo4j.csv.reader.Configuration.Builder.DEFAULT_BUFFER_SIZE_IF_SKIDBLADNIR);
                } else {
                    builder.withBufferSize(DEFAULT_CSV_CONFIG.bufferSize());
                }
            } else {
                // Use provided value
                builder.withBufferSize(toIntExact(bufferSize));
            }

            if (multilineFieldOptions != null) {
                final var multilineFields = multilineFieldOptions.multilineFields;
                switch (multilineFieldOptions.multilineFormat) {
                    case V1 -> {
                        if (Boolean.TRUE.toString().equalsIgnoreCase(multilineFields)) {
                            builder.withLegacyMultilineBehaviour();
                        } else if (!Boolean.FALSE.toString().equalsIgnoreCase(multilineFields)) {
                            throw new IllegalArgumentException(
                                    "Illegal format for %s when using the v1 format - must be either true or false"
                                            .formatted(MULTILINE_FIELDS));
                        }
                    }
                    case V2 -> {
                        final var paths = Arrays.stream(parseFilesList(fs, multilineFields, patternStyle))
                                .map(StorageUtils::toString)
                                .collect(toSet());
                        builder.withMultilineDocuments(paths::contains);
                    }
                }
            }

            return builder.build();
        }

        private org.neo4j.batchimport.api.Configuration importConfiguration(Config databaseConfig) {
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
                    return customiseIndexConfig(databaseConfig, IndexConfig.create());
                }

                @Override
                public boolean strictNodeCheck() {
                    return strict;
                }

                @Override
                public boolean enableInstrumentation() {
                    return !disableInstrumentation;
                }

                @Override
                public boolean instrumentationCaptureJFRs() {
                    return captureJFRs;
                }

                @Override
                public boolean instrumentationCaptureThreadDumps() {
                    return captureThreadDumps;
                }

                @Override
                public int intermediaryBufferSize() {
                    if (skidbladnir) {
                        return (int) ByteUnit.mebiBytes(1);
                    }
                    return super.intermediaryBufferSize();
                }

                @Override
                public boolean captureProfile() {
                    return captureProfile;
                }

                @Override
                public Path captureProfileResultPath() {
                    return captureProfileResultPath != null ? captureProfileResultPath.toAbsolutePath() : null;
                }

                @Override
                public int forcedNumberOfNodeIdRanges() {
                    if (overrideNumRanges != -1) {
                        return overrideNumRanges;
                    }
                    return super.forcedNumberOfNodeIdRanges();
                }

                @Override
                public Path tempDirectory(Path databaseDirectory) {
                    if (tempPath != null) {
                        // The idea is that when providing a specific directory it may be the case that one
                        // import configuration could be used for multiple internal imports
                        // (e.g. multi-stage incremental). Therefor guard for this fact by including the db directory
                        // name on the path too.
                        return tempPath.resolve(
                                "temp-" + databaseDirectory.getFileName().toString());
                    }
                    return super.tempDirectory(databaseDirectory);
                }
            };
        }

        private FileInputType resolveFileInputType(FileInputType expectedType, FileGroup fileGroup) {
            Predicate<Path> checkForParquet = path ->
                    path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".parquet");
            if (expectedType == FileInputType.PARQUET) {
                // Parquet imports allows the first path of a group to be a header CSV - which obvs isn't .parquet
                return fileGroup.fileCount() == 1
                                || fileGroup.streamPaths().skip(1).anyMatch(checkForParquet)
                        ? FileInputType.PARQUET
                        : FileInputType.CSV;
            }

            // default to CSV otherwise
            return fileGroup.streamPaths().anyMatch(checkForParquet) ? FileInputType.PARQUET : FileInputType.CSV;
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

        static class RelationshipFilesConverter implements ITypeConverter<RelationshipFilesGroup> {
            @Override
            public RelationshipFilesGroup convert(String value) {
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

        static class FileInputTypeConverter implements CommandLine.ITypeConverter<FileImporter.FileInputType> {
            @Override
            public FileImporter.FileInputType convert(String in) {
                try {
                    return FileImporter.FileInputType.valueOf(in.toUpperCase(Locale.ROOT));
                } catch (Exception e) {
                    throw new CommandLine.TypeConversionException(format(
                            "Invalid file import type: %s. Only %s are supported as values",
                            in,
                            Arrays.stream(FileImporter.FileInputType.values())
                                    .map(value ->
                                            String.format("'%s'", value.name().toLowerCase(Locale.ROOT)))
                                    .collect(Collectors.joining(", "))));
                }
            }
        }
    }

    @Command(
            name = "full",
            description = "High-speed initial import of fault-free data from CSV files into a non-existent or empty"
                    + " database.")
    public static class Full extends Base {
        @Option(
                names = "--format",
                showDefaultValue = NEVER,
                description = "Name of database format. The imported database will be created in the specified format "
                        + "or use the format set in the configuration. Valid formats are `standard`, `aligned`, "
                        + "`high_limit`, and `block`.")
        protected String format;

        // Was force
        @Option(
                names = "--overwrite-destination",
                arity = "0..1",
                showDefaultValue = ALWAYS,
                paramLabel = "true|false",
                fallbackValue = "true",
                description = "Delete any existing database files prior to the import.")
        protected boolean overwriteDestination;

        public Full(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void preImportValidation(SchemeFileSystemAbstraction fs, String resolvedDbFormat)
                throws CommandFailedException {
            super.preImportValidation(fs, resolvedDbFormat);

            if (isSkidbladnir() && !"block".equals(resolvedDbFormat)) {
                throw new CommandFailedException(
                        "ERROR: Skidbladnir import is only supported for the 'block' format, but '%s' was specified."
                                .formatted(resolvedDbFormat));
            }
        }

        @Override
        protected boolean requiresNodeParameter() {
            return true;
        }

        @Override
        public String importType() {
            return "Full import";
        }

        @Override
        protected boolean includeUpdatesInProgress() {
            return false;
        }

        @Override
        public boolean isDistributedGraphShard() {
            return false;
        }

        @Override
        public boolean isDistributedPropShard() {
            return false;
        }

        @Override
        protected String importFormat() {
            return format;
        }

        @Override
        protected FileImporter.Builder configureFileImporterBuilder(FileImporter.Builder builder) {
            return withStorageEngineFactory(builder.withForce(overwriteDestination)
                    .withCursorContextFactory(new CursorContextFactory(
                            PageCacheTracer.NULL, new FixedVersionContextSupplier(BASE_TX_ID))));
        }

        @Override
        protected MaybeLocker maybeLockChecker() {
            return databaseLayout -> {
                // Create the db folder if it doesn't exist, to be able to create and lock the lockfile.
                ctx.fs().mkdirs(databaseLayout.databaseDirectory());
                return LockChecker.checkDatabaseLock(databaseLayout);
            };
        }

        @Override
        protected void doDryRun(
                Input input,
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
                JobScheduler jobScheduler,
                CursorContextFactory contextFactory,
                Configuration importConfig,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                PrintStream stdOut,
                boolean verbose,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException {
            var batchImporter = storageEngineFactory.batchImporter(
                    databaseLayout,
                    fileSystem,
                    false,
                    PageCacheTracer.NULL,
                    importConfig,
                    NullLogService.getInstance(),
                    stdOut,
                    verbose,
                    DefaultAdditionalIds.EMPTY,
                    new LogTailMetadataFactoryImpl(fileSystem),
                    databaseConfig,
                    monitor,
                    jobScheduler,
                    Collector.EMPTY,
                    LogFilesInitializer.NULL,
                    IndexImporterFactoryImpl.EMPTY,
                    EmptyMemoryTracker.INSTANCE,
                    contextFactory,
                    indexProvidersAccess,
                    shardingArguments == null ? 0 : shardingArguments.numShards(),
                    shardingArguments == null ? null : shardingArguments.additionalArguments(),
                    DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                    HardwareValidation.INFORMATION);
            batchImporter.doDryRun(input, stdOut);
        }

        @Override
        protected void doSkidbladnirImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                boolean force,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
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
                Input input,
                Charset encoding,
                Map<Set<String>, List<FileGroup>> nodeFileGroupsByAdditionalLabels,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException {
            storageEngineFactory
                    .batchImporter(
                            databaseLayout,
                            fileSystem,
                            force,
                            pageCacheTracer,
                            importConfig,
                            logService,
                            stdOut,
                            verbose,
                            DefaultAdditionalIds.EMPTY,
                            new LogTailMetadataFactoryImpl(fileSystem),
                            databaseConfig,
                            new PrintingImportLogicMonitor(stdOut, stdErr, monitor),
                            jobScheduler,
                            badCollector,
                            TransactionLogInitializer.getLogFilesInitializer(),
                            new IndexImporterFactoryImpl(),
                            memoryTracker,
                            contextFactory,
                            indexProvidersAccess,
                            shardingArguments == null ? 0 : shardingArguments.numShards,
                            shardingArguments == null ? null : shardingArguments.additionalArguments,
                            DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                            HardwareValidation.WARNING)
                    .doSkidbladnirImport(input, encoding, nodeFileGroupsByAdditionalLabels);
        }

        @Override
        protected void doImport(
                FileSystemAbstraction fileSystem,
                DatabaseLayout databaseLayout,
                boolean force,
                Config databaseConfig,
                StorageEngineFactory storageEngineFactory,
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
                Input input,
                Supplier<IndexProvidersAccess> indexProvidersAccess,
                ShardingArguments shardingArguments,
                Monitor monitor)
                throws IOException {
            storageEngineFactory
                    .batchImporter(
                            databaseLayout,
                            fileSystem,
                            force,
                            pageCacheTracer,
                            importConfig,
                            logService,
                            stdOut,
                            verbose,
                            DefaultAdditionalIds.EMPTY,
                            new LogTailMetadataFactoryImpl(fileSystem),
                            databaseConfig,
                            new PrintingImportLogicMonitor(stdOut, stdErr, monitor),
                            jobScheduler,
                            badCollector,
                            TransactionLogInitializer.getLogFilesInitializer(),
                            new IndexImporterFactoryImpl(),
                            memoryTracker,
                            contextFactory,
                            indexProvidersAccess,
                            shardingArguments == null ? 0 : shardingArguments.numShards,
                            shardingArguments == null ? null : shardingArguments.additionalArguments,
                            DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                            HardwareValidation.WARNING)
                    .doImport(input);
        }

        @Override
        protected SchemaCommandReader schemaCommandReader(
                SchemeFileSystemAbstraction fileSystem,
                Config config,
                DeferredSchemaCommands.Adaptor adaptor,
                TokenHolders tokenHolders) {
            return new SchemaCommandReader(
                    fileSystem,
                    SchemaCommandParser.createCommunity(CypherConfiguration.fromConfig(config)),
                    ReaderConfig.communityImporter(config));
        }

        protected FileImporter.Builder withStorageEngineFactory(FileImporter.Builder builder) {
            return builder.withStorageEngineFactory(
                    StorageEngineFactory.selectStorageEngine(builder.getDatabaseConfig()));
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

    public record ShardingArguments(int numShards, DependencyResolver additionalArguments) {}

    abstract static class InputFilesGroup<T> {
        final T key;
        final String files;

        InputFilesGroup(T key, String files) {
            this.key = key;
            this.files = files;
        }

        Path[] toPathArray(FileSystemAbstraction fs, PatternStyle patternStyle) {
            return parseFilesList(fs, files, patternStyle);
        }
    }

    @VisibleForTesting
    public static IndexConfig setupIndexConfigForImport(IndexConfig indexConfig) {
        return indexConfig.withLabelIndex().withRelationshipTypeIndex();
    }

    @VisibleForTesting
    static RelationshipFilesGroup parseRelationshipFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, s -> s == null ? null : s.trim());
        return new RelationshipFilesGroup(p.getOne(), p.getTwo());
    }

    @VisibleForTesting
    static NodeFilesGroup parseNodeFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, s -> {
            if (s == null) {
                return Collections.<String>emptySet();
            }
            return stream(s.split(":"))
                    .map(String::trim)
                    .filter(x -> !x.isEmpty())
                    .collect(toSet());
        });
        return new NodeFilesGroup(p.getOne(), p.getTwo());
    }

    private static <T> Pair<T, String> parseInputFilesGroup(String str, Function<String, ? extends T> keyParser) {
        final var i = str.indexOf('=');
        if (i < 0) {
            return pair(keyParser.apply(null), str);
        }
        if (i == 0 || i == str.length() - 1) {
            throw new IllegalArgumentException("illegal `=` position: " + str);
        }
        final var keyStr = str.substring(0, i);
        return pair(keyParser.apply(keyStr), str.substring(i + 1));
    }

    private static Path[] parseFilesList(FileSystemAbstraction fs, String str, PatternStyle patternStyle) {
        return Converters.toFiles(MULTI_FILE_DELIMITER, Converters.patternMatchFiles(fs, true, patternStyle))
                .apply(str);
    }

    @Option(
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Show this help message and exit.")
    private boolean helpRequested;
}
