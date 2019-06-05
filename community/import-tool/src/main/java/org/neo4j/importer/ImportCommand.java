/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.importer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArgWithMetadata;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LayoutConfig;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Math.toIntExact;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.Settings.parseLongWithUnit;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.importer.CsvImporter.MULTI_FILE_DELIMITER;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT_MAX_MEMORY_PERCENT;
import static org.neo4j.internal.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.internal.batchimport.Configuration.canDetectFreeMemory;
import static org.neo4j.internal.helpers.TextUtil.tokenizeStringWithQuotes;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.fs.FileUtils.readTextFile;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

public class ImportCommand implements AdminCommand
{
    public static final String DEFAULT_REPORT_FILE_NAME = "import.report";
    static final String OPT_MULTILINE_FIELDS = "multiline-fields";
    private static final String OPT_REPORT_FILE = "report-file";
    private static final String OPT_NODES = "nodes";
    private static final String OPT_RELATIONSHIPS = "relationships";
    private static final String OPT_ID_TYPE = "id-type";
    private static final String OPT_INPUT_ENCODING = "input-encoding";
    private static final String OPT_IGNORE_EXTRA_COLUMNS = "ignore-extra-columns";
    private static final String OPT_HIGH_IO = "high-io";
    private static final String OPT_TRIM_STRINGS = "trim-strings";
    private static final String OPT_PROCESSORS = "processors";
    private static final String OPT_SKIP_DUPLICATE_NODES = "skip-duplicate-nodes";
    private static final String OPT_SKIP_BAD_RELATIONSHIPS = "skip-bad-relationships";
    private static final String OPT_SKIP_BAD_ENTRIES_LOGGING = "skip-bad-entries-logging";
    private static final String OPT_READ_BUFFER_SIZE = "read-buffer-size";
    private static final String OPT_LEGACY_STYLE_QUOTING = "legacy-style-quoting";
    private static final String OPT_IGNORE_EMPTY_STRINGS = "ignore-empty-strings";
    private static final String OPT_DETAILED_PROGRESS = "detailed-progress";
    private static final String OPT_CACHE_ON_HEAP = "cache-on-heap";
    private static final String OPT_BAD_TOLERANCE = "bad-tolerance";
    private static final String OPT_MAX_MEMORY = "max-memory";
    private static final String OPT_QUOTE = "quote";
    private static final String OPT_DELIMITER = "delimiter";
    private static final String OPT_ARRAY_DELIMITER = "array-delimiter";
    private static final String OPT_NORMALIZE_TYPES = "normalize-types";

    private static final Arguments arguments;
    private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();

    static
    {
        arguments = new Arguments()
                .withDatabase()
                .withAdditionalConfig()
                .withArgument( new OptionalNamedArg( OPT_REPORT_FILE, "filename", DEFAULT_REPORT_FILE_NAME,
                    "File in which to store the report of the csv-import." ) )
                .withArgument( new OptionalNamedArgWithMetadata( OPT_NODES,
                    ":Label1:Label2",
                    "\"file1,file2,...\"", "",
                    "Node CSV header and data. Multiple files will be logically seen as " +
                            "one big file from the perspective of the importer. The first line " +
                            "must contain the header. Multiple data sources like these can be " +
                            "specified in one import, where each data source has its own header. " +
                    "Note that file groups must be enclosed in quotation marks." ) )
                .withArgument( new OptionalNamedArgWithMetadata( OPT_RELATIONSHIPS,
                    ":RELATIONSHIP_TYPE",
                    "\"file1,file2,...\"",
                    "",
                    "Relationship CSV header and data. Multiple files will be logically " +
                            "seen as one big file from the perspective of the importer. The first " +
                            "line must contain the header. Multiple data sources like these can be " +
                            "specified in one import, where each data source has its own header. " +
                    "Note that file groups must be enclosed in quotation marks." ) )
                .withArgument( new OptionalNamedArg( OPT_ID_TYPE, new String[]{"STRING", "INTEGER", "ACTUAL"},
                    "STRING", "Each node must provide a unique id. This is used to find the correct " +
                    "nodes when creating relationships. Possible values are:\n" +
                    "  STRING: arbitrary strings for identifying nodes,\n" +
                    "  INTEGER: arbitrary integer values for identifying nodes,\n" +
                    "  ACTUAL: (advanced) actual node ids.\n" +
                    "For more information on id handling, please see the Neo4j Manual: " +
                    "https://neo4j.com/docs/operations-manual/current/tools/import/" ) )
                .withArgument( new OptionalNamedArg( OPT_INPUT_ENCODING, "character-set", "UTF-8",
                    "Character set that input data is encoded in." ) )
                .withArgument( new OptionalBooleanArg( OPT_IGNORE_EXTRA_COLUMNS, false,
                    "If un-specified columns should be ignored during the import." ) )
                .withArgument( new OptionalBooleanArg( OPT_MULTILINE_FIELDS,
                    COMMAS.multilineFields(),
                    "Whether or not fields from input source can span multiple lines," +
                            " i.e. contain newline characters." ) )
                .withArgument( new OptionalNamedArg( OPT_DELIMITER,
                    "delimiter-character",
                    String.valueOf( COMMAS.delimiter() ),
                    "Delimiter character between values in CSV data." ) )
                .withArgument( new OptionalNamedArg( OPT_ARRAY_DELIMITER,
                    "array-delimiter-character",
                    String.valueOf( COMMAS.arrayDelimiter() ),
                    "Delimiter character between array elements within a value in CSV data." ) )
                .withArgument( new OptionalNamedArg( OPT_QUOTE,
                    "quotation-character",
                    String.valueOf( COMMAS.quotationCharacter() ),
                    "Character to treat as quotation character for values in CSV data. "
                            + "Quotes can be escaped as per RFC 4180 by doubling them, for example \"\" would be " +
                            "interpreted as a literal \". You cannot escape using \\." ) )
                .withArgument( new OptionalNamedArg( OPT_MAX_MEMORY,
                    "max-memory-that-importer-can-use",
                    DEFAULT_MAX_MEMORY_PERCENT + "%",
                    "Maximum memory that neo4j-admin can use for various data structures and caching " +
                            "to improve performance. " +
                            "Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%" +
                            "." ) )
                .withArgument( new OptionalNamedArg( "f",
                    "File containing all arguments to this import",
                    "",
                    "File containing all arguments, used as an alternative to supplying all arguments on the command line directly."
                            + "Each argument can be on a separate line or multiple arguments per line separated by space."
                            + "Arguments containing spaces needs to be quoted."
                            + "Supplying other arguments in addition to this file argument is not supported." ) )
                .withArgument( new OptionalBooleanArg( OPT_HIGH_IO,
                    true,
                    "Ignore environment-based heuristics, and assume that the target storage subsystem can support parallel IO with high throughput." ) )
                .withArgument( new OptionalNamedArg( OPT_BAD_TOLERANCE,
                        "max-number-of-bad-entries-or-'" + BadCollector.UNLIMITED_TOLERANCE + "'-for-unlimited",
                    "1000",
                    "Number of bad entries before the import is considered failed. This tolerance threshold is "
                            + "about relationships referring to missing nodes. Format errors in input data are "
                            + "still treated as errors" ) )
                .withArgument( new OptionalBooleanArg( OPT_CACHE_ON_HEAP,
                        DEFAULT.allowCacheAllocationOnHeap(),
                        "(advanced) Whether or not to allow allocating memory for the cache on heap. "
                            + "If 'false' then caches will still be allocated off-heap, but the additional free memory "
                            + "inside the JVM will not be allocated for the caches. This to be able to have better control "
                            + "over the heap memory" ) )
                .withArgument( new OptionalBooleanArg( OPT_DETAILED_PROGRESS,
                    false,
                    "Use the old detailed 'spectrum' progress printing" ) )
                .withArgument( new OptionalBooleanArg( OPT_IGNORE_EMPTY_STRINGS,
                        COMMAS.emptyQuotedStringsAsNull(),
                        "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null." ) )
                .withArgument( new OptionalBooleanArg( OPT_LEGACY_STYLE_QUOTING,
                    false,
                    "Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner quote." ) )
                .withArgument( new OptionalNamedArg( OPT_READ_BUFFER_SIZE,
                    "bytes, e.g. 10k, 4M",
                    Integer.toString( COMMAS.bufferSize() ),
                    "Size of each buffer for reading input data. It has to at least be large enough to hold the " +
                            "biggest single value in the input data." ) )
                .withArgument( new OptionalBooleanArg( OPT_SKIP_BAD_ENTRIES_LOGGING,
                    false,
                    "Whether or not to skip logging bad entries detected during import." ) )
                .withArgument( new OptionalBooleanArg( OPT_SKIP_BAD_RELATIONSHIPS,
                    false,
                    "Whether or not to skip importing relationships that refers to missing node ids, i.e. either "
                            + "start or end node id/group referring to node that wasn't specified by the "
                            + "node input data. Skipped nodes will be logged"
                            + ", containing at most number of entities specified by bad-tolerance, unless "
                            + "otherwise specified by skip-bad-entries-logging option." ) )
                .withArgument( new OptionalBooleanArg( OPT_SKIP_DUPLICATE_NODES,
                    false,
                    "Whether or not to skip importing nodes that have the same id/group. In the event of multiple "
                            + "nodes within the same group having the same id, the first encountered will be imported "
                            + "whereas consecutive such nodes will be skipped. "
                            + "Skipped nodes will be logged"
                            + ", containing at most number of entities specified by bad-tolerance, unless " +
                            "otherwise specified by skip-bad-entries-logging option." ) )
                .withArgument( new OptionalNamedArg( OPT_PROCESSORS,
                    "max processor count",
                    null,
                    "(advanced) Max number of processors used by the importer. Defaults to the number of "
                            + "available processors reported by the JVM skip-bad-entries-logging"
                            + ". There is a certain amount of minimum threads needed so for that reason there "
                            + "is no lower bound for this value. For optimal performance this value shouldn't be "
                            + "greater than the number of available processors." ) )
                .withArgument( new OptionalBooleanArg( OPT_TRIM_STRINGS,
                    true,
                    "Whether or not strings should be trimmed for whitespaces." ) )
                .withArgument( new OptionalBooleanArg( "normalize-types",
                        true,
                        "Whether or not to normalize property types to Cypher types, e.g. 'int' becomes 'long' and 'float' becomes 'double'" ) );
    }

    public static Arguments arguments()
    {
        return arguments;
    }

    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;

    public ImportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    @Override
    public void execute( String[] userSupplierArguments ) throws IncorrectUsage, CommandFailed
    {
        final String[] args;
        final Optional<Path> additionalConfigFile;
        final String database;

        try
        {
            args = getImportToolArgs( userSupplierArguments );
            arguments.parse( args );
            database = arguments.get( ARG_DATABASE );
            additionalConfigFile = arguments.getOptionalPath( "additional-config" );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        try
        {
            Config config = loadNeo4jConfig( additionalConfigFile );
            DatabaseLayout databaseLayout = DatabaseLayout.of( config.get( neo4j_home ), LayoutConfig.of( config ), database );

            final var parsedArgs = arguments.parsedArgs();
            final var csvConfig = csvConfiguration( parsedArgs, false );
            final var importConfig = importConfiguration( parsedArgs, databaseLayout );
            final var nodesFiles = extractInputFiles( parsedArgs, OPT_NODES, outsideWorld.errorStream() );
            final var relationshipsFiles = extractInputFiles( parsedArgs, OPT_RELATIONSHIPS, outsideWorld.errorStream() );
            final var reportFile = new File( parsedArgs.get( OPT_REPORT_FILE, ImportCommand.DEFAULT_REPORT_FILE_NAME ) ).getAbsoluteFile();
            final var ignoreExtraColumns = parsedArgs.getBoolean( OPT_IGNORE_EXTRA_COLUMNS );
            final var badTolerance = parsedArgs.getNumber( OPT_BAD_TOLERANCE, 1000 ).longValue();
            final var skipBadRelationships = parsedArgs.getBoolean( OPT_SKIP_BAD_RELATIONSHIPS );
            final var skipDuplicateNodes = parsedArgs.getBoolean( OPT_SKIP_DUPLICATE_NODES );
            final var skipBadEntriesLogging = parsedArgs.getBoolean( OPT_SKIP_BAD_ENTRIES_LOGGING );
            final var detailedProgress = parsedArgs.getBoolean( OPT_DETAILED_PROGRESS );
            final var idType = parsedArgs.interpretOption( OPT_ID_TYPE, withDefault( IdType.STRING ), from -> IdType.valueOf( from.toUpperCase() ) );
            final var inputEncoding = Charset.forName( parsedArgs.get( OPT_INPUT_ENCODING, defaultCharset().name() ) );
            final var normalizeTypes = parsedArgs.getBoolean( OPT_NORMALIZE_TYPES, true );

            try
            {
                validateInputFiles( nodesFiles, relationshipsFiles );
            }
            catch ( IllegalArgumentException e )
            {
                throw new IncorrectUsage( e.getMessage() );
            }

            final var importerBuilder = CsvImporter.builder()
                .withDatabaseLayout( databaseLayout )
                .withDatabaseConfig( config )
                .withOutsideWorld( outsideWorld )
                .withCsvConfig( csvConfig )
                .withImportConfig( importConfig )
                .withIdType( idType )
                .withInputEncoding( inputEncoding )
                .withReportFile( reportFile )
                .withIgnoreExtraColumns( ignoreExtraColumns )
                .withBadTolerance( badTolerance )
                .withSkipBadRelationships( skipBadRelationships )
                .withSkipDuplicateNodes( skipDuplicateNodes )
                .withSkipBadEntriesLogging( skipBadEntriesLogging )
                .withSkipBadRelationships( skipBadRelationships )
                .withVerbose( detailedProgress )
                .withNormalizeTypes( normalizeTypes );

            nodesFiles.forEach( opt ->
            {
                final var labels = Optional.ofNullable( opt.metadata() ).map( s -> s.split( ":" ) ).map( Set::of ).orElse( emptySet() );
                importerBuilder.addNodeFiles( labels, opt.value() );
            } );

            relationshipsFiles.forEach( opt ->
            {
                final var defaultType = Optional.ofNullable( opt.metadata() ).orElse( "" );
                importerBuilder.addRelationshipFiles( defaultType, opt.value() );
            } );

            final var importer = importerBuilder.build();
            importer.doImport();
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static String[] getImportToolArgs( String[] userSupplierArguments ) throws IOException, IncorrectUsage
    {
        arguments.parse( userSupplierArguments );
        Optional<Path> fileArgument = arguments.getOptionalPath( "f" );
        if ( fileArgument.isPresent() && userSupplierArguments.length > 2 )
        {
            throw new IncorrectUsage( "Supplying arguments in addition to --f isn't supported." );
        }
        return fileArgument.isPresent() ? parseFileArgumentList( fileArgument.get().toFile() ) : userSupplierArguments;
    }

    private static Config loadAdditionalConfig( Optional<Path> additionalConfigFile )
    {
        return additionalConfigFile.map( path -> Config.fromFile( path ).build() ).orElseGet( Config::defaults );
    }

    @VisibleForTesting
     Config loadNeo4jConfig( Optional<Path> additionalConfigFile )
    {
        Config config = Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled()
                .withNoThrowOnFileLoadFailure()
                .build();

        final var additionalConfig = loadAdditionalConfig( additionalConfigFile );

        // This is a temporary hack. Without this line there the loaded additionalConfig instance will always have the
        // neo4j_home setting set to the value of system property "user.dir", which overrides whatever homeDir that was given to
        // this command. At the time of writing this there's a configuration refactoring which will, among other things, fix this issue.
        additionalConfig.augment( GraphDatabaseSettings.neo4j_home, homeDir.toString() );

        config.augment( additionalConfig );
        return config;
    }

    private static String[] parseFileArgumentList( File file ) throws IOException
    {
        List<String> arguments = new ArrayList<>();
        readTextFile( file, line -> arguments.addAll( asList( tokenizeStringWithQuotes( line, true, true, false ) ) ) );
        return arguments.toArray( new String[0] );
    }

    private static org.neo4j.csv.reader.Configuration csvConfiguration( Args args, final boolean defaultSettingsSuitableForTests )
    {
        final var config = COMMAS.toBuilder();

        Optional.ofNullable( args.interpretOption( OPT_DELIMITER, Converters.optional(), CHARACTER_CONVERTER ) )
            .ifPresent( config::withDelimiter );

        Optional.ofNullable( args.interpretOption( OPT_ARRAY_DELIMITER, Converters.optional(), CHARACTER_CONVERTER ) )
            .ifPresent( config::withArrayDelimiter );

        Optional.ofNullable( args.interpretOption( OPT_QUOTE, Converters.optional(), CHARACTER_CONVERTER ) )
            .ifPresent( config::withQuotationCharacter );

        Optional.ofNullable( args.getBoolean( OPT_MULTILINE_FIELDS, null ) )
            .ifPresent( config::withMultilineFields );

        Optional.ofNullable( args.getBoolean( OPT_IGNORE_EMPTY_STRINGS, null ) )
            .ifPresent( config::withEmptyQuotedStringsAsNull );

        Optional.ofNullable( args.getBoolean( OPT_TRIM_STRINGS, null ) )
            .ifPresent( config::withTrimStrings );

        Optional.ofNullable( args.getBoolean( OPT_LEGACY_STYLE_QUOTING, null ) )
            .ifPresent( config::withLegacyStyleQuoting );

        Optional.ofNullable( args.get( OPT_READ_BUFFER_SIZE, null ) )
            .map( s -> toIntExact( parseLongWithUnit( s ) ) )
            .ifPresentOrElse( config::withBufferSize, () ->
            {
                if ( defaultSettingsSuitableForTests )
                {
                    config.withBufferSize( 10_000 );
                }
            } );

        return config.build();
    }

    private static org.neo4j.internal.batchimport.Configuration importConfiguration( Args args, DatabaseLayout databaseLayout )
    {
        final var processors = args.getNumber( OPT_PROCESSORS, Configuration.DEFAULT.maxNumberOfProcessors() );
        final var maxMemory = parseMaxMemory( args.get( OPT_MAX_MEMORY, Configuration.DEFAULT_MAX_MEMORY_PERCENT + "%", null ) );
        final var highIO = args.getBoolean( OPT_HIGH_IO, Configuration.DEFAULT.highIO() );
        final var cacheOnHeap = args.getBoolean( OPT_CACHE_ON_HEAP );

        return new org.neo4j.internal.batchimport.Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : DEFAULT.maxNumberOfProcessors();
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory : DEFAULT.maxMemoryUsage();
            }

            @Override
            public boolean highIO()
            {
                return highIO != null ? highIO : FileUtils.highIODevice( databaseLayout.databaseDirectory().toPath(), false );
            }

            @Override
            public boolean allowCacheAllocationOnHeap()
            {
                return cacheOnHeap;
            }
        };
    }

    private static Long parseMaxMemory( String maxMemoryString )
    {
        if ( maxMemoryString != null )
        {
            maxMemoryString = maxMemoryString.trim();
            if ( maxMemoryString.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                long result = calculateMaxMemoryFromPercent( percent );
                if ( !canDetectFreeMemory() )
                {
                    System.err.println( "WARNING: amount of free memory couldn't be detected so defaults to " +
                        bytesToString( result ) + ". For optimal performance instead explicitly specify amount of " +
                        "memory that importer is allowed to use using " + OPT_MAX_MEMORY );
                }
                return result;
            }
            return parseLongWithUnit( maxMemoryString );
        }
        return null;
    }

    private static Collection<Args.Option<File[]>> extractInputFiles( Args args, String key, PrintStream err )
    {
        return args
            .interpretOptionsWithMetadata( key, Converters.optional(),
                Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ),
                filesExist( err ),
                Validators.atLeast( "--" + key, 1 ) );
    }

    private static Validator<File[]> filesExist( PrintStream err )
    {
        return files ->
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    err.println( "It looks like you're trying to specify default label or relationship type (" +
                        file.getName() + "). Please put such directly on the key, f.ex. " +
                        OPT_NODES + ":MyLabel" );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        };
    }

    private static void validateInputFiles( Collection<Args.Option<File[]>> nodesFiles,
        Collection<Args.Option<File[]>> relationshipsFiles )
    {
        if ( nodesFiles.isEmpty() )
        {
            if ( relationshipsFiles.isEmpty() )
            {
                throw new IllegalArgumentException( "No input specified, nothing to import" );
            }
            throw new IllegalArgumentException( "No node input specified, cannot import relationships without nodes" );
        }
    }
}
