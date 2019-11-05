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

import org.eclipse.collections.api.tuple.Pair;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.ByteUnitConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.parseLongWithUnit;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.importer.CsvImporter.DEFAULT_REPORT_FILE_NAME;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.internal.batchimport.Configuration.canDetectFreeMemory;
import static org.neo4j.io.ByteUnit.bytesToString;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Help.Visibility.NEVER;

@Command(
        name = "import",
        description = "Import a collection of CSV files."
)
@SuppressWarnings( "FieldMayBeFinal" )
public class ImportCommand extends AbstractCommand
{
    /**
     * Delimiter used between files in an input group.
     */
    private static final String MULTI_FILE_DELIMITER = ",";
    private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();
    private static final org.neo4j.csv.reader.Configuration DEFAULT_CSV_CONFIG = COMMAS;
    private static final Configuration DEFAULT_IMPORTER_CONFIG = DEFAULT;

    @Option( names = "--database", description = "Name of the database to import." )
    private String database = DEFAULT_DATABASE_NAME;

    @Option( names = "--additional-config", paramLabel = "<path>", description = "Configuration file to supply additional configuration in." )
    private Path additionalConfig;

    @Option( names = "--report-file", paramLabel = "<path>", defaultValue = DEFAULT_REPORT_FILE_NAME,
            description = "File in which to store the report of the csv-import." )
    private File reportFile = new File( DEFAULT_REPORT_FILE_NAME );

    @Option( names = "--id-type", paramLabel = "<STRING|INTEGER|ACTUAL>", description = "Each node must provide a unique id. This is used to find the " +
            "correct nodes when creating relationships. Possible values are:%n" +
            "  STRING: arbitrary strings for identifying nodes,%n" +
            "  INTEGER: arbitrary integer values for identifying nodes,%n" +
            "  ACTUAL: (advanced) actual node ids.%n" +
            "For more information on id handling, please see the Neo4j Manual: " +
            "https://neo4j.com/docs/operations-manual/current/tools/import/" )
    private IdType idType = IdType.STRING;

    @Option( names = "--input-encoding", paramLabel = "<character-set>", description = "Character set that input data is encoded in." )
    private Charset inputEncoding = StandardCharsets.UTF_8;

    @Option( names = "--ignore-extra-columns", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "If un-specified columns should be ignored during the import." )
    private boolean ignoreExtraColumns;

    @Option( names = "--multiline-fields", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not fields from input source can span multiple lines, i.e. contain newline characters." )
    private boolean multilineFields = DEFAULT_CSV_CONFIG.multilineFields();

    @Option( names = "--ignore-empty-strings", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null." )
    private boolean ignoreEmptyStrings = DEFAULT_CSV_CONFIG.emptyQuotedStringsAsNull();

    @Option( names = "--trim-strings", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not strings should be trimmed for whitespaces." )
    private boolean trimStrings = DEFAULT_CSV_CONFIG.trimStrings();

    @Option( names = "--legacy-style-quoting", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner quote." )
    private boolean legacyStyleQuoting = DEFAULT_CSV_CONFIG.legacyStyleQuoting();

    @Option( names = "--delimiter", paramLabel = "<char>", converter = EscapedCharacterConverter.class,
            description = "Delimiter character between values in CSV data." )
    private char delimiter = DEFAULT_CSV_CONFIG.delimiter();

    @Option( names = "--array-delimiter", paramLabel = "<char>", converter = EscapedCharacterConverter.class,
            description = "Delimiter character between array elements within a value in CSV data." )
    private char arrayDelimiter = DEFAULT_CSV_CONFIG.arrayDelimiter();

    @Option( names = "--quote", paramLabel = "<char>", converter = EscapedCharacterConverter.class,
            description = "Character to treat as quotation character for values in CSV data. Quotes can be escaped as per RFC 4180 by doubling them, " +
                    "for example \"\" would be interpreted as a literal \". You cannot escape using \\." )
    private char quote = DEFAULT_CSV_CONFIG.quotationCharacter();

    @Option( names = "--read-buffer-size", paramLabel = "<size>", converter = ByteUnitConverter.class,
            description = "Size of each buffer for reading input data. It has to at least be large enough to hold the biggest single value in the input data." )
    private long bufferSize = DEFAULT_CSV_CONFIG.bufferSize();

    @Option( names = "--max-memory", paramLabel = "<size>", defaultValue = "90%", converter = MemoryConverter.class,
            description = "Maximum memory that neo4j-admin can use for various data structures and caching to improve performance. " +
                    "Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%." )
    private long maxMemory;

    @Option( names = "--high-io", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Ignore environment-based heuristics, and assume that the target storage subsystem can support parallel IO with high throughput." )
    private boolean highIo = DEFAULT_IMPORTER_CONFIG.highIO();

    @Option( names = "--cache-on-heap", showDefaultValue = ALWAYS, arity = "0..1", paramLabel = "<true/false>",
            description = "(advanced) Whether or not to allow allocating memory for the cache on heap. If 'false' then caches will still be " +
                    "allocated off-heap, but the additional free memory inside the JVM will not be allocated for the caches. This to be able to have better " +
                    "control over the heap memory" )
    private boolean cacheOnHeap = DEFAULT_IMPORTER_CONFIG.allowCacheAllocationOnHeap();

    @Option( names = "--processors", paramLabel = "<num>",
            description = "(advanced) Max number of processors used by the importer. Defaults to the number of available processors reported by the JVM. " +
                    "There is a certain amount of minimum threads needed so for that reason there is no lower bound for this " +
                    "value. For optimal performance this value shouldn't be greater than the number of available processors." )
    private int processors = DEFAULT_IMPORTER_CONFIG.maxNumberOfProcessors();

    @Option( names = "--bad-tolerance", paramLabel = "<num>",
            description = "Number of bad entries before the import is considered failed. This tolerance threshold is about relationships referring to " +
                    "missing nodes. Format errors in input data are still treated as errors" )
    private long badTolerance = 1000;

    @Option( names = "--skip-bad-entries-logging", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not to skip logging bad entries detected during import." )
    private boolean skipBadEntriesLogging;

    @Option( names = "--skip-bad-relationships", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not to skip importing relationships that refers to missing node ids, i.e. either start or end node id/group referring " +
                    "to node that wasn't specified by the node input data. Skipped nodes will be logged, containing at most number of entities " +
                    "specified by bad-tolerance, unless otherwise specified by skip-bad-entries-logging option." )
    private boolean skipBadRelationships;

    @Option( names = "--skip-duplicate-nodes", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not to skip importing nodes that have the same id/group. In the event of multiple nodes within the same group having " +
                    "the same id, the first encountered will be imported whereas consecutive such nodes will be skipped. Skipped nodes will be logged, " +
                    "containing at most number of entities specified by bad-tolerance, unless otherwise specified by skip-bad-entries-logging option." )
    private boolean skipDuplicateNodes;

    @Option( names = "--normalize-types", arity = "0..1", showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Whether or not to normalize property types to Cypher types, e.g. 'int' becomes 'long' and 'float' becomes 'double'" )
    private boolean normalizeTypes = true;

    @Option( names = "--nodes", required = true, arity = "1..*", converter = NodeFilesConverter.class, paramLabel = "[<label>[:<label>]...=]<files>",
            description = "Node CSV header and data. Multiple files will be logically seen as one big file from the perspective of the importer. The first " +
                    "line must contain the header. Multiple data sources like these can be specified in one import, where each data source has its " +
                    "own header." )
    private List<NodeFilesGroup> nodes;

    @Option( names = "--relationships", arity = "1..*", converter = RelationsipFilesConverter.class, showDefaultValue = NEVER, paramLabel = "[<type>=]<files>",
            description = "Relationship CSV header and data. Multiple files will be logically seen as one big file from the perspective of the importer. " +
                    "The first line must contain the header. Multiple data sources like these can be specified in one import, where each data source has " +
                    "its own header." )
    private List<RelationshipFilesGroup> relationships = new ArrayList<>();

    public ImportCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute()
    {
        try
        {
            final var databaseConfig = loadNeo4jConfig();
            final var databaseLayout = Neo4jLayout.of( databaseConfig ).databaseLayout( database );
            final var csvConfig = csvConfiguration();
            final var importConfig = importConfiguration();

            final var importerBuilder = CsvImporter.builder()
                    .withDatabaseLayout( databaseLayout )
                    .withDatabaseConfig( databaseConfig )
                    .withFileSystem( ctx.fs() )
                    .withStdOut( ctx.out() )
                    .withStdErr( ctx.err() )
                    .withCsvConfig( csvConfig )
                    .withImportConfig( importConfig )
                    .withIdType( idType )
                    .withInputEncoding( inputEncoding )
                    .withReportFile( reportFile.getAbsoluteFile() )
                    .withIgnoreExtraColumns( ignoreExtraColumns )
                    .withBadTolerance( badTolerance )
                    .withSkipBadRelationships( skipBadRelationships )
                    .withSkipDuplicateNodes( skipDuplicateNodes )
                    .withSkipBadEntriesLogging( skipBadEntriesLogging )
                    .withSkipBadRelationships( skipBadRelationships )
                    .withNormalizeTypes( normalizeTypes )
                    .withVerbose( verbose );

            nodes.forEach( n -> {
                importerBuilder.addNodeFiles( n.key, n.files );
            } );

            relationships.forEach( n -> {
                importerBuilder.addRelationshipFiles( n.key, n.files );
            } );

            final var importer = importerBuilder.build();
            importer.doImport();
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @VisibleForTesting
    Config loadNeo4jConfig()
    {
        Config cfg = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath() )
                .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .fromFileNoThrow( additionalConfig )
                .build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }

    private org.neo4j.csv.reader.Configuration csvConfiguration()
    {
        return DEFAULT_CSV_CONFIG.toBuilder()
                .withDelimiter( delimiter )
                .withArrayDelimiter( arrayDelimiter )
                .withQuotationCharacter( quote )
                .withMultilineFields( multilineFields )
                .withEmptyQuotedStringsAsNull( ignoreEmptyStrings )
                .withTrimStrings( trimStrings )
                .withLegacyStyleQuoting( legacyStyleQuoting )
                .withBufferSize( toIntExact( bufferSize ) )
                .build();
    }

    private org.neo4j.internal.batchimport.Configuration importConfiguration()
    {
        return new org.neo4j.internal.batchimport.Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return processors;
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory;
            }

            @Override
            public boolean highIO()
            {
                return highIo;
            }

            @Override
            public boolean allowCacheAllocationOnHeap()
            {
                return cacheOnHeap;
            }
        };
    }

    @VisibleForTesting
    static RelationshipFilesGroup parseRelationshipFilesGroup( String str )
    {
        final var p = parseInputFilesGroup( str, String::trim );
        return new RelationshipFilesGroup( p.getOne(), p.getTwo() );
    }

    @VisibleForTesting
    static NodeFilesGroup parseNodeFilesGroup( String str )
    {
        final var p = parseInputFilesGroup( str, s -> stream( s.split( ":" ) )
                .map( String::trim )
                .filter( x -> !x.isEmpty() )
                .collect( toSet() ) );
        return new NodeFilesGroup( p.getOne(), p.getTwo() );
    }

    private static <T> Pair<T, File[]> parseInputFilesGroup( String str, Function<String, ? extends T> keyParser )
    {
        final var i = str.indexOf( '=' );
        if ( i < 0 )
        {
            return pair( keyParser.apply( "" ), parseFilesList( str ) );
        }
        if ( i == 0 || i == str.length() - 1 )
        {
            throw new IllegalArgumentException( "illegal `=` position: " + str );
        }
        final var keyStr = str.substring( 0, i );
        final var filesStr = str.substring( i + 1 );
        final var key = keyParser.apply( keyStr );
        final var files = parseFilesList( filesStr );
        return pair( key, files );
    }

    private static File[] parseFilesList( String str )
    {
        final var converter = Converters.regexFiles( true );
        return Converters.toFiles( MULTI_FILE_DELIMITER, s ->
        {
            Validators.REGEX_FILE_EXISTS.validate( new File( s ) );
            return converter.apply( s );
        } ).apply( str );
    }

    static class MemoryConverter implements ITypeConverter<Long>
    {
        @Override
        public Long convert( String value )
        {
            value = value.trim();
            if ( value.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( value.substring( 0, value.length() - 1 ) );
                long result = calculateMaxMemoryFromPercent( percent );
                if ( !canDetectFreeMemory() )
                {
                    System.err.println( "WARNING: amount of free memory couldn't be detected so defaults to " +
                            bytesToString( result ) + ". For optimal performance instead explicitly specify amount of " +
                            "memory that importer is allowed to use using --max-memory" );
                }
                return result;
            }
            return parseLongWithUnit( value );
        }
    }

    static class EscapedCharacterConverter implements ITypeConverter<Character>
    {

        @Override
        public Character convert( String value )
        {
            return CHARACTER_CONVERTER.apply( value );
        }
    }

    static class NodeFilesConverter implements ITypeConverter<NodeFilesGroup>
    {
        @Override
        public NodeFilesGroup convert( String value )
        {
            try
            {
                return parseNodeFilesGroup( value );
            }
            catch ( Exception e )
            {
                throw new CommandLine.TypeConversionException( format( "Invalid nodes file: %s (%s)", value, e ) );
            }
        }
    }

    static class RelationsipFilesConverter implements ITypeConverter<InputFilesGroup<String>>
    {
        @Override
        public InputFilesGroup<String> convert( String value )
        {
            try
            {
                return parseRelationshipFilesGroup( value );
            }
            catch ( Exception e )
            {
                throw new CommandLine.TypeConversionException( format( "Invalid relationships file: %s (%s)", value, e ) );
            }
        }
    }

    static class NodeFilesGroup extends InputFilesGroup<Set<String>>
    {
        NodeFilesGroup( Set<String> key, File[] files )
        {
            super( key, files );
        }
    }

    static class RelationshipFilesGroup extends InputFilesGroup<String>
    {
        RelationshipFilesGroup( String key, File[] files )
        {
            super( key, files );
        }
    }

    abstract static class InputFilesGroup<T>
    {
        final T key;
        final File[] files;

        InputFilesGroup( T key, File[] files )
        {
            this.key = key;
            this.files = files;
        }
    }
}
