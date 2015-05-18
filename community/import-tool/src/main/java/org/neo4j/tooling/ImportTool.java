/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.function.Function;
import org.neo4j.function.Function2;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.System.out;
import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.Converters.withDefault;
import static org.neo4j.unsafe.impl.batchimport.Configuration.BAD_FILE_NAME;
import static org.neo4j.unsafe.impl.batchimport.cache.AvailableMemoryCalculator.RUNTIME;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.collect;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

/**
 * User-facing command line tool around a {@link BatchImporter}.
 */
public class ImportTool
{
    enum Options
    {
        STORE_DIR( "into", null,
                "<store-dir>",
                "Database directory to import into. " + "Must not contain existing database." ),
        NODE_DATA( "nodes", null,
                "[:Label1:Label2] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" + MULTI_FILE_DELIMITER + "...\"",
                "Node CSV header and data. Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks." ),
        RELATIONSHIP_DATA( "relationships", null,
                "[:RELATIONSHIP_TYPE] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" +
                MULTI_FILE_DELIMITER + "...\"",
                "Relationship CSV header and data. Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks." ),
        DELIMITER( "delimiter", null,
                "<delimiter-character>",
                "Delimiter character, or 'TAB', between values in CSV data. The default option is `" + COMMAS.delimiter() + "`." ),
        ARRAY_DELIMITER( "array-delimiter", null,
                "<array-delimiter-character>",
                "Delimiter character, or 'TAB', between array elements within a value in CSV data. The default option is `" + COMMAS.arrayDelimiter() + "`." ),
        QUOTE( "quote", null,
                "<quotation-character>",
                "Character to treat as quotation character for values in CSV data. "
                        + "The default option is `" + COMMAS.quotationCharacter() + "`. "
                        + "Quotes inside quotes escaped like `\"\"\"Go away\"\", he said.\"` and "
                        + "`\"\\\"Go away\\\", he said.\"` are supported. "
                        + "If you have set \"`'`\" to be used as the quotation character, "
                        + "you could write the previous example like this instead: " + "`'\"Go away\", he said.'`" ),
        MULTILINE_FIELDS( "multiline-fields", org.neo4j.csv.reader.Configuration.DEFAULT.multilineFields(),
                "<true/false>",
                "Whether or not fields from input source can span multiple lines, i.e. contain newline characters." ),
        ID_TYPE( "id-type", IdType.STRING,
                "<id-type>",
                "One out of " + Arrays.toString( IdType.values() )
                        + " and specifies how ids in node/relationship "
                        + "input files are treated.\n"
                        + IdType.STRING + ": arbitrary strings for identifying nodes.\n"
                        + IdType.INTEGER + ": arbitrary integer values for identifying nodes.\n"
                        + IdType.ACTUAL + ": (advanced) actual node ids. The default option is `" + IdType.STRING  + "`." ),
        PROCESSORS( "processors", null,
                "<max processor count>",
                "(advanced) Max number of processors used by the importer. Defaults to the number of "
                        + "available processors reported by the JVM"
                        + availableProcessorsHint()
                        + ". There is a certain amount of minimum threads needed so for that reason there "
                        + "is no lower bound for this value. For optimal performance this value shouldn't be "
                        + "greater than the number of available processors." ),
        STACKTRACE( "stacktrace", null,
                "<true/false>",
                "Enable printing of error stack traces." ),
        BAD_TOLERANCE( "bad-tolerance", 1000,
                "<max number of bad entries>",
                "Number of bad entries before the import is considered failed. This tolerance threshold is "
                        + "about relationships refering to missing nodes. Format errors in input data are "
                        + "still treated as errors" ),

        INPUT_ENCODING( "input-encoding", null,
                "<character set>",
                "Character set that input data is encoded in. Provided value must be one out of the available "
                        + "character sets in the JVM, as provided by Charset#availableCharsets(). "
                        + "If no input encoding is provided, the default character set of the JVM will be used." ),

        SKIP_BAD_RELATIONSHIPS( "skip-bad-relationships", Boolean.TRUE,
                "<true/false>",
                "Whether or not to skip importing relationships that refers to missing node ids, i.e. either "
                        + "start or end node id/group referring to node that wasn't specified by the "
                        + "node input data. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entites specified by " + BAD_TOLERANCE.key() + "." ),
        SKIP_DUPLICATE_NODES( "skip-duplicate-nodes", Boolean.FALSE,
                "<true/false>",
                "Whether or not to skip importing nodes that have the same id/group. In the event of multiple "
                        + "nodes within the same group having the same id, the first encountered will be imported "
                        + "whereas consecutive such nodes will be skipped. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entites specified by " + BAD_TOLERANCE.key() + "." );

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;

        Options( String key, Object defaultValue, String usage, String description )
        {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
        }

        String key()
        {
            return key;
        }

        String argument()
        {
            return "--" + key();
        }

        void printUsage( PrintStream out )
        {
            out.println( argument() + " " + usage );
            for ( String line : Args.splitLongLine( descriptionWithDefaultValue().replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        String descriptionWithDefaultValue()
        {
            String result = description;
            if ( defaultValue != null )
            {
                if ( !result.endsWith( "." ) )
                {
                    result += ". ";
                }
                result += "Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry()
        {
            String filteredDescription = descriptionWithDefaultValue().replace( availableProcessorsHint(), "" );
            String usageString = (usage.length() > 0) ? " " + usage : "";
            return "*" + argument() + usageString + "*::\n" + filteredDescription + "\n\n";
        }

        Object defaultValue()
        {
            return defaultValue;
        }

        private static String availableProcessorsHint()
        {
            return " (in your case " + Runtime.getRuntime().availableProcessors() + ")";
        }
    }

    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     */
    public static void main( String[] incomingArguments )
    {
        main( incomingArguments, false );
    }

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     * @param defaultSettingsSuitableForTests default configuration geared towards unit/integration
     * test environments, for example lower default buffer sizes.
     */
    public static void main( String[] incomingArguments, boolean defaultSettingsSuitableForTests )
    {
        Args args = Args.parse( incomingArguments );
        if ( asksForUsage( args ) )
        {
            printUsage( System.out );
            return;
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir;
        Collection<Option<File[]>> nodesFiles, relationshipsFiles;
        boolean enableStacktrace;
        Number processors = null;
        Input input = null;
        int badTolerance;
        Charset inputEncoding;
        boolean skipBadRelationships, skipDuplicateNodes;

        try
        {
            storeDir = args.interpretOption( Options.STORE_DIR.key(), Converters.<File>mandatory(),
                    Converters.toFile(), Validators.DIRECTORY_IS_WRITABLE, Validators.CONTAINS_NO_EXISTING_DATABASE );
            nodesFiles = INPUT_FILES_EXTRACTOR.apply( args, Options.NODE_DATA.key() );
            relationshipsFiles = INPUT_FILES_EXTRACTOR.apply( args, Options.RELATIONSHIP_DATA.key() );
            validateInputFiles( nodesFiles, relationshipsFiles );
            enableStacktrace = args.getBoolean( Options.STACKTRACE.key(), Boolean.FALSE, Boolean.TRUE );
            processors = args.getNumber( Options.PROCESSORS.key(), null );
            IdType idType = args.interpretOption( Options.ID_TYPE.key(),
                    withDefault( (IdType)Options.ID_TYPE.defaultValue() ), TO_ID_TYPE );
            badTolerance = args.getNumber( Options.BAD_TOLERANCE.key(),
                    (Number) Options.BAD_TOLERANCE.defaultValue() ).intValue();
            inputEncoding = Charset.forName( args.get( Options.INPUT_ENCODING.key(), defaultCharset().name() ) );
            skipBadRelationships = args.getBoolean( Options.SKIP_BAD_RELATIONSHIPS.key(),
                    (Boolean)Options.SKIP_BAD_RELATIONSHIPS.defaultValue(), true );
            skipDuplicateNodes = args.getBoolean( Options.SKIP_DUPLICATE_NODES.key(),
                    (Boolean)Options.SKIP_DUPLICATE_NODES.defaultValue(), true );
            input = new CsvInput(
                    nodeData( inputEncoding, nodesFiles ), defaultFormatNodeFileHeader(),
                    relationshipData( inputEncoding, relationshipsFiles ), defaultFormatRelationshipFileHeader(),
                    idType, csvConfiguration( args, defaultSettingsSuitableForTests ),
                    badCollector( badTolerance, collect( skipBadRelationships, skipDuplicateNodes ) ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false );
        }

        LifeSupport life = new LifeSupport();
        Logging logging = life.add( new ClassicLoggingService(
                new Config( stringMap( store_dir.name(), storeDir.getAbsolutePath() ) ) ) );
        life.start();
        org.neo4j.unsafe.impl.batchimport.Configuration config =
                importConfiguration( processors, defaultSettingsSuitableForTests );
        BatchImporter importer = new ParallelBatchImporter( storeDir.getPath(),
                config,
                logging,
                ExecutionMonitors.defaultVisible() );
        printOverview( storeDir, nodesFiles, relationshipsFiles );
        boolean success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( Exception e )
        {
            throw andPrintError( "Import error", e, enableStacktrace );
        }
        finally
        {
            File badFile = new File( storeDir, BAD_FILE_NAME );
            if ( badFile.exists() )
            {
                out.println( "There were bad entries which were skipped and logged into " +
                        badFile.getAbsolutePath() );
            }

            life.shutdown();
            if ( !success )
            {
                try
                {
                    StoreFile.fileOperation( FileOperation.DELETE, fs, storeDir, null,
                            Iterables.<StoreFile,StoreFile>iterable( StoreFile.values() ),
                            false, false, StoreFileType.values() );
                }
                catch ( IOException e )
                {
                    System.err.println( "Unable to delete store files after an aborted import " + e );
                    if ( enableStacktrace )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static void printOverview( File storeDir, Collection<Option<File[]>> nodesFiles,
            Collection<Option<File[]>> relationshipsFiles )
    {
        System.out.println( "Importing the contents of these files into " + storeDir + ":" );
        printInputFiles( "Nodes", nodesFiles );
        printInputFiles( "Relationships", relationshipsFiles );
        System.out.println();
        System.out.println( "Available memory:" );
        printIndented( "Free machine memory: " + bytes( RUNTIME.availableOffHeapMemory() ) );
        printIndented( "Max heap memory : " + bytes( Runtime.getRuntime().maxMemory() ) );
        System.out.println();
    }

    private static void printInputFiles( String name, Collection<Option<File[]>> files )
    {
        if ( files.isEmpty() )
        {
            return;
        }

        System.out.println( name + ":" );
        int i = 0;
        for ( Option<File[]> group : files )
        {
            if ( i++ > 0 )
            {
                System.out.println();
            }
            if ( group.metadata() != null )
            {
                printIndented( ":" + group.metadata() );
            }
            for ( File file : group.value() )
            {
                printIndented( file );
            }
        }
    }

    private static void printIndented( Object value )
    {
        System.out.println( "  " + value );
    }

    private static void validateInputFiles( Collection<Option<File[]>> nodesFiles,
            Collection<Option<File[]>> relationshipsFiles )
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

    private static org.neo4j.unsafe.impl.batchimport.Configuration importConfiguration( final Number processors,
            final boolean defaultSettingsSuitableForTests )
    {
        return new org.neo4j.unsafe.impl.batchimport.Configuration.Default()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : super.maxNumberOfProcessors();
            }

            @Override
            public int bigFileChannelBufferSizeMultiplier()
            {
                return defaultSettingsSuitableForTests ? 1 : super.bigFileChannelBufferSizeMultiplier();
            }
        };
    }

    private static String manualReference( String page )
    {
        return " http://neo4j.com/docs/" + Version.getKernel().getVersion() + "/" + page;
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     * @param stackTrace whether or not to also print the stack trace of the error.
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace )
    {
        if ( e.getClass().equals( DuplicateInputIdException.class ) )
        {
            System.err.println( "Duplicate input ids that would otherwise clash can be put into separate id space," +
                    " read more about how to use id spaces in the manual:" +
                    manualReference( "import-tool-header-format.html#_id_spaces" ) );
        }
        else if ( e.getClass().equals( IllegalMultilineFieldException.class ) )
        {
            System.err.println( "Detected field which spanned multiple lines for an import where " +
                    Options.MULTILINE_FIELDS.argument() + "=false. If you know that your input data include " +
                    "fields containing new-line characters then import with this option set to true." );
        }

        System.err.println( typeOfError + ": " + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( System.err );
        }
        System.err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( new UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException( Thread t, Throwable e )
            {   // Shhhh
            }
        } );
        return launderedException( e ); // throw in order to have process exit with !0
    }

    private static Iterable<DataFactory<InputRelationship>>
            relationshipData( final Charset encoding, Collection<Option<File[]>> relationshipsFiles )
    {
        return new IterableWrapper<DataFactory<InputRelationship>,Option<File[]>>( relationshipsFiles )
        {
            @Override
            protected DataFactory<InputRelationship> underlyingObjectToObject( Option<File[]> group )
            {
                return data( defaultRelationshipType( group.metadata() ), encoding, group.value() );
            }
        };
    }

    private static Iterable<DataFactory<InputNode>> nodeData( final Charset encoding,
            Collection<Option<File[]>> nodesFiles )
    {
        return new IterableWrapper<DataFactory<InputNode>,Option<File[]>>( nodesFiles )
        {
            @Override
            protected DataFactory<InputNode> underlyingObjectToObject( Option<File[]> input )
            {
                Function<InputNode,InputNode> decorator = input.metadata() != null
                        ? additiveLabels( input.metadata().split( ":" ) )
                        : NO_NODE_DECORATOR;
                return data( decorator, encoding, input.value() );
            }
        };
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Import Tool" );
        for ( String line : Args.splitLongLine( "neo4j-import is used to create a new Neo4j database "
                + "from data in CSV files. "
                + "See the chapter \"Import Tool\" in the Neo4j Manual for details on the CSV file format "
                + "- a special kind of header is required.", 80 ) )
        {
            out.println( "\t" + line );
        }
        out.println( "Usage:" );
        for ( Options option : Options.values() )
        {
            option.printUsage( out );
        }
    }

    private static boolean asksForUsage( Args args )
    {
        for ( String orphan : args.orphans() )
        {
            if ( isHelpKey( orphan ) )
            {
                return true;
            }
        }

        for ( Entry<String,String> option : args.asMap().entrySet() )
        {
            if ( isHelpKey( option.getKey() ) )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isHelpKey( String key )
    {
        return key.equals( "?" ) || key.equals( "help" );
    }

    private static Configuration csvConfiguration( Args args, final boolean defaultSettingsSuitableForTests )
    {
        final Configuration defaultConfiguration = COMMAS;
        final Character specificDelimiter =
                args.interpretOption( Options.DELIMITER.key(), Converters.<Character>optional(), DELIMITER_CONVERTER );
        final Character specificArrayDelimiter =
                args.interpretOption( Options.ARRAY_DELIMITER.key(), Converters.<Character>optional(), DELIMITER_CONVERTER );
        final Character specificQuote =
                args.interpretOption( Options.QUOTE.key(), Converters.<Character>optional(), Converters.toCharacter() );
        return new Configuration.Default()
        {
            @Override
            public char delimiter()
            {
                return specificDelimiter != null
                        ? specificDelimiter.charValue()
                        : defaultConfiguration.delimiter();
            }

            @Override
            public char arrayDelimiter()
            {
                return specificArrayDelimiter != null
                        ? specificArrayDelimiter.charValue()
                        : defaultConfiguration.arrayDelimiter();
            }

            @Override
            public char quotationCharacter()
            {
                return specificQuote != null
                        ? specificQuote.charValue()
                        : defaultConfiguration.quotationCharacter();
            }

            @Override
            public int bufferSize()
            {
                return defaultSettingsSuitableForTests ? 10_000 : super.bufferSize();
            }
        };
    }

    private static final Function<String,IdType> TO_ID_TYPE = new Function<String,IdType>()
    {
        @Override
        public IdType apply( String from )
        {
            return IdType.valueOf( from.toUpperCase() );
        }
    };

    private static final Function<String,Character> DELIMITER_CONVERTER = new Function<String,Character>()
    {
        private final Function<String,Character> fallback = Converters.toCharacter();

        @Override
        public Character apply( String value ) throws RuntimeException
        {
            if ( value.equals( "TAB" ) )
            {
                return '\t';
            }
            return fallback.apply( value );
        }
    };

    private static final Function2<Args,String,Collection<Option<File[]>>> INPUT_FILES_EXTRACTOR =
            new Function2<Args,String,Collection<Option<File[]>>>()
    {
        @Override
        public Collection<Option<File[]>> apply( Args args, String key )
        {
            return args.interpretOptionsWithMetadata( key, Converters.<File[]>optional(),
                    Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ), FILES_EXISTS,
                    Validators.<File>atLeast( "--" + key, 1 ) );
        }
    };

    static final Validator<File[]> FILES_EXISTS = new Validator<File[]>()
    {
        @Override
        public void validate( File[] files )
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    warn( "It looks like you're trying to specify default label or relationship type (" +
                            file.getName() + "). Please put such directly on the key, f.ex. " +
                            Options.NODE_DATA.argument() + ":MyLabel" );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        }
    };

    static void warn( String warning )
    {
        System.err.println( warning );
    }
}
