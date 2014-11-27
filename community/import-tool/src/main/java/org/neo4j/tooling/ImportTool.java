/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.Converters.withDefault;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
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
    private static final Function<String,IdType> TO_ID_TYPE = new Function<String,IdType>()
    {
        @Override
        public IdType apply( String from )
        {
            return IdType.valueOf( from.toUpperCase() );
        }
    };

    enum Options
    {
        STORE_DIR( "into", "<store-dir>", "Database directory to import into. " + "Must not contain existing database." ),
        NODE_DATA(
                "nodes",
                "\"<file1>" + MULTI_FILE_DELIMITER + "<file2>" + MULTI_FILE_DELIMITER + "...\"",
                "Node CSV header and data. "
                        + "Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks." ),
        RELATIONSHIP_DATA(
                "relationships",
                "\"<file1>" + MULTI_FILE_DELIMITER + "<file2>" + MULTI_FILE_DELIMITER + "...\"",
                "Relationship CSV header and data. "
                + "Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks." ),
        DELIMITER( "delimiter", "<delimiter-character>", "Delimiter character, or 'TAB', between values in CSV data." ),
        ARRAY_DELIMITER( "array-delimiter", "<array-delimiter-character>",
                "Delimiter character, or 'TAB', between array elements within a value in CSV data." ),
        QUOTE( "quote", "<quotation-character>", "Character to treat as quotation character for values in CSV data. "
                + "Quotes inside quotes escaped like `\"\"\"Go away\"\", he said.\"` and "
                + "`\"\\\"Go away\\\", he said.\"` are supported. "
                + "If you have set \"`'`\" to be used as the quotation character, "
                + "you could write the previous example like this instead: " + "`'\"Go away\", he said.'`" ),
        ID_TYPE( "id-type", "<id-type>", "One out of " + Arrays.toString( IdType.values() )
                         + " and specifies how ids in node/relationship "
                         + "input files are treated.\n"
                         + IdType.STRING + ": arbitrary strings for identifying nodes.\n"
                         + IdType.INTEGER + ": arbitrary integer values for identifying nodes.\n"
                         + IdType.ACTUAL + ": (advanced) actual node ids." );

        private final String key;
        private final String usage;
        private final String description;

        Options( String key, String usage, String description )
        {
            this.key = key;
            this.usage = usage;
            this.description = description;
        }

        String key()
        {
            return key;
        }

        void printUsage( PrintStream out )
        {
            out.println( "--" + key + " " + usage );
            for ( String line : Args.splitLongLine( description.replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        String manPageEntry()
        {
            return "*--" + key + " " + usage + "*::\n" + description + "\n\n";
        }
    }

    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = " ";

    public static void main( String[] incomingArguments )
    {
        Args args = Args.parse( incomingArguments );
        if ( asksForUsage( args ) )
        {
            printUsage( System.out );
            return;
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir;
        // The input groups
        Collection<Option<File[]>> nodesFiles, relationshipsFiles;
        try
        {
            storeDir =
                    args.interpretOption( Options.STORE_DIR.key(), Converters.<File> mandatory(), Converters.toFile(),
                            Validators.DIRECTORY_IS_WRITABLE, Validators.CONTAINS_NO_EXISTING_DATABASE );
            nodesFiles =
                    args.interpretOptionsWithMetadata( Options.NODE_DATA.key(), Converters.<File[]> mandatory(),
                            Converters.toFiles( MULTI_FILE_DELIMITER ), Validators.FILES_EXISTS,
                            Validators.<File> atLeast( 1 ) );
            relationshipsFiles =
                    args.interpretOptionsWithMetadata( Options.RELATIONSHIP_DATA.key(),
                            Converters.<File[]> mandatory(), Converters.toFiles( MULTI_FILE_DELIMITER ),
                            Validators.FILES_EXISTS, Validators.<File> atLeast( 1 ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false );
        }

        LifeSupport life = new LifeSupport();
        Logging logging = life.add( new ClassicLoggingService(
                new Config( stringMap( store_dir.name(), storeDir.getAbsolutePath() ) ) ) );
        life.start();
        BatchImporter importer = new ParallelBatchImporter( storeDir.getPath(),
                // TODO Ability to specify batch importer configuration as well?
                DEFAULT,
                logging,
                ExecutionMonitors.defaultVisible() );
        Input input = new CsvInput(
                nodeData( nodesFiles ),
                defaultFormatNodeFileHeader(),
                relationshipData( relationshipsFiles ),
                defaultFormatRelationshipFileHeader(),
                args.interpretOption( Options.ID_TYPE.key(), withDefault( IdType.STRING ), TO_ID_TYPE ),
                csvConfiguration( args ) );
        boolean success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( IOException e )
        {
            throw andPrintError( "Import error", e, true );
        }
        finally
        {
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
                }
            }
        }
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     * @param stackTrace whether or not to also print the stack trace of the error.
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace )
    {
        System.err.println( typeOfError + ": " + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( System.err );
        }
        System.err.println();
        printUsage( System.err );

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
            relationshipData( Collection<Option<File[]>> relationshipsFiles )
    {
        return new IterableWrapper<DataFactory<InputRelationship>,Option<File[]>>( relationshipsFiles )
        {
            @Override
            protected DataFactory<InputRelationship> underlyingObjectToObject( Option<File[]> group )
            {
                return data( defaultRelationshipType( group.metadata() ), group.value() );
            }
        };
    }

    private static Iterable<DataFactory<InputNode>> nodeData( Collection<Option<File[]>> files )
    {
        return new IterableWrapper<DataFactory<InputNode>,Option<File[]>>( files )
        {
            @Override
            protected DataFactory<InputNode> underlyingObjectToObject( Option<File[]> group )
            {
                Function<InputNode,InputNode> decorator = group.metadata() != null
                        ? additiveLabels( group.metadata().split( ":" ) )
                        : Functions.<InputNode>identity();
                return data( decorator, group.value() );
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

    private static Configuration csvConfiguration( Args args )
    {
        final Configuration defaultConfiguration = COMMAS;
        final Character specificDelimiter =
                args.interpretOption( Options.DELIMITER.key(), Converters.<Character>optional(), DELIMITER_CONVERTER );
        final Character specificArrayDelimiter =
                args.interpretOption( Options.ARRAY_DELIMITER.key(), Converters.<Character>optional(), DELIMITER_CONVERTER );
        final Character specificQuote =
                args.interpretOption( Options.QUOTE.key(), Converters.<Character>optional(), Converters.toCharacter() );
        return new Configuration()
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
        };
    }

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
}
