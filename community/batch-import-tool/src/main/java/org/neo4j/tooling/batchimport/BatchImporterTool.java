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
package org.neo4j.tooling.batchimport;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.neo4j.function.Function;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.io.File.pathSeparator;

import static org.neo4j.kernel.impl.util.Converters.withDefault;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.TABS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

/**
 * User-facing command line tool around a {@link BatchImporter}.
 */
public class BatchImporterTool
{
    private static final Function<String,IdType> TO_ID_TYPE = new Function<String,IdType>()
    {
        @Override
        public IdType apply( String from )
        {
            return IdType.valueOf( from.toUpperCase() );
        }
    };

    private static final String NODE_DATA = "nodes";
    private static final String RELATIONSHIP_DATA = "relationships";
    private static final String STORE_DIR = "into";
    private static final String DELIMITER = "delimiter";
    private static final String ARRAY_DELIMITER = "array-delimiter";
    private static final String QUOTE = "quote";
    private static final String ID_TYPE = "id-type";

    public static void main( String[] incomingArguments )
    {
        Args args = new Args( incomingArguments );
        if ( asksForUsage( args ) )
        {
            printUsage();
            return;
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir;
        File[] nodesFiles, relationshipsFiles;
        try
        {
            storeDir = args.interpretOption( STORE_DIR, Converters.<File>mandatory(), Converters.toFile(),
                    Validators.DIRECTORY_IS_WRITABLE, Validators.CONTAINS_NO_EXISTING_DATABASE );
            nodesFiles = args.interpretOption( NODE_DATA, Converters.<File[]>mandatory(), Converters.toFiles(),
                    Validators.FILES_EXISTS, Validators.<File>atLeast( 1 ) );
            relationshipsFiles = args.interpretOption( RELATIONSHIP_DATA, Converters.<File[]>mandatory(),
                    Converters.toFiles(), Validators.FILES_EXISTS, Validators.<File>atLeast( 1 ) );
        }
        catch ( IllegalArgumentException e )
        {
            printUsage();
            throw new RuntimeException( e ); // throw in order to have process exit with !0
        }

        BatchImporter importer = new ParallelBatchImporter( storeDir.getPath(),
                // TODO Ability to specify batch importer configuration as well?
                DEFAULT,
                // TODO Log to System.out, or to messages.log?
                new SystemOutLogging(),
                ExecutionMonitors.defaultVisible() );
        Input input = new CsvInput(
                // TODO Ability to specify multiple files?
                DataFactories.data( nodesFiles ),
                defaultFormatNodeFileHeader(),
                // TODO Ability to specify multiple files?
                DataFactories.data( relationshipsFiles ),
                defaultFormatRelationshipFileHeader(),
                args.interpretOption( ID_TYPE, withDefault( IdType.STRING ), TO_ID_TYPE ),
                csvConfiguration( args, nodesFiles[0] ) );
        boolean success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
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

    private static void printUsage()
    {
        System.out.println( "Usage:" );
        printArgumentUsage( "--into <store-dir>", "database directory to import into. " +
                "Must not contain existing database." );
        printArgumentUsage( "--nodes <file1>" + pathSeparator + "<file2>" + pathSeparator + "...",
                "Node CSV header and data. Multiple files will be logically seen as one big file " +
                "from the perspective of the importer. First line must contain the header." );
        printArgumentUsage( "--relationships <file1>" + pathSeparator + "<file2>" + pathSeparator + "...",
                "Relationship CSV header and data. Multiple files will be logically seen as one big file " +
                "from the perspective of the importer. First line must contain the header." );
        printArgumentUsage( "--delimiter <delimiter-character>",
                "Delimiter character between values in CSV data." );
        printArgumentUsage( "--array-delimiter <array-delimiter-character>",
                "Delimiter character between array elements within a value in CSV data." );
        printArgumentUsage( "--quote <quotation-character>",
                "Character to treat as quotation character in values in CSV data. " +
                "Quotes inside quotes like '\"\"' and '\\\"' are supported." );
        printArgumentUsage( "--id-type <id-type>",
                "One out of " + Arrays.toString( IdType.values() ) + " and specifies how ids in node/relationship " +
                "input files are treated.\n" +
                IdType.STRING + ": arbitrary strings for identifying nodes.\n" +
                IdType.ACTUAL + ": (advanced) actual node ids, starting from 0" );
    }

    private static void printArgumentUsage( String usage, String description )
    {
        System.out.println( usage );
        for ( String line : Args.splitLongLine( description, 80 ) )
        {
            System.out.println( "\t" + line );
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

    private static Configuration csvConfiguration( Args args, File nodesFile )
    {
        String name = nodesFile.getName().toLowerCase();
        final Configuration defaultConfiguration = name.endsWith( ".tsv" ) ? TABS : COMMAS;
        final Character specificDelimiter =
                args.interpretOption( DELIMITER, Converters.<Character>optional(), Converters.toCharacter() );
        final Character specificArrayDelimiter =
                args.interpretOption( ARRAY_DELIMITER, Converters.<Character>optional(), Converters.toCharacter() );
        final Character specificQuote =
                args.interpretOption( QUOTE, Converters.<Character>optional(), Converters.toCharacter() );
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
}
