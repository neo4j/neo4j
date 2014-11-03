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

import org.neo4j.function.Function;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.neo4j.tooling.batchimport.Converters.toIdType;
import static org.neo4j.tooling.batchimport.Converters.withDefault;
import static org.neo4j.tooling.batchimport.Validators.CONTAINS_NO_EXISTING_DATABASE;
import static org.neo4j.tooling.batchimport.Validators.DIRECTORY_IS_WRITABLE;
import static org.neo4j.tooling.batchimport.Validators.FILE_EXISTS;
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
    private static final String NODE_DATA = "nodes";
    private static final String RELATIONSHIP_DATA = "relationships";
    private static final String NODE_HEADER = "nodes-header";
    private static final String RELATIONSHIP_HEADER = "relationships-header";
    private static final String STORE_DIR = "into";
    private static final String DELIMITER = "delimiter";
    private static final String ARRAY_DELIMITER = "array-delimiter";
    private static final String QUOTE = "quote";
    private static final String ID_TYPE = "id-type";

    public static void main( String[] incomingArguments )
    {
        Args args = new Args( incomingArguments );

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir, nodesFile, relationshipsFile, nodeHeaderFile, relationshipHeaderFile;
        storeDir = parseArgument( args, STORE_DIR, Converters.<File>mandatory(), Converters.toFile(),
                DIRECTORY_IS_WRITABLE, CONTAINS_NO_EXISTING_DATABASE );
        nodesFile = parseArgument( args, NODE_DATA, Converters.<File>mandatory(), Converters.toFile(),
                FILE_EXISTS );
        relationshipsFile = parseArgument( args, RELATIONSHIP_DATA, Converters.<File>mandatory(),
                Converters.toFile(), FILE_EXISTS );
        nodeHeaderFile = parseArgument( args, NODE_HEADER, Converters.<File>optional(),
                Converters.toFile(), FILE_EXISTS );
        relationshipHeaderFile = parseArgument( args, RELATIONSHIP_HEADER, Converters.<File>optional(),
                Converters.toFile(), FILE_EXISTS );
        // If we specify headers as the same as the data files it's the same as not specifying header
        // files at all since we will read headers off of the top of the data files.
        if ( nodeHeaderFile != null && nodeHeaderFile.equals( nodesFile ) )
        {
            nodeHeaderFile = null;
        }
        if ( relationshipHeaderFile != null && relationshipHeaderFile.equals( relationshipsFile ) )
        {
            relationshipHeaderFile = null;
        }

        BatchImporter importer = new ParallelBatchImporter( storeDir.getPath(),
                // TODO Ability to specify batch importer configuration as well?
                DEFAULT,
                // TODO Log to System.out, or to messages.log?
                new SystemOutLogging(),
                ExecutionMonitors.defaultVisible() );
        Input input = new CsvInput(
                // TODO Ability to specify multiple files?
                DataFactories.file( nodesFile ),
                nodeHeaderFile != null
                        ? defaultFormatNodeFileHeader( nodeHeaderFile )
                        : defaultFormatNodeFileHeader(),
                // TODO Ability to specify multiple files?
                DataFactories.file( relationshipsFile ),
                relationshipHeaderFile != null
                        ? defaultFormatRelationshipFileHeader( relationshipHeaderFile )
                        : defaultFormatRelationshipFileHeader(),
                parseArgument( args, ID_TYPE, withDefault( IdType.STRING ), toIdType() ),
                csvConfiguration( args, nodesFile ) );
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

    private static Configuration csvConfiguration( Args args, File nodesFile )
    {
        String name = nodesFile.getName().toLowerCase();
        final Configuration defaultConfiguration = name.endsWith( ".tsv" ) ? TABS : COMMAS;
        final Character specificDelimiter = parseCharArgument( args, DELIMITER );
        final Character specificArrayDelimiter = parseCharArgument( args, ARRAY_DELIMITER );
        final Character specificQuote = parseCharArgument( args, QUOTE );
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

    @SafeVarargs
    private static <T> T parseArgument( Args args, String key, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        T value;
        if ( !args.has( key ) )
        {
            value = defaultValue.apply( key );
        }
        else
        {
            String stringValue = args.get( key );
            value = converter.apply( stringValue );
        }

        if ( value != null )
        {
            for ( Validator<T> validator : validators )
            {
                validator.validate( value );
            }
        }
        return value;
    }

    private static Character parseCharArgument( Args args, String key )
    {
        return parseArgument( args, key, Converters.<Character>optional(), Converters.toCharacter() );
    }
}
