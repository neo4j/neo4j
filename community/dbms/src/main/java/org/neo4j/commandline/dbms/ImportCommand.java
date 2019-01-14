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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArgWithMetadata;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.csv.reader.Configuration.DEFAULT;
import static org.neo4j.tooling.ImportTool.parseFileArgumentList;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT_MAX_MEMORY_PERCENT;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;

public class ImportCommand implements AdminCommand
{
    public static final String DEFAULT_REPORT_FILE_NAME = "import.report";
    private static final String[] allowedModes = {"database", "csv"};
    private static final Arguments databaseArguments = new Arguments()
            .withArgument( new MandatoryNamedArg( "mode", "database", "Import a pre-3.0 installation." )
            {
                @Override
                public String usage()
                {
                    return String.format( "--%s=%s", name(), exampleValue() );
                }
            } )
            .withDatabase()
            .withAdditionalConfig();

    private static final Arguments csvArguments = new Arguments()
            .withArgument( new OptionalNamedArg( "mode", "csv", "csv", "Import a collection of CSV files." )
            {
                @Override
                public String usage()
                {
                    return String.format( "[--%s=%s]", name(), exampleValue() );
                }
            } )
            .withDatabase()
            .withAdditionalConfig();

    private static final Arguments allArguments = new Arguments()
            .withDatabase()
            .withAdditionalConfig()
            .withArgument( new OptionalNamedArg( "mode", allowedModes, "csv",
                    "Import a collection of CSV files or a pre-3.0 installation." ) );

    private static void includeDatabaseArguments( Arguments arguments )
    {
        arguments
            .withArgument( new OptionalNamedArg( "from", "source-directory", "",
                    "The location of the pre-3.0 database (e.g. <neo4j-root>/data/graph.db)." ) );
    }

    private static void includeCsvArguments( Arguments arguments )
    {
        arguments
            .withArgument( new OptionalNamedArg( "report-file", "filename", DEFAULT_REPORT_FILE_NAME,
                    "File in which to store the report of the csv-import." ) )
            .withArgument( new OptionalNamedArgWithMetadata( "nodes",
                    ":Label1:Label2",
                    "\"file1,file2,...\"", "",
                    "Node CSV header and data. Multiple files will be logically seen as " +
                            "one big file from the perspective of the importer. The first line " +
                            "must contain the header. Multiple data sources like these can be " +
                            "specified in one import, where each data source has its own header. " +
                    "Note that file groups must be enclosed in quotation marks." ) )
            .withArgument( new OptionalNamedArgWithMetadata( "relationships",
                    ":RELATIONSHIP_TYPE",
                    "\"file1,file2,...\"",
                    "",
                    "Relationship CSV header and data. Multiple files will be logically " +
                            "seen as one big file from the perspective of the importer. The first " +
                            "line must contain the header. Multiple data sources like these can be " +
                            "specified in one import, where each data source has its own header. " +
                    "Note that file groups must be enclosed in quotation marks." ) )
            .withArgument( new OptionalNamedArg( "id-type", new String[]{"STRING", "INTEGER", "ACTUAL"},
                    "STRING", "Each node must provide a unique id. This is used to find the correct " +
                    "nodes when creating relationships. Possible values are:\n" +
                    "  STRING: arbitrary strings for identifying nodes,\n" +
                    "  INTEGER: arbitrary integer values for identifying nodes,\n" +
                    "  ACTUAL: (advanced) actual node ids.\n" +
                    "For more information on id handling, please see the Neo4j Manual: " +
                    "https://neo4j.com/docs/operations-manual/current/tools/import/" ) )
            .withArgument( new OptionalNamedArg( "input-encoding", "character-set", "UTF-8",
                    "Character set that input data is encoded in." ) )
            .withArgument( new OptionalBooleanArg( "ignore-extra-columns", false,
                    "If un-specified columns should be ignored during the import." ) )
            .withArgument( new OptionalBooleanArg( "ignore-duplicate-nodes", false,
                    "If duplicate nodes should be ignored during the import." ) )
            .withArgument( new OptionalBooleanArg( "ignore-missing-nodes", false,
                    "If relationships referring to missing nodes should be ignored during the import." ) )
            .withArgument( new OptionalBooleanArg( "multiline-fields",
                    DEFAULT.multilineFields(),
                    "Whether or not fields from input source can span multiple lines," +
                            " i.e. contain newline characters." ) )
            .withArgument( new OptionalNamedArg( "delimiter",
                    "delimiter-character",
                    String.valueOf( COMMAS.delimiter() ),
                    "Delimiter character between values in CSV data." ) )
            .withArgument( new OptionalNamedArg( "array-delimiter",
                    "array-delimiter-character",
                    String.valueOf( COMMAS.arrayDelimiter() ),
                    "Delimiter character between array elements within a value in CSV data." ) )
            .withArgument( new OptionalNamedArg( "quote",
                    "quotation-character",
                    String.valueOf( COMMAS.quotationCharacter() ),
                    "Character to treat as quotation character for values in CSV data. "
                            + "Quotes can be escaped as per RFC 4180 by doubling them, for example \"\" would be " +
                            "interpreted as a literal \". You cannot escape using \\." ) )
            .withArgument( new OptionalNamedArg( "max-memory",
                    "max-memory-that-importer-can-use",
                    String.valueOf( DEFAULT_MAX_MEMORY_PERCENT ) + "%",
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
            .withArgument( new OptionalNamedArg( "high-io",
                    "true/false",
                    null,
                    "Ignore environment-based heuristics, and assume that the target storage subsystem can support parallel IO with high throughput." ) );
    }

    static
    {
        includeDatabaseArguments( databaseArguments );
        includeDatabaseArguments( allArguments );

        includeCsvArguments( csvArguments );
        includeCsvArguments( allArguments );
    }

    public static Arguments databaseArguments()
    {
        return databaseArguments;
    }

    public static Arguments csvArguments()
    {
        return csvArguments;
    }

    public static Arguments allArguments()
    {
        return allArguments;
    }

    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;
    private final ImporterFactory importerFactory;

    public ImportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this( homeDir, configDir, outsideWorld, new ImporterFactory() );
    }

    ImportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld, ImporterFactory importerFactory )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
        this.importerFactory = importerFactory;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        String mode;
        Optional<Path> additionalConfigFile;
        String database;

        try
        {
            mode = allArguments.parse( args ).get( "mode" );
            Optional<Path> fileArgument = allArguments.getOptionalPath( "f" );
            if ( fileArgument.isPresent() )
            {
                // Parsing the arguments inside the -f file and reassigning the "args" parameter, because it's the one
                // carrying the arguments to the actual importer.
                args = parseFileArgumentList( fileArgument.get().toFile() );
                allArguments.parse( args );
            }
            database = allArguments.get( ARG_DATABASE );
            additionalConfigFile = allArguments.getOptionalPath( "additional-config" );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        try
        {
            Config config =
                    loadNeo4jConfig( homeDir, configDir, database, loadAdditionalConfig( additionalConfigFile ) );

            // The "args" parameter may have been reassigned from what came into this method.
            // This can happen if there was a -f argument in it, where arguments inside that file gets loaded into it.
            Importer importer = importerFactory.getImporterForMode( mode, Args.parse( args ), config, outsideWorld );
            importer.doImport();
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static Map<String,String> loadAdditionalConfig( Optional<Path> additionalConfigFile )
    {
        if ( additionalConfigFile.isPresent() )
        {
            try
            {
                return MapUtil.load( additionalConfigFile.get().toFile() );
            }
            catch ( IOException e )
            {
                throw new IllegalArgumentException(
                        String.format( "Could not read configuration file [%s]", additionalConfigFile ), e );
            }
        }

        return new HashMap<>();
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName,
            Map<String,String> additionalConfig )
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withSetting( GraphDatabaseSettings.active_database, databaseName )
                .withSettings( additionalConfig )
                .withConnectorsDisabled().build();
    }
}
