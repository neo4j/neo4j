/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.server.configuration.ConfigLoader;

public class ImportCommand implements AdminCommand
{
    public static final String DEFAULT_REPORT_FILE_NAME = "import.report";

    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "import" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional
                    .of( "--database=<database-name> [--mode={csv|database}] [--additional-config=<config-file-path>]" +
                            " " + DatabaseImporter.arguments() + " " + CsvImporter.arguments() );
        }

        @Override
        public String description()
        {
            return "Import a collection of CSV files with --mode=csv (default), or a database from a " +
                    "pre-3.0 installation with --mode=database." + "\n" + DatabaseImporter.description() + "\n\n" +
                    CsvImporter.description();
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new ImportCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;
    private final ImporterFactory importerFactory;
    private final String[] allowedModes = {"database", "csv"};

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
        Args parsedArgs = Args.parse( args );
        String mode;
        File additionalConfigFile;
        String database;

        try
        {
            mode = parsedArgs.interpretOption( "mode", Converters.withDefault( "csv" ), s -> s,
                    Validators.inList( allowedModes ) );
            database = parsedArgs.interpretOption( "database", Converters.mandatory(), s -> s );
            additionalConfigFile =
                    parsedArgs.interpretOption( "additional-config", Converters.optional(), Converters.toFile() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        try
        {
            Config config =
                    loadNeo4jConfig( homeDir, configDir, database, loadAdditionalConfig( additionalConfigFile ) );
            Validators.CONTAINS_NO_EXISTING_DATABASE
                    .validate( config.get( DatabaseManagementSystemSettings.database_path ) );

            Importer importer = importerFactory.getImporterForMode( mode, parsedArgs, config, outsideWorld );
            importer.doImport();
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Map<String,String> loadAdditionalConfig( File additionalConfigFile )
    {
        if ( additionalConfigFile == null )
        {
            return new HashMap<>();
        }

        try
        {
            return MapUtil.load( additionalConfigFile );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException(
                    String.format( "Could not read configuration file [%s]", additionalConfigFile ), e );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName,
            Map<String,String> additionalConfig )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        Config config = configLoader.loadOfflineConfig( Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) );
        additionalConfig.put( DatabaseManagementSystemSettings.active_database.name(), databaseName );
        return config.with( additionalConfig );
    }

    private static List<Class<?>> settings()
    {
        return Arrays.asList( GraphDatabaseSettings.class, DatabaseManagementSystemSettings.class );
    }
}
