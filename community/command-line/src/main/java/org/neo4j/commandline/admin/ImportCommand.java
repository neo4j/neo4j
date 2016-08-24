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
package org.neo4j.commandline.admin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ImportCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "import" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--mode=<mode> --database=<database-name> --from=<source-directory>" );
        }

        @Override
        public String description()
        {
            return "Import a database from a pre-3.0 Neo4j installation. <source-directory> " +
                    "is the database location (e.g. <neo4j-root>/data/graph.db).";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new ImportCommand( homeDir, configDir );
        }
    }

    private final Path homeDir;
    private final Path configDir;

    public ImportCommand( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        File from;
        String database;

        Args parsedArgs = Args.parse( args );
        try
        {
            parsedArgs.interpretOption( "mode", Converters.<String>mandatory(), s -> s,
                    Validators.inList( new String[]{"database"} ) );
            database = parsedArgs.interpretOption( "database", Converters.<String>mandatory(), s -> s );
            from = parsedArgs.interpretOption( "from", Converters.<File>mandatory(), Converters.toFile(),
                    Validators.CONTAINS_EXISTING_DATABASE );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        try
        {
            Config config = loadNeo4jConfig( homeDir, configDir, database );
            copyDatabase( from, config );
            removeMessagesLog( config );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

    }

    private void copyDatabase( File from, Config config ) throws IOException
    {
        FileUtils.copyRecursively( from, config.get( database_path ) );
    }

    private void removeMessagesLog( Config config )
    {
        FileUtils.deleteFile( new File( config.get( database_path ), "messages.log" ) );
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        Config config = configLoader.loadConfig( Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) );

        return config.with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) );
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }
}
