/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin.security;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.logging.NullLog;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;

public class ListTransactionsCommand implements AdminCommand
{
    public class Provider extends AdminCommand.Provider
    {

        protected Provider()
        {
            super( "list-active-transactions" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--username=<username> --password=<password>" );
        }

        @Override
        public String description()
        {
            return "Lists running transactions, optionally for a specified user";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir )
        {
            return new ListTransactionsCommand( homeDir, configDir );
        }
    }

    private final Path homeDir;
    private final Path configDir;

    public ListTransactionsCommand( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        String username;
        String password;
        Args parsedArgs = Args.parse( args );
        try
        {
            username = parsedArgs.interpretOption( "username", Converters.<String>mandatory(), s -> s );
            password = parsedArgs.interpretOption( "password", Converters.<String>mandatory(), s -> s );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        try
        {
            Config config = loadNeo4jConfig( homeDir, configDir );
            File databasePath = config.get( database_path );
            final File dbms = new File( new File( databasePath, "data" ), "dbms" );
            // TODO: Connect to running server with Java BOLT client and list transactions
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failed to list transactions: " + e.getMessage(), e );
        }
        catch ( Throwable t )
        {
            throw new CommandFailed( "Failed to list transactions: " + t.getMessage(),
                    new RuntimeException( t.getMessage() ) );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        return configLoader.loadConfig(
                Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) );
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }
}
