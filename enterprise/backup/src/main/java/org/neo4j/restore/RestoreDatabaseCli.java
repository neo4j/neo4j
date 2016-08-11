
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
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RestoreDatabaseCli implements AdminCommand
{
    private final Path homeDir;
    private final Path configDir;

    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "restore" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--from=<backup-directory> --database=<database-name> [--force]" );
        }

        @Override
        public String description()
        {
            return "Restore a backed up database.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new RestoreDatabaseCli( homeDir, configDir );
        }
    }

    public RestoreDatabaseCli( Path homeDir, Path configDir )
    {

        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        Config config = configLoader.loadConfig(
                Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ));

        return config.with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) );
    }

    @Override
    public void execute( String[] incomingArguments ) throws IncorrectUsage, CommandFailed
    {
        String databaseName;
        String fromPath;
        boolean forceOverwrite;

        Args args = Args.parse( incomingArguments );
        try
        {
            databaseName = args.interpretOption( "database", Converters.mandatory(), s -> s );
            fromPath = args.interpretOption( "from", Converters.mandatory(), s -> s );
            forceOverwrite = args.getBoolean( "force", Boolean.FALSE, true );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        Config config = loadNeo4jConfig( homeDir, configDir, databaseName );

        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand(
                new DefaultFileSystemAbstraction(),
                new File( fromPath ),
                config,
                databaseName,
                forceOverwrite );

        try
        {
            restoreDatabaseCommand.execute();
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Failed to restore database", e );
        }
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Restore Tool" );
        for ( String line : Args.splitLongLine( "The restore tool is used to restore a backed up database", 80 ) )
        {
            out.println( "\t" + line );
        }

        out.println( "Usage:" );
        out.println("--home-dir <path-to-neo4j>");
        out.println("--from <path-to-backup-directory>");
        out.println("--database <database-name>");
        out.println("--config <path-to-config-directory>");
        out.println("--force");
    }
}
