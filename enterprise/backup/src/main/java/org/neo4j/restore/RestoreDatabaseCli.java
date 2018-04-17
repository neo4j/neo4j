/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;

public class RestoreDatabaseCli implements AdminCommand
{
    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryNamedArg( "from", "backup-directory", "Path to backup to restore from." ) )
            .withDatabase()
            .withArgument( new OptionalBooleanArg( "force", false, "If an existing database should be replaced." ) );
    private final Path homeDir;
    private final Path configDir;

    public RestoreDatabaseCli( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName )
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withSetting( GraphDatabaseSettings.active_database, databaseName )
                .withConnectorsDisabled().build();
    }

    @Override
    public void execute( String[] incomingArguments ) throws IncorrectUsage, CommandFailed
    {
        String databaseName;
        String fromPath;
        boolean forceOverwrite;

        try
        {
            databaseName = arguments.parse( incomingArguments ).get( ARG_DATABASE );
            fromPath = arguments.get( "from" );
            forceOverwrite = arguments.getBoolean( "force" );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        Config config = loadNeo4jConfig( homeDir, configDir, databaseName );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fileSystem,
                    new File( fromPath ), config, databaseName, forceOverwrite );
            restoreDatabaseCommand.execute();
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Failed to restore database", e );
        }
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
