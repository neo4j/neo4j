/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusterStateException;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Validators;

import static org.neo4j.kernel.configuration.Config.fromFile;

public class UnbindFromClusterCommand implements AdminCommand
{
    private static final Arguments arguments = new Arguments().withDatabase();
    private Path homeDir;
    private Path configDir;
    private OutsideWorld outsideWorld;

    UnbindFromClusterCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    static Arguments arguments()
    {
        return arguments;
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName )
    {
        return fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withSetting( GraphDatabaseSettings.active_database, databaseName )
                .withHome( homeDir ).build();
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            Config config = loadNeo4jConfig( homeDir, configDir, arguments.parse( args ).get( "database" ) );
            File dataDirectory = config.get( GraphDatabaseSettings.data_directory );
            Path pathToSpecificDatabase = config.get( GraphDatabaseSettings.database_path ).toPath();

            boolean hasDatabase = true;
            try
            {
                Validators.CONTAINS_EXISTING_DATABASE.validate( pathToSpecificDatabase.toFile() );
            }
            catch ( IllegalArgumentException ignored )
            {
                // No such database, it must have been deleted. Must be OK to delete cluster state
                hasDatabase = false;
            }

            if ( hasDatabase )
            {
                confirmTargetDirectoryIsWritable( pathToSpecificDatabase );
            }

            ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDirectory );
            clusterStateDirectory.initialize( outsideWorld.fileSystem() );

            deleteClusterStateIn( clusterStateDirectory.get().toPath() );
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "Database is currently locked. Please shutdown Neo4j.", e );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( UnbindFailureException | CannotWriteException | IOException | ClusterStateException e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    private void confirmTargetDirectoryIsWritable( Path pathToSpecificDatabase )
            throws CommandFailed, CannotWriteException, IOException
    {
        try ( Closeable ignored = StoreLockChecker.check( pathToSpecificDatabase ) )
        {
            // empty
        }
    }

    private void deleteClusterStateIn( Path target ) throws UnbindFailureException
    {
        try
        {
            FileUtils.deleteRecursively( target.toFile() );
        }
        catch ( IOException e )
        {
            throw new UnbindFailureException( e );
        }
    }

    private class UnbindFailureException extends Exception
    {
        UnbindFailureException( Exception e )
        {
            super( e );
        }
    }
}
