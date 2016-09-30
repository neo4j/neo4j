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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.StoreLocker;

import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static org.neo4j.coreedge.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;

public class UnbindFromClusterCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "unbind" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "[--database=<name>] " );
        }

        @Override
        public String description()
        {
            return "Removes cluster state data from the specified database making it suitable for use non-Core-Edge " +
                    "databases";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new UnbindFromClusterCommand( homeDir, new DefaultFileSystemAbstraction() );
        }
    }

    private Path homeDir;
    private FileSystemAbstraction fsa;

    UnbindFromClusterCommand( Path homeDir, FileSystemAbstraction fsa )
    {
        this.homeDir = homeDir;
        this.fsa = fsa;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Args parsedArgs = Args.parse( args );

        try
        {
            Path pathToSpecificDatabase =
                    homeDir.resolve( "data" ).resolve( "databases" ).resolve( databaseNameFrom( parsedArgs ) );
            confirmTargetDatabaseDirectoryExists( pathToSpecificDatabase );
            confirmClusterStateDirectoryExists( Paths.get( pathToSpecificDatabase.toString(), "cluster-state" ) );
            confirmTargetDirectoryIsWritable( pathToSpecificDatabase );
            deleteClusterStateIn( clusterStateFrom( pathToSpecificDatabase ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( UnbindFailureException e )
        {
            throw new CommandFailed( "Unbind failed: " + e.getMessage(), e );
        }
    }

    private Path clusterStateFrom( Path target )
    {
        return Paths.get( target.toString(), CLUSTER_STATE_DIRECTORY_NAME );
    }

    private void confirmTargetDirectoryIsWritable( Path dbDir ) throws CommandFailed
    {
        Path lockFile = dbDir.resolve( StoreLocker.STORE_LOCK_FILENAME );
        if ( exists( lockFile ) )
        {
            if ( Files.isWritable( lockFile ) )
            {
                StoreLocker storeLocker = new StoreLocker( fsa );
                try
                {
                    storeLocker.checkLock( dbDir.toFile() );
                }
                catch ( StoreLockException e )
                {
                    throw new CommandFailed( "Database is currently locked. Is a Neo4j instance still using it?" );
                }
            }
        }
    }

    private void confirmTargetDatabaseDirectoryExists( Path target )
    {
        if ( !exists( target ) )
        {
            throw new IllegalArgumentException( format( "Database %s does not exist", target ) );
        }
    }

    private void confirmClusterStateDirectoryExists( Path clusterStateDirectory ) throws UnbindFailureException
    {
        if ( !exists( clusterStateDirectory ) )
        {
            throw new UnbindFailureException( "Database %s is not bound to any cluster", clusterStateDirectory );
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

    private String databaseNameFrom( Args parsedArgs )
    {
        String databaseName = parsedArgs.get( "database" );
        if ( databaseName == null )
        {
            throw new IllegalArgumentException(
                    "No database name specified. Usage: neo4j-admin " + "unbind --database-<name>" );
        }
        else
        {
            return databaseName;
        }
    }

    private class UnbindFailureException extends Exception
    {
        UnbindFailureException( Exception e )
        {
            super( e );
        }

        UnbindFailureException( String message, Object... args )
        {
            super( format( message, args ) );
        }
    }
}
