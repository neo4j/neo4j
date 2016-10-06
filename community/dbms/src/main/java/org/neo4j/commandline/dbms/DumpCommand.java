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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.StoreLocker;
import org.neo4j.server.configuration.ConfigLoader;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.mandatory;

public class DumpCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "dump" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--database=<database> --to=<destination-path>" );
        }

        @Override
        public String description()
        {
            return "Dump a database into a single-file archive. The archive can be used by the load command. " +
                    "<destination-path> can be a file or directory (in which case a file called <database>.dump will " +
                    "be created). It is not possible to dump a database that is mounted in a running Neo4j server.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new DumpCommand( homeDir, configDir, new Dumper() );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private final Dumper dumper;

    public DumpCommand( Path homeDir, Path configDir, Dumper dumper )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.dumper = dumper;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        String database = parse( args, "database", identity() );
        Path archive = calculateArchive( database, parse( args, "to", Paths::get ) );
        Path databaseDirectory = toDatabaseDirectory( database );

        try ( Closeable ignored = withLock( databaseDirectory ) )
        {
            dump( database, databaseDirectory, archive );
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "the database is in use -- stop Neo4j and try again", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private <T> T parse( String[] args, String argument, Function<String, T> converter ) throws IncorrectUsage
    {
        try
        {
            return Args.parse( args ).interpretOption( argument, mandatory(), converter );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }

    private Path toDatabaseDirectory( String databaseName )
    {
        //noinspection unchecked
        return new ConfigLoader( asList( DatabaseManagementSystemSettings.class, GraphDatabaseSettings.class ) )
                .loadOfflineConfig(
                        Optional.of( homeDir.toFile() ),
                        Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) )
                .with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) )
                .get( database_path ).toPath();
    }

    private Path calculateArchive( String database, Path to )
    {
        return Files.isDirectory( to ) ? to.resolve( database + ".dump" ) : to;
    }

    private Closeable withLock( Path databaseDirectory ) throws CommandFailed
    {
        Path lockFile = databaseDirectory.resolve( StoreLocker.STORE_LOCK_FILENAME );
        if ( Files.exists( lockFile ) )
        {
            if ( Files.isWritable( lockFile ) )
            {
                StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() );
                storeLocker.checkLock( databaseDirectory.toFile() );
                return storeLocker::release;
            }
            else
            {
                throw new CommandFailed( "you do not have permission to dump the database -- is Neo4j running as a " +
                        "different user?" );
            }
        }
        return () ->
        {
        };
    }

    private void dump( String database, Path databaseDirectory, Path archive ) throws CommandFailed
    {
        try
        {
            dumper.dump( databaseDirectory, archive, this::isStoreLock );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailed( "archive already exists: " + e.getMessage(), e );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( databaseDirectory.toAbsolutePath() ) )
            {
                throw new CommandFailed( "database does not exist: " + database, e );
            }
            wrapIOException( e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private boolean isStoreLock( Path path )
    {
        return Objects.equals( path.getFileName().toString(), StoreLocker.STORE_LOCK_FILENAME );
    }

    private void wrapIOException( IOException e ) throws CommandFailed
    {
        throw new CommandFailed(
                format( "unable to dump database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }
}
