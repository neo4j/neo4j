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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.CompressionFormat;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(
        name = "dump",
        header = "Dump a database into a single-file archive.",
        description = "Dump a database into a single-file archive. The archive can be used by the load command. " +
                "<destination-path> can be a file or directory (in which case a file called <database>.dump will " +
                "be created). It is not possible to dump a database that is mounted in a running Neo4j server."
)
public class DumpCommand extends AbstractCommand
{
    @Option( names = "--database", description = "Name of database.", defaultValue = DEFAULT_DATABASE_NAME )
    private String database;
    @Option( names = "--to", paramLabel = "<path>", required = true, description = "Destination (file or folder) of database dump." )
    private Path to;

    private final Dumper dumper;

    public DumpCommand( ExecutionContext ctx, Dumper dumper )
    {
        super( ctx );
        this.dumper = requireNonNull( dumper );
    }

    @Override
    public void execute()
    {
        Path archive = calculateArchive( database, to.toAbsolutePath() );

        Config config = buildConfig();
        Path storeDirectory = getDatabaseDirectory( config );
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory.toFile(), of( config ), database );

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( "Database does not exist: " + database, e );
        }

        try ( Closeable ignored = DatabaseLockChecker.check( databaseLayout ) )
        {
            checkDbState( databaseLayout, config );
            dump( database, databaseLayout, archive );
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + database + "' and try again.", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
        catch ( CannotWriteException e )
        {
            throw new CommandFailedException( "You do not have permission to dump the database.", e );
        }
    }

    private static Path getDatabaseDirectory( Config config )
    {
        return config.get( databases_root_path );
    }

    private Config buildConfig()
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir() ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;

    }

    private static Path calculateArchive( String database, Path to )
    {
        return Files.isDirectory( to ) ? to.resolve( database + ".dump" ) : to;
    }

    private void dump( String database, DatabaseLayout databaseLayout, Path archive )
    {
        Path databasePath = databaseLayout.databaseDirectory().toPath();
        try
        {
            File lockFile = databaseLayout.databaseLockFile();
            dumper.dump( databasePath, databaseLayout.getTransactionLogsDirectory().toPath(), archive,
                    CompressionFormat.ZSTD, path -> Objects.equals( path.getFileName().toString(), lockFile.getName() ) );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailedException( "Archive already exists: " + e.getMessage(), e );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( databasePath ) )
            {
                throw new CommandFailedException( "Database does not exist: " + database, e );
            }
            wrapIOException( e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration ) )
        {
            throw new CommandFailedException( joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the dump.",
                    "To perform recovery please start database and perform clean shutdown." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Failure when checking for recovery state: '%s'." + e.getMessage(), e );
        }
    }

    private static void wrapIOException( IOException e )
    {
        throw new CommandFailedException(
                format( "Unable to dump database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }
}
