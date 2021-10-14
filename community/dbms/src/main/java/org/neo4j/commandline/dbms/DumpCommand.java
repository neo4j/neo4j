/*
 * Copyright (c) "Neo4j"
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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(
        name = "dump",
        header = "Dump a database into a single-file archive.",
        description = "Dump a database into a single-file archive. The archive can be used by the load command. " +
                "<destination-path> can be a file or directory (in which case a file called <database>.dump will " +
                "be created), or '-' to use standard output. It is not possible to dump a database that is mounted in a running Neo4j server."
)
public class DumpCommand extends AbstractCommand
{
    public static final String STANDARD_OUTPUT = "-";
    @Option( names = "--database", description = "Name of the database to dump.", defaultValue = DEFAULT_DATABASE_NAME,
            converter = DatabaseNameConverter.class )
    protected NormalizedDatabaseName database;
    @Option( names = "--to", paramLabel = "<path>", required = true, description = "Destination (file or folder or '-' for stdout) of database dump." )
    private String to;
    private final Dumper dumper;

    public DumpCommand( ExecutionContext ctx, Dumper dumper )
    {
        super( ctx );
        this.dumper = requireNonNull( dumper );
    }

    @Override
    public void execute()
    {
        var databaseName = database.name();
        var memoryTracker =  EmptyMemoryTracker.INSTANCE;

        Config config = CommandHelpers.buildConfig( ctx, allowCommandExpansion );
        DatabaseLayout databaseLayout = Neo4jLayout.of( config ).databaseLayout( databaseName );

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( "Database does not exist: " + databaseName, e );
        }

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            if ( fileSystem.fileExists( databaseLayout.file( StoreUpgrader.MIGRATION_DIRECTORY ) ) )
            {
                throw new CommandFailedException( "Store migration folder detected - A dump can not be taken during a store migration. Make sure " +
                                                  "store migration is completed before trying again." );
            }
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }

        try ( Closeable ignored = LockChecker.checkDatabaseLock( databaseLayout ) )
        {
            checkDbState( databaseLayout, config, memoryTracker );
            dump( databaseLayout, databaseName );
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + databaseName + "' and try again.", e );
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

    private static Path buildArchivePath( String database, Path to )
    {
        return Files.isDirectory( to ) ? to.resolve( database + ".dump" ) : to;
    }

    private OutputStream openDumpStream( String databaseName, String destination ) throws IOException
    {
        if ( destination.equals( STANDARD_OUTPUT ) )
        {
            return ctx.out();
        }
        var archive = buildArchivePath( databaseName, Path.of( destination ).toAbsolutePath() );
        return dumper.openForDump( archive );
    }

    private void dump( DatabaseLayout databaseLayout, String databaseName )
    {
        Path databasePath = databaseLayout.databaseDirectory();
        try
        {
            var format = DumpFormatSelector.selectFormat( ctx.err() );
            var lockFile = databaseLayout.databaseLockFile().getFileName().toString();
            var quarantineMarkerFile = databaseLayout.quarantineMarkerFile().getFileName().toString();
            var out = openDumpStream( databaseName, to );
            dumper.dump( databasePath, databaseLayout.getTransactionLogsDirectory(), out, format, path -> oneOf( path, lockFile, quarantineMarkerFile ) );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailedException( "Archive already exists: " + e.getMessage(), e );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( databasePath ) )
            {
                throw new CommandFailedException( "Database does not exist: " + databaseLayout.getDatabaseName(), e );
            }
            wrapIOException( e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private static boolean oneOf( Path path, String... names )
    {
        return ArrayUtil.contains( names, path.getFileName().toString() );
    }

    protected void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration, memoryTracker ) )
        {
            throw new CommandFailedException( joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the dump.",
                    "To perform recovery please start database and perform clean shutdown." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration, memoryTracker );
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
