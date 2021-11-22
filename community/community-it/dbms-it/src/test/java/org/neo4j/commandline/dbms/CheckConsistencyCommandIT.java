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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.MutuallyExclusiveArgsException;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.CheckConsistencyCommand;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Neo4jLayoutExtension
class CheckConsistencyCommandIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Neo4jLayout neo4jLayout;
    private Path homeDir;
    private Path confPath;

    @BeforeEach
    void setUp()
    {
        homeDir = testDirectory.homePath();
        confPath = testDirectory.directory( "conf" );
        prepareDatabase( neo4jLayout.databaseLayout( "mydb" ) );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new CheckConsistencyCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Check the consistency of a database.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "check-consistency [--expand-commands] [--verbose] [--additional-config=<path>]%n" +
                "                  [--check-graph=<true/false>]%n" +
                "                  [--check-index-structure=<true/false>]%n" +
                "                  [--check-indexes=<true/false>] [--report-dir=<path>]%n" +
                "                  (--database=<database> | --backup=<path>)%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "This command allows for checking the consistency of a database or a backup%n" +
                "thereof. It cannot be used with a database which is currently in use.%n" +
                "%n" +
                "All checks except 'check-graph' can be quite expensive so it may be useful to%n" +
                "turn them off for very large databases. Increasing the heap size can also be a%n" +
                "good idea. See 'neo4j-admin help' for details.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose             Enable verbose output.%n" +
                "      --expand-commands     Allow command expansion in config value evaluation.%n" +
                "      --database=<database> Name of the database to check.%n" +
                "      --backup=<path>       Path to backup to check consistency of. Cannot be%n" +
                "                              used together with --database.%n" +
                "      --additional-config=<path>%n" +
                "                            Configuration file to supply additional%n" +
                "                              configuration in.%n" +
                "      --report-dir=<path>   Directory where consistency report will be written.%n" +
                "                              Default: .%n" +
                "      --check-graph=<true/false>%n" +
                "                            Perform consistency checks between nodes,%n" +
                "                              relationships, properties, types and tokens.%n" +
                "                              Default: true%n" +
                "      --check-indexes=<true/false>%n" +
                "                            Perform consistency checks on indexes.%n" +
                "                              Default: true%n" +
                "      --check-index-structure=<true/false>%n" +
                "                            Perform structure checks on indexes.%n" +
                "                              Default: true"
        ) );
    }

    @Test
    void runsConsistencyChecker()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.of( neo4jLayout, "mydb" );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( DatabaseLayout.class, databaseLayout );
    }

    @Test
    void consistencyCheckerRespectDatabaseLock() throws CannotWriteException, IOException
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );
        RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.of( neo4jLayout, "mydb" );

        testDirectory.getFileSystem().mkdirs( databaseLayout.databaseDirectory() );

        try ( Closeable ignored = LockChecker.checkDatabaseLock( databaseLayout ) )
        {
            CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
            CommandFailedException exception = assertThrows( CommandFailedException.class, checkConsistencyCommand::execute );
            assertThat( exception.getCause() ).isInstanceOf( FileLockException.class );
            assertThat( exception.getMessage() ).isEqualTo( "The database is in use. Stop database 'mydb' and try again." );
        }
    }

    @Test
    void enablesVerbosity()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.of( neo4jLayout, "mydb" );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( DatabaseLayout.class, databaseLayout );
        consistencyCheckService.verifyArgument( Boolean.class, true );
    }

    @Test
    void failsWhenInconsistenciesAreFound()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService(
                ConsistencyCheckService.Result.failure( Path.of( "/the/report/path" ), new ConsistencySummaryStatistics() ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandFailedException commandFailed =
                assertThrows( CommandFailedException.class, () ->
                {
                    CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
                    checkConsistencyCommand.execute();
                } );
        assertThat( commandFailed.getMessage() ).contains( Path.of( "/the/report/path" ).toString() );
    }

    @Test
    void shouldWriteReportFileToCurrentDirectoryByDefault()
            throws CommandFailedException

    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( Path.class, Path.of( "" ) );
    }

    @Test
    void shouldWriteReportFileToSpecifiedDirectory()
            throws CommandFailedException

    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=some-dir-or-other" );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( Path.class, Path.of( "some-dir-or-other" ) );
    }

    @Test
    void shouldCanonicalizeReportDirectory()
            throws CommandFailedException
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=" + Paths.get( "..", "bar" ) );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( Path.class, Path.of( "../bar" ) );
    }

    @Test
    void passesOnCheckParameters()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--check-graph=false",
                "--check-indexes=false", "--check-index-structure=true" );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( ConsistencyFlags.class, new ConsistencyFlags( false, false, true ) );
    }

    @Test
    void databaseAndBackupAreMutuallyExclusive()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        MutuallyExclusiveArgsException incorrectUsage =
                assertThrows( MutuallyExclusiveArgsException.class, () ->
                {
                    CommandLine.populateCommand( checkConsistencyCommand, "--database=foo", "--backup=bar" );
                    checkConsistencyCommand.execute();
                } );
        assertThat( incorrectUsage.getMessage() ).contains( "--database=<database>, --backup=<path> are mutually exclusive (specify only one)" );
    }

    @Test
    void backupNeedsToBePath()
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        Path backupPath = homeDir.resolve( "dir/does/not/exist" );

        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () ->
        {
            CommandLine.populateCommand( checkConsistencyCommand, "--backup=" + backupPath );
            checkConsistencyCommand.execute();
        } );
        assertThat( commandFailed.getMessage() ).contains( "Report directory path doesn't exist or not a directory" );
    }

    @Test
    void canRunOnBackup() throws Exception
    {
        TrackingConsistencyCheckService consistencyCheckService = new TrackingConsistencyCheckService( ConsistencyCheckService.Result.success( null, null ) );

        RecordDatabaseLayout backupLayout = RecordDatabaseLayout.ofFlat( testDirectory.directory( "backup" ) );
        prepareBackupDatabase( backupLayout );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        CommandLine.populateCommand( checkConsistencyCommand, "--backup=" + backupLayout.databaseDirectory() );
        checkConsistencyCommand.execute();

        consistencyCheckService.verifyArgument( DatabaseLayout.class, backupLayout );
    }

    private void prepareBackupDatabase( DatabaseLayout backupLayout ) throws IOException
    {
        testDirectory.getFileSystem().deleteRecursively( homeDir );
        prepareDatabase( backupLayout );
    }

    private static void prepareDatabase( DatabaseLayout databaseLayout )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
        managementService.shutdown();
    }

    private static class TrackingConsistencyCheckService extends ConsistencyCheckService
    {
        private final Map<Class<?>,Object> arguments;
        private final Result result;

        TrackingConsistencyCheckService( Result result )
        {
            super( null );
            this.result = result;
            this.arguments = new HashMap<>();
        }

        TrackingConsistencyCheckService( TrackingConsistencyCheckService from )
        {
            super( null );
            this.result = from.result;
            this.arguments = from.arguments;
        }

        @Override
        public ConsistencyCheckService with( Config config )
        {
            arguments.put( Config.class, config );
            super.with( config );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( Date timestamp )
        {
            arguments.put( Date.class, timestamp );
            super.with( timestamp );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( DatabaseLayout layout )
        {
            arguments.put( DatabaseLayout.class, layout );
            super.with( layout );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( OutputStream progressOutput )
        {
            arguments.put( OutputStream.class, progressOutput );
            super.with( progressOutput );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( LogProvider logProvider )
        {
            arguments.put( LogProvider.class, logProvider );
            super.with( logProvider );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( FileSystemAbstraction fileSystem )
        {
            arguments.put( FileSystemAbstraction.class, fileSystem );
            super.with( fileSystem );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( PageCache pageCache )
        {
            arguments.put( PageCache.class, pageCache );
            super.with( pageCache );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService verbose( boolean verbose )
        {
            arguments.put( Boolean.class, verbose );
            super.verbose( verbose );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( Path reportDir )
        {
            arguments.put( Path.class, reportDir );
            super.with( reportDir );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( ConsistencyFlags consistencyFlags )
        {
            arguments.put( ConsistencyFlags.class, consistencyFlags );
            super.with( consistencyFlags );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( PageCacheTracer pageCacheTracer )
        {
            arguments.put( PageCacheTracer.class, pageCacheTracer );
            super.with( pageCacheTracer );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public ConsistencyCheckService with( MemoryTracker memoryTracker )
        {
            arguments.put( MemoryTracker.class, memoryTracker );
            super.with( memoryTracker );
            return new TrackingConsistencyCheckService( this );
        }

        @Override
        public Result runFullConsistencyCheck()
        {
            return result;
        }

        void verifyArgument( Class<?> type, Object expectedValue )
        {
            Object actualValue = arguments.get( type );
            assertThat( actualValue ).isEqualTo( expectedValue );
        }
    }
}
