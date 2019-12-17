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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.MutuallyExclusiveArgsException;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.CheckConsistencyCommand;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

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
        homeDir = testDirectory.homeDir().toPath();
        confPath = testDirectory.directory( "conf" ).toPath();
        prepareDatabase( neo4jLayout.databaseLayout( "mydb" ) );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new CheckConsistencyCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Check the consistency of a database.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "check-consistency (--database=<database> | --backup=<path>) [--verbose]%n" +
                "                  [--additional-config=<path>] [--check-graph=<true/false>]%n" +
                "                  [--check-index-structure=<true/false>]%n" +
                "                  [--check-indexes=<true/false>]%n" +
                "                  [--check-label-scan-store=<true/false>]%n" +
                "                  [--check-property-owners=<true/false>] [--report-dir=<path>]%n" +
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
                "                              Default: true%n" +
                "      --check-label-scan-store=<true/false>%n" +
                "                            Perform consistency checks on the label scan store.%n" +
                "                              Default: true%n" +
                "      --check-property-owners=<true/false>%n" +
                "                            Perform additional consistency checks on property%n" +
                "                              ownership. This check is very expensive in time%n" +
                "                              and memory.%n" +
                "                              Default: false"
        ) );
    }

    @Test
    void runsConsistencyChecker() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void consistencyCheckerRespectDatabaseLock() throws CannotWriteException, IOException
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( "mydb" );

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
    void enablesVerbosity() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void failsWhenInconsistenciesAreFound() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );
        when( consistencyCheckService
                .runFullConsistencyCheck( any( DatabaseLayout.class ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.failure( new File( "/the/report/path" ), new ConsistencySummaryStatistics() ) );

        CommandFailedException commandFailed =
                assertThrows( CommandFailedException.class, () ->
                {
                    CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
                    checkConsistencyCommand.execute();
                } );
        assertThat( commandFailed.getMessage() ).contains( new File( "/the/report/path" ).toString() );
    }

    @Test
    void shouldWriteReportFileToCurrentDirectoryByDefault()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        eq( new File( "." ).getCanonicalFile() ), any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldWriteReportFileToSpecifiedDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=some-dir-or-other" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "some-dir-or-other" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldCanonicalizeReportDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=" + Paths.get( "..", "bar" ) );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "../bar" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void passesOnCheckParameters() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--check-graph=false",
            "--check-indexes=false", "--check-index-structure=false", "--check-label-scan-store=false", "--check-property-owners=true" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        any(), eq( new ConsistencyFlags( false, false, false, false, true ) ) );
    }

    @Test
    void databaseAndBackupAreMutuallyExclusive() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

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
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        File backupPath = new File( homeDir.toFile(), "dir/does/not/exist" );

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
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        DatabaseLayout backupLayout = Neo4jLayout.ofFlat( testDirectory.directory( "backup" ) ).databaseLayout( DEFAULT_DATABASE_NAME );
        prepareBackupDatabase( backupLayout );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, confPath ), consistencyCheckService );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null, null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--backup=" + backupLayout.databaseDirectory() );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    private void prepareBackupDatabase( DatabaseLayout backupLayout ) throws IOException
    {
        testDirectory.getFileSystem().deleteRecursively( homeDir.toFile() );
        prepareDatabase( backupLayout );
    }

    private void prepareDatabase( DatabaseLayout databaseLayout )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
        managementService.shutdown();
    }
}
