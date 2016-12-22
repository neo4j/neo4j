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
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.OnlineBackupCommand.MAX_OLD_BACKUPS;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_planner;

public class OnlineBackupCommandTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private BackupService backupService = mock( BackupService.class );
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private Path configDir;
    private ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );
    private ConsistencyCheckService.Result ccResult = mock( ConsistencyCheckService.Result.class );
    private PrintStream out = mock( PrintStream.class );
    private PrintStream err = mock( PrintStream.class );
    private FileSystemAbstraction mockFs = mock( FileSystemAbstraction.class );

    @Before
    public void setUp() throws Exception
    {
        when( outsideWorld.fileSystem() ).thenReturn( new DefaultFileSystemAbstraction() );
        when( outsideWorld.errorStream() ).thenReturn( err );
        when( outsideWorld.outStream() ).thenReturn( out );
        when( ccResult.isSuccessful() ).thenReturn( true );
        when( consistencyCheckService
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(), any() ) )
                .thenReturn( ccResult );
        configDir = testDirectory.directory( "config-dir" ).toPath();
    }

    @Test
    public void shouldNotRequestForensics() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), eq( false ) );
    }

    @Test
    public void shouldDefaultFromToDefaultBackupAddress()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( eq( "localhost" ), eq( 6362 ), any(), any(), any(), anyLong(),
                anyBoolean() );
    }

    @Test
    public void shouldDefaultPortAndPassHost() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( eq( "foo.bar.server" ), eq( 6362 ), any(), any(), any(), anyLong(),
                anyBoolean() );
    }

    @Test
    public void shouldAcceptAHostWithATrailingColon()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server:", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( eq( "foo.bar.server" ), eq( 6362 ), any(), any(), any(), anyLong(),
                anyBoolean() );
    }

    @Test
    public void shouldDefaultHostAndPassPort() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=:1234", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( eq( "localhost" ), eq( 1234 ), any(), any(), any(), anyLong(),
                anyBoolean() );
    }

    @Test
    public void shouldPassHostAndPort() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server:1234", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( eq( "foo.bar.server" ), eq( 1234 ), any(), any(), any(), anyLong(),
                anyBoolean() );
    }

    @Test
    public void shouldPassDestination() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup(
                any(), anyInt(), eq( new File( "/mybackup" ) ), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void nonExistingBackupDirThrows() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        expected.expect( CommandFailed.class );
        expected.expectMessage( "Directory '/Idontexist/sasdfasdfa' does not exist." );
        execute( "--backup-dir=/Idontexist/sasdfasdfa", "--name=mybackup" );
    }

    @Test
    public void shouldTreatBackupDirArgumentAsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'backup-dir'" );
        execute();
    }

    @Test
    public void shouldTreatNameArgumentAsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'name'" );
        execute( "--backup-dir=/" );
    }

    @Test
    public void shouldNotAskForConsistencyCheckIfNotSpecified()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--check-consistency=false", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verifyNoMoreInteractions( consistencyCheckService );
    }

    @Test
    public void shouldAskForConsistencyCheckIfSpecified() throws Exception

    {
        execute( "--check-consistency=true", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(),
                anyBoolean(), eq( new File( "." ).getCanonicalFile() ) );
    }

    @Test
    public void shouldAskForConsistencyCheckIfSpecifiedIncremental() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        assertTrue( new File( dir, "afile" ).createNewFile() );

        execute( "--check-consistency=true", "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );

        verify( backupService ).doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() );
        verifyNoMoreInteractions( backupService );
        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(),
                anyBoolean(), eq( new File( "." ).getCanonicalFile() ) );
    }

    @Test
    public void shouldNotAskForConsistencyCheckIfSpecifiedIncremental() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        assertTrue( new File( dir, "afile" ).createNewFile() );

        execute( "--check-consistency=false", "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );

        verify( backupService ).doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() );
        verifyNoMoreInteractions( backupService );
        verifyNoMoreInteractions( consistencyCheckService );
    }

    @Test
    public void failedCCIsReported() throws Exception

    {
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(), any(),
                anyBoolean(), eq( new File( "." ).getCanonicalFile() ) ) ).thenReturn(
                ConsistencyCheckService.Result.failure( new File( "/foo/bar" ) ) );

        expected.expect( CommandFailed.class );
        expected.expectMessage( "Inconsistencies found. See '/foo/bar' for details." );
        execute( "--check-consistency=true", "--backup-dir=/", "--name=mybackup" );
    }

    @Test
    public void shouldDoFullIfDirectoryDoesNotExist() throws Exception

    {
        File dir = testDirectory.directory( "ccFull" );
        assertTrue( dir.delete() );

        execute( "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( outsideWorld ).stdOutLine( "Doing full backup..." );
        verify( outsideWorld ).stdOutLine( "Backup complete." );
        verifyNoMoreInteractions( backupService );
    }

    @Test
    public void shouldDoFullIfDirectoryEmpty() throws Exception

    {
        File dir = testDirectory.directory( "ccFull" );

        execute( "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( outsideWorld ).stdOutLine( "Doing full backup..." );
        verifyNoMoreInteractions( backupService );
    }

    @Test
    public void shouldDoIncrementalIfDirectoryNonEmpty() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        assertTrue( new File( dir, "afile" ).createNewFile() );

        execute( "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );

        verify( backupService ).doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() );
        verify( outsideWorld ).stdOutLine( "Destination is not empty, doing incremental backup..." );
        verifyNoMoreInteractions( backupService );
    }

    @Test
    public void shouldFallbackToFullIfIncrementalFails() throws Exception

    {
        when( outsideWorld.fileSystem() ).thenReturn( mockFs );
        File dir = testDirectory.directory( "ccInc" );
        when( mockFs.isDirectory( eq( dir.getParentFile() ) ) ).thenReturn( true );
        when( mockFs.listFiles( eq( dir ) ) ).thenReturn( new File[]{dir} );
        when( backupService.doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() ) )
                .thenThrow( new RuntimeException( "nah-ah" ) );

        execute( "--cc-report-dir=" + dir.getParent(), "--backup-dir=" + dir.getParent(),
                "--name=" + dir.getName() );

        verify( backupService ).doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() );
        verify( outsideWorld ).stdOutLine( "Destination is not empty, doing incremental backup..." );
        verify( outsideWorld ).stdErrLine( "Incremental backup failed: nah-ah" );
        verify( outsideWorld ).stdErrLine( "Old backup renamed to 'ccInc.err.1'." );
        verify( mockFs ).renameFile( eq( dir ), eq( testDirectory.directory( "ccInc.err.1" ) ) );
        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( outsideWorld ).stdOutLine( "Doing full backup..." );
        verifyNoMoreInteractions( backupService );
    }

    @Test
    public void failToRenameIsReported() throws Exception
    {
        when( outsideWorld.fileSystem() ).thenReturn( mockFs );
        File dir = testDirectory.directory( "ccInc" );
        when( mockFs.isDirectory( eq( dir.getParentFile() ) ) ).thenReturn( true );
        when( mockFs.listFiles( eq( dir ) ) ).thenReturn( new File[]{dir} );
        when( backupService.doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() ) )
                .thenThrow( new RuntimeException( "nah-ah" ) );

        doThrow( new IOException( "kaboom" ) ).when( mockFs ).renameFile( any(), any() );

        expected.expectMessage( "Failed to move old backup out of the way: kaboom" );
        expected.expect( CommandFailed.class );

        execute( "--cc-report-dir=" + dir.getParent(), "--backup-dir=" + dir.getParent(),
                "--name=" + dir.getName() );
    }

    @Test
    public void shouldNotFallbackToFullIfSpecified() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        assertTrue( new File( dir, "afile" ).createNewFile() );
        when( backupService.doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() ) )
                .thenThrow( new RuntimeException( "nah-ah" ) );

        expected.expectMessage( "Backup failed: nah-ah" );
        expected.expect( CommandFailed.class );

        execute( "--fallback-to-full=false", "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );
    }

    @Test
    public void renamingOldBackupIncrements() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        when( outsideWorld.fileSystem() ).thenReturn( mockFs );
        when( mockFs.isDirectory( eq( dir.getParentFile() ) ) ).thenReturn( true );
        when( mockFs.listFiles( eq( dir ) ) ).thenReturn( new File[]{dir} );
        for ( int i = 1; i < 50; i++ )
        {
            when( mockFs.fileExists( eq( new File( dir.getParentFile(), "ccInc.err." + i ) ) ) ).thenReturn( true );
        }
        when( backupService.doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() ) )
                .thenThrow( new RuntimeException( "nah-ah" ) );

        execute( "--cc-report-dir=" + dir.getParent(), "--backup-dir=" + dir.getParent(),
                "--name=" + dir.getName() );

        verify( backupService ).doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() );
        verify( outsideWorld ).stdOutLine( "Destination is not empty, doing incremental backup..." );
        verify( outsideWorld ).stdErrLine( "Incremental backup failed: nah-ah" );
        verify( outsideWorld ).stdErrLine( "Old backup renamed to 'ccInc.err.50'." );
        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( outsideWorld ).stdOutLine( "Doing full backup..." );
        verifyNoMoreInteractions( backupService );
    }

    @Test
    public void renamingOldBackupIncrementsOnlySoFar() throws Exception

    {
        File dir = testDirectory.directory( "ccInc" );
        when( outsideWorld.fileSystem() ).thenReturn( mockFs );
        when( mockFs.isDirectory( eq( dir.getParentFile() ) ) ).thenReturn( true );
        when( mockFs.listFiles( eq( dir ) ) ).thenReturn( new File[]{dir} );
        for ( int i = 1; i < MAX_OLD_BACKUPS; i++ )
        {
            when( mockFs.fileExists( eq( new File( dir.getParentFile(), "ccInc.err." + i ) ) ) ).thenReturn( true );
        }
        when( backupService.doIncrementalBackup( any(), anyInt(), any(), anyLong(), any() ) )
                .thenThrow( new RuntimeException( "nah-ah" ) );

        expected.expect( CommandFailed.class );
        expected.expectMessage( "ailed to move old backup out of the way: too many old backups." );
        execute( "--cc-report-dir=" + dir.getParent(), "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );
    }

    @Test
    public void shouldSpecifyReportDirIfSpecified() throws Exception
    {
        File reportDir = testDirectory.directory( "ccreport" );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                any() ) ).thenReturn( ConsistencyCheckService.Result.success( null ) );

        execute( "--check-consistency", "--backup-dir=/", "--name=mybackup",
                "--cc-report-dir=" + reportDir );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() );
        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(),
                anyBoolean(), eq( reportDir.getCanonicalFile() ) );
    }

    @Test
    public void fullFailureIsReported() throws Exception

    {
        File dir = testDirectory.directory( "ccFull" );

        when( backupService.doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() ) )
                .thenThrow( new RuntimeException( "nope" ) );

        expected.expect( CommandFailed.class );
        expected.expectMessage( "Backup failed: nope" );
        execute( "--backup-dir=" + dir.getParent(), "--name=" + dir.getName() );
    }

    @Test
    public void reportDirMustBeAPath() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "cc-report-dir must be a path" );
        execute( "--check-consistency", "--backup-dir=/", "--name=mybackup",
                "--cc-report-dir" );
    }

    @Test
    public void reportDirMustExist() throws Exception
    {
        expected.expect( CommandFailed.class );
        expected.expectMessage( "Directory '/aalivnmoimzlckmvPDK' does not exist." );
        execute( "--check-consistency", "--backup-dir=/", "--name=mybackup",
                "--cc-report-dir=/aalivnmoimzlckmvPDK" );
    }

    @Test
    public void shouldIncludeGraphDatabaseSettings()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), config.capture(), anyLong(),
                anyBoolean() );
        assertThat( config.getValue().getSettingsClasses(), hasItem( GraphDatabaseSettings.class ) );
    }

    @Test
    public void shouldIncludeConsistencyCheckSettings()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), config.capture(), anyLong(),
                anyBoolean() );
        assertThat( config.getValue().getSettingsClasses(), hasItem( ConsistencyCheckSettings.class ) );
    }

    @Test
    public void shouldReadStandardConfig() throws IOException, CommandFailed, IncorrectUsage, BackupTool
            .ToolFailureException

    {
        Files.write( configDir.resolve( "neo4j.conf" ), singletonList( cypher_planner.name() + "=RULE" ) );
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), config.capture(), anyLong(),
                anyBoolean() );
        assertThat( config.getValue().get( cypher_planner ), is( "RULE" ) );
    }

    @Test
    public void shouldAugmentConfig()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        Path extraConf = testDirectory.directory( "someOtherDir" ).toPath().resolve( "extra.conf" );
        Files.write( extraConf, singletonList( cypher_planner.name() + "=RULE" ) );
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--additional-config=" + extraConf, "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), config.capture(), anyLong(),
                anyBoolean() );
        assertThat( config.getValue().get( cypher_planner ), is( "RULE" ) );
    }

    @Test
    public void shouldDefaultTimeoutToTwentyMinutes()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(),
                eq( MINUTES.toMillis( 20 ) ),
                anyBoolean() );
    }

    @Test
    public void shouldInterpretAUnitlessTimeoutAsSeconds()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--timeout=10", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(),
                eq( SECONDS.toMillis( 10 ) ),
                anyBoolean() );
    }

    @Test
    public void shouldParseATimeoutWithUnits()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--timeout=10h", "--backup-dir=/", "--name=mybackup" );

        verify( backupService ).doFullBackup( any(), anyInt(), any(), any(), any(),
                eq( HOURS.toMillis( 10 ) ),
                anyBoolean() );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new OnlineBackupCommand.Provider(), ps::println );

            assertEquals(
                    String.format( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<graph.db-backup>%n" +
                            "                          [--from=<address>] [--fallback-to-full[=<true|false>]]%n" +
                            "                          [--check-consistency[=<true|false>]]%n" +
                            "                          [--cc-report-dir=<directory>]%n" +
                            "                          [--additional-config=<config-file-path>]%n" +
                            "                          [--timeout=<timeout>]%n" +
                            "%n" +
                            "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                            "service must have been configured on the server beforehand. See%n" +
                            "http://neo4j.com/docs/operations-manual/current/backup/ for more details.%n" +
                            "%n" +
                            "options:%n" +
                            "  --backup-dir=<backup-path>               Directory to place backup in.%n" +
                            "  --name=<graph.db-backup>                 Name of backup. If a backup with this%n" +
                            "                                           name already exists an incremental%n" +
                            "                                           backup will be attempted.%n" +
                            "  --from=<address>                         Host and port of Neo4j.%n" +
                            "                                           [default:localhost:6362]%n" +
                            "  --fallback-to-full=<true|false>          If an incremental backup fails backup%n" +
                            "                                           will move the old backup to%n" +
                            "                                           <name>.err.<N> and fallback to a full%n" +
                            "                                           backup instead. [default:true]%n" +
                            "  --check-consistency=<true|false>         If a consistency check should be%n" +
                            "                                           made. [default:true]%n" +
                            "  --cc-report-dir=<directory>              Directory where consistency report%n" +
                            "                                           will be written. [default:.]%n" +
                            "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                            "                                           additional configuration in.%n" +
                            "                                           [default:]%n" +
                            "  --timeout=<timeout>                      Timeout in the form <time>[ms|s|m|h],%n" +
                            "                                           where the default unit is seconds.%n" +
                            "                                           [default:20m]%n" ),
                    baos.toString() );
        }
    }

    private void execute( String... args ) throws IncorrectUsage, CommandFailed
    {
        new OnlineBackupCommand( backupService, Paths.get( "/some/path" ), configDir, consistencyCheckService,
                outsideWorld ).execute( args );
    }
}
