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
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.neo4j.consistency.ConsistencyCheckService.Result.success;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_planner;

public class OnlineBackupCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private final BackupTool tool = mock( BackupTool.class );
    private Path configDir;
    private ConsistencyCheckService consistencyCheckService;

    @Before
    public void setUp() throws Exception
    {
        configDir = testDirectory.directory( "config-dir" ).toPath();
    }

    @Test
    public void shouldNotRequestForensics() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--to=/" );

        verify( tool )
                .executeBackup( any(), any(), any(), any(), anyLong(), eq( false ) );
    }

    @Test
    public void shouldDefaultFromToDefaultBackupAddress()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--to=/" );

        verify( tool ).executeBackup(
                eq( new HostnamePort( "localhost", 6362 ) ), any(), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldDefaultPortAndPassHost() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server", "--to=/" );

        verify( tool ).executeBackup(
                eq( new HostnamePort( "foo.bar.server", 6362 ) ), any(), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldAcceptAHostWithATrailingColon() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server:", "--to=/" );

        verify( tool ).executeBackup(
                eq( new HostnamePort( "foo.bar.server", 6362 ) ), any(), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldDefaultHostAndPassPort() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=:1234", "--to=/" );

        verify( tool ).executeBackup(
                eq( new HostnamePort( "localhost", 1234 ) ), any(), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldPassHostAndPort() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--from=foo.bar.server:1234", "--to=/" );

        verify( tool ).executeBackup(
                eq( new HostnamePort( "foo.bar.server", 1234 ) ), any(), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldPassDestination() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--to=/some/path/or/other" );

        verify( tool ).executeBackup(
                any(), eq( new File( "/some/path/or/other" ) ), any(), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldTreatToArgumentAsMandatory() throws CommandFailed
    {
        try
        {
            execute();
            fail( "exception expected" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            assertThat( incorrectUsage.getMessage(), containsString( "to" ) );
        }
    }

    @Test
    public void shouldNotAskForConsistencyCheckIfNotSpecified()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--check-consistency=false", "--to=/" );

        verify( tool ).executeBackup( any(), any(), eq( ConsistencyCheck.NONE ), any(), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldAskForConsistencyCheckIfSpecified() throws Exception

    {
        consistencyCheckService = mock( ConsistencyCheckService.class );
        stub( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(), any(), any(), anyBoolean(),
                any() ) ).toReturn( success( null ) );
        ArgumentCaptor<ConsistencyCheck> captor = ArgumentCaptor.forClass( ConsistencyCheck.class );
        stub( tool.executeBackup( any(), any(), captor.capture(), any(), anyLong(), anyBoolean() ) ).toReturn( null );

        execute( "--check-consistency", "--to=/" );

        captor.getValue().runFull( null, null, null, null, null, null, false );

        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(), any(),
                anyBoolean(), eq( new File(".").getCanonicalFile()) );
    }

    @Test
    public void shouldSpecifyReportDirIfSpecified() throws Exception
    {
        consistencyCheckService = mock( ConsistencyCheckService.class );
        stub( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(), any(), any(), anyBoolean(),
                any() ) ).toReturn( success( null ) );
        ArgumentCaptor<ConsistencyCheck> captor = ArgumentCaptor.forClass( ConsistencyCheck.class );
        stub( tool.executeBackup( any(), any(), captor.capture(), any(), anyLong(), anyBoolean() ) ).toReturn( null );

        execute( "--check-consistency", "--to=/", "--cc-report-dir=" + Paths.get( "some", "dir" ) );

        captor.getValue().runFull( null, null, null, null, null, null, false );

        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(), any(),
                anyBoolean(), eq( new File("some/dir").getCanonicalFile() ) );
    }

    @Test
    public void shouldIncludeGraphDatabaseSettings()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), config.capture(), anyLong(), anyBoolean() );
        assertThat( config.getValue().getSettingsClasses(), hasItem( GraphDatabaseSettings.class ) );
    }

    @Test
    public void shouldIncludeConsistencyCheckSettings()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), config.capture(), anyLong(), anyBoolean() );
        assertThat( config.getValue().getSettingsClasses(), hasItem( ConsistencyCheckSettings.class ) );
    }

    @Test
    public void shouldReadStandardConfig() throws IOException, CommandFailed, IncorrectUsage, BackupTool
            .ToolFailureException

    {
        Files.write( configDir.resolve( "neo4j.conf" ), asList( cypher_planner.name() + "=RULE" ) );
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), config.capture(), anyLong(), anyBoolean() );
        assertThat( config.getValue().get( cypher_planner ), is( "RULE" ) );
    }

    @Test
    public void shouldAugmentConfig()
            throws IOException, CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        Path extraConf = testDirectory.directory( "someOtherDir" ).toPath().resolve( "extra.conf" );
        Files.write( extraConf, asList( cypher_planner.name() + "=RULE" ) );
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );

        execute( "--additional-config=" + extraConf, "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), config.capture(), anyLong(), anyBoolean() );
        assertThat( config.getValue().get( cypher_planner ), is( "RULE" ) );
    }

    @Test
    public void shouldDefaultTimeoutToTwentyMinutes()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), any(), eq( MINUTES.toMillis( 20 ) ), anyBoolean() );
    }

    @Test
    public void shouldInterpretAUnitlessTimeoutAsSeconds()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--timeout=10", "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), any(), eq( SECONDS.toMillis( 10 ) ), anyBoolean() );
    }

    @Test
    public void shouldParseATimeoutWithUnits()
            throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        execute( "--timeout=10h", "--to=/" );

        verify( tool ).executeBackup( any(), any(), any(), any(), eq( HOURS.toMillis( 10 ) ), anyBoolean() );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new OnlineBackupCommand.Provider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin backup [--from=<address>] --to=<backup-path>%n" +
                            "                          [--check-consistency[=<true|false>]]%n" +
                            "                          [--cc-report-dir=<directory>]%n" +
                            "                          [--additional-config=<config-file-path>]%n" +
                            "                          [--timeout=<timeout>]%n" +
                            "%n" +
                            "Perform a backup, over the network, from a running Neo4j server into a local%n" +
                            "copy of the database store (the backup). Neo4j Server must be configured to run%n" +
                            "a backup service. See http://neo4j.com/docs/operations-manual/current/backup/%n" +
                            "for more details.%n" +
                            "%n" +
                            "WARNING: this command is experimental and subject to change.%n" +
                            "%n" +
                            "options:%n" +
                            "  --from=<address>                         Host and port of Neo4j.%n" +
                            "                                           [default:localhost:6362]%n" +
                            "  --to=<backup-path>                       Directory where the backup will be%n" +
                            "                                           made; if there is already a backup%n" +
                            "                                           present an incremental backup will be%n" +
                            "                                           attempted.%n" +
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
        new OnlineBackupCommand( tool, Paths.get( "/some/path" ), configDir, consistencyCheckService )
                .execute( args );
    }
}
