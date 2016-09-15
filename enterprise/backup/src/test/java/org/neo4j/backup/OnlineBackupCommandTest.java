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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_planner;

public class OnlineBackupCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private final BackupTool tool = mock( BackupTool.class );
    private OnlineBackupCommand command;
    private Path configDir;

    public OnlineBackupCommandTest()
    {
    }

    @Before
    public void setUp() throws Exception
    {
        configDir = testDirectory.directory( "config-dir" ).toPath();
        command = new OnlineBackupCommand( tool, Paths.get( "/some/path" ), configDir );
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
    public void shouldAskForConsistencyCheckIfSpecified()
            throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        execute( "--check-consistency", "--to=/" );

        verify( tool ).executeBackup( any(), any(), eq( ConsistencyCheck.FULL ), any(), anyLong(), anyBoolean() );
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

    private void execute( String... args ) throws IncorrectUsage, CommandFailed
    {
        command.execute( args );
    }
}
