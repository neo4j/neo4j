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
package org.neo4j.backup;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.system.SystemExitRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class BackupToolTest
{
    @Rule
    public SystemExitRule systemExitRule = SystemExitRule.none();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldToolFailureExceptionCauseExitCode()
    {
        systemExitRule.expectExit( 1 );
        BackupTool.exitFailure( "tool failed" );
    }

    @Test
    public void shouldBackupToolMainCauseExitCode()
    {
        systemExitRule.expectExit( 1 );
        BackupTool.main( new String[]{} );
    }

    @Test
    public void shouldUseIncrementalOrFallbackToFull() throws Exception
    {
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ),
                eq( BackupServer.DEFAULT_PORT ), eq( new File( "my_backup" ) ), eq( ConsistencyCheck.FULL ),
                any( Config.class ), eq( BackupClient.BIG_READ_TIMEOUT ), eq( false ) );
        verify( systemOut ).println(
                "Performing backup from '" + new HostnamePort( "localhost", BackupServer.DEFAULT_PORT ) + "'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldResetTimeout() throws Exception
    {
        String newTimeout = "3"; /*seconds by default*/
        long expectedTimeout = 3 * 1000;
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-timeout", newTimeout};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( new File( "my_backup" ) ), eq( ConsistencyCheck.FULL ), any( Config.class ), eq( expectedTimeout ), eq( false ) );
        verify( systemOut ).println(
                "Performing backup from '" + new HostnamePort( "localhost", BackupServer.DEFAULT_PORT ) + "'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldIgnoreIncrementalFlag() throws Exception
    {
        String[] args = new String[]{"-incremental", "-host", "localhost", "-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( new File( "my_backup" ) ), eq( ConsistencyCheck.FULL ), any( Config.class ),
                eq( BackupClient.BIG_READ_TIMEOUT ), eq( false ) );
        verify( systemOut ).println(
                "Performing backup from '" + new HostnamePort( "localhost", BackupServer.DEFAULT_PORT ) + "'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldIgnoreFullFlag() throws Exception
    {
        String[] args = new String[]{"-full", "-host", "localhost", "-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        when( service.directoryContainsDb( any( FileSystemAbstraction.class), eq( new File( "my_backup" ) ) ) )
                .thenReturn( true );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( new File( "my_backup" ) ), eq( ConsistencyCheck.FULL ), any( Config.class ),
                eq( BackupClient.BIG_READ_TIMEOUT ), eq( false ) );
        verify( systemOut ).println(
                "Performing backup from '" + new HostnamePort( "localhost", BackupServer.DEFAULT_PORT ) + "'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void appliesDefaultTuningConfigurationForConsistencyChecker() throws Exception
    {
        // given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), eq( new File( "my_backup" ) ),
                any( ConsistencyCheck.class ), config.capture(), eq( BackupClient.BIG_READ_TIMEOUT ), eq( false ) );
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    public void passesOnConfigurationIfProvided() throws Exception
    {
        // given
        File configFile = testDirectory.file( "neo4j.conf" );
        Properties properties = new Properties();
        properties.setProperty( ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        properties.store( new FileWriter( configFile ), null );

        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-config", configFile.getPath()};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), eq( new File( "my_backup" ) ),
                any( ConsistencyCheck.class ), config.capture(), anyLong(), eq( false ) );
        assertTrue( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    public void exitWithFailureIfConfigSpecifiedButConfigFileDoesNotExist() throws Exception
    {
        // given
        File configFile = testDirectory.file( "nonexistent_file" );
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-config", configFile.getPath()};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool backupTool = new BackupTool( service, systemOut );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally" );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Could not read configuration file" ) );
            assertThat( e.getCause(), instanceOf( IOException.class ) );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool backupTool = new BackupTool( service, systemOut );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally" );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            // then
            assertEquals( BackupTool.NO_SOURCE_SPECIFIED, e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfInvalidSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-host", "foo:localhost", "-port", "123", "-to", "my_backup"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool backupTool = new BackupTool( service, systemOut );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally" );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            // then
            assertEquals( BackupTool.WRONG_FROM_ADDRESS_SYNTAX, e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoDestinationSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-host", "localhost"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool backupTool = new BackupTool( service, systemOut );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally" );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            // then
            assertEquals( "Specify target location with -to <target-directory>", e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void helpMessageForWrongUriShouldNotContainSchema() throws BackupTool.ToolFailureException
    {
        // given
        String[] args = new String[]{"-host", ":VeryWrongURI:", "-to", "/var/backup/graph"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        try
        {
            // when
            new BackupTool( service, systemOut ).run( args );
            fail( "should exit abnormally" );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), equalTo( BackupTool.WRONG_FROM_ADDRESS_SYNTAX ) );
            assertThat( e.getMessage(), not( containsString( "<schema>" ) ) );
        }

        verifyZeroInteractions( service, systemOut );
    }

    @Test
    public void shouldRespectVerifyFlagWithLegacyArguments() throws BackupTool.ToolFailureException
    {
        // Given
        String host = "localhost";
        File targetDir = new File( "/var/backup/neo4j/" ).getAbsoluteFile();
        String[] args = {"-from", host, "-to", targetDir.getAbsolutePath(), "-verify", "false"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // When
        new BackupTool( service, systemOut ).run( args );

        // Then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( host ), eq( BackupServer.DEFAULT_PORT ),
                eq( targetDir ), eq( ConsistencyCheck.NONE ), any( Config.class ), eq( BackupClient.BIG_READ_TIMEOUT ),
                eq( false ) );
        verify( systemOut ).println(
                "Performing backup from '" + new HostnamePort( host, BackupServer.DEFAULT_PORT ) + "'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldMakeUseOfDebugArgument() throws Exception
    {
        // given
        String[] args = new String[]{"-from", "localhost", "-to", "my_backup", "-gather-forensics"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), eq( new File( "my_backup" ) ),
                any( ConsistencyCheck.class ), any( Config.class ), anyLong(), eq( true ) );
    }

    @Test
    public void shouldHaveNoConsistencyCheckIfVerifyFalse() throws Exception
    {
        // Given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-verify", "false"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // When
        new BackupTool( service, systemOut ).run( args );

        // Then
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), any( File.class ),
                eq( ConsistencyCheck.NONE ), any( Config.class ), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldIgnoreConsistencyCheckIfVerifyFalse() throws Exception
    {
        // Given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-verify", "false",
                "-consistency-checker", "legacy"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // When
        new BackupTool( service, systemOut ).run( args );

        // Then
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), any( File.class ),
                eq( ConsistencyCheck.NONE ), any( Config.class ), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldHaveDefaultConsistencyCheckIfVerifyTrue() throws Exception
    {
        // Given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-verify", "true",};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // When
        new BackupTool( service, systemOut ).run( args );

        // Then
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), any( File.class ),
                eq( ConsistencyCheck.FULL ), any( Config.class ), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldRespectConsistencyCheckerWithDefaultVerify() throws Exception
    {
        // Given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup",
                "-consistency-checker", "full"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // When
        new BackupTool( service, systemOut ).run( args );

        // Then
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), any( File.class ),
                eq( ConsistencyCheck.FULL ), any( Config.class ), anyLong(), anyBoolean() );
    }

    @Test
    public void shouldCrashIfInvalidConsistencyCheckerSpecified() throws Exception
    {
        // Given
        String[] args = new String[]{"-host", "localhost", "-to", "my_backup", "-verify", "true",
                "-consistency-checker", "notarealname"};
        BackupProtocolService service = mock( BackupProtocolService.class );
        PrintStream systemOut = mock( PrintStream.class );

        try
        {
            // When
            new BackupTool( service, systemOut ).run( args );
            fail( "Should throw exception if invalid consistency checker is specified." );
        }
        catch ( IllegalArgumentException t )
        {
            // Then
            assertThat( t.getMessage(), containsString( "Unknown consistency check name" ) );
        }
    }
}
