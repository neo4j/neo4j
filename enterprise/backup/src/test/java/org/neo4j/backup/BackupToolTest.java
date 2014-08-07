/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.store.windowpool.WindowPoolImplementation;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.TargetDirectory;

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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class BackupToolTest
{
    @Test
    public void shouldUseIncrementalOrFallbackToFull() throws Exception
    {
        String[] args = new String[]{"-from", "localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ),
                eq( BackupServer.DEFAULT_PORT ), eq( "my_backup" ), eq( true ), any( Config.class ) );
        verify( systemOut ).println( "Performing backup from 'localhost'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldIgnoreIncrementalFlag() throws Exception
    {
        String[] args = new String[]{"-incremental", "-from", "localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( "my_backup" ), eq( true ), any( Config.class ) );
        verify( systemOut ).println( "Performing backup from 'localhost'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void shouldIgnoreFullFlag() throws Exception
    {
        String[] args = new String[]{"-full", "-from", "localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        when( service.directoryContainsDb( anyString() ) ).thenReturn( true );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        verify( service ).doIncrementalBackupOrFallbackToFull( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( "my_backup" ), eq( true ), any( Config.class ) );
        verify( systemOut ).println( "Performing backup from 'localhost'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void appliesDefaultTuningConfigurationForConsistencyChecker() throws Exception
    {
        // given
        String[] args = new String[]{"-from", "localhost",
                "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), anyString(), anyBoolean(),
                config.capture() );
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
        assertEquals( TaskExecutionOrder.MULTI_PASS,
                config.getValue().get( ConsistencyCheckSettings.consistency_check_execution_order ) );
        WindowPoolImplementation expectedPoolImplementation = !Settings.osIsWindows() ?
                WindowPoolImplementation.SCAN_RESISTANT :
                WindowPoolImplementation.MOST_FREQUENTLY_USED;
        assertEquals( expectedPoolImplementation,
                config.getValue().get( ConsistencyCheckSettings.consistency_check_window_pool_implementation ) );
    }

    @Test
    public void passesOnConfigurationIfProvided() throws Exception
    {
        // given
        File propertyFile = TargetDirectory.forTest( getClass() ).file( "neo4j.properties" );
        Properties properties = new Properties();
        properties.setProperty( ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        properties.store( new FileWriter( propertyFile ), null );

        String[] args = new String[]{"-from", "localhost",
                "-to", "my_backup", "-config", propertyFile.getPath()};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).doIncrementalBackupOrFallbackToFull( anyString(), anyInt(), anyString(), anyBoolean(),
                config.capture() );
        assertTrue( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    public void exitWithFailureIfConfigSpecifiedButPropertiesFileDoesNotExist() throws Exception
    {
        // given
        File propertyFile = TargetDirectory.forTest( getClass() ).file( "nonexistent_file" );
        String[] args = new String[]{"-from", "localhost",
                "-to", "my_backup", "-config", propertyFile.getPath()};
        BackupService service = mock( BackupService.class );
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
            assertThat( e.getMessage(), containsString( "Could not read configuration properties file" ) );
            assertThat( e.getCause(), instanceOf( IOException.class ) );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-to", "my_backup"};
        BackupService service = mock( BackupService.class );
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
            assertEquals( "Please specify -from, examples:\n" +
                            "  -from 192.168.1.34\n" +
                            "  -from 192.168.1.34:1234\n" +
                            "  -from 192.168.1.15:2181,192.168.1.16:2181",
                    e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfInvalidSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-from", "foo:localhost:123", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
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
            assertEquals( BackupTool.WRONG_URI_SYNTAX, e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoDestinationSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-from", "localhost"};
        BackupService service = mock( BackupService.class );
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
            assertEquals( "Specify target location with -to <target-directory>",
                    e.getMessage() );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void helpMessageForWrongUriShouldNotContainSchema() throws BackupTool.ToolFailureException
    {
        // given
        String[] args = new String[]{"-from", ":VeryWrongURI:", "-to", "/var/backup/graph"};
        BackupService service = mock( BackupService.class );
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
            assertThat( e.getMessage(), equalTo( BackupTool.WRONG_URI_SYNTAX ) );
            assertThat( e.getMessage(), not( containsString( "<schema>" ) ) );
        }

        verifyZeroInteractions( service, systemOut );
    }
}
