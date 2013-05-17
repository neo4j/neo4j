/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.store.windowpool.WindowPoolImplementation;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.helpers.Settings.osIsWindows;

public class ConsistencyCheckToolTest
{
    @Test
    public void runsConsistencyCheck() throws Exception
    {
        // given
        String storeDirectoryPath = storeDirectory.directory().getPath();
        String[] args = {storeDirectoryPath};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemError = mock( PrintStream.class );

        // when
        new ConsistencyCheckTool( service, systemError ).run( args );

        // then
        verify( service ).runFullConsistencyCheck( eq( storeDirectoryPath ), any( Config.class ),
                any( ProgressMonitorFactory.class ), any( StringLogger.class ) );
    }

    @Test
    public void appliesDefaultTuningConfigurationForConsistencyChecker() throws Exception
    {
        // given
        String[] args = {storeDirectory.directory().getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new ConsistencyCheckTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( anyString(), config.capture(),
                any( ProgressMonitorFactory.class ), any( StringLogger.class ));
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
        assertEquals( TaskExecutionOrder.MULTI_PASS,
                config.getValue().get( ConsistencyCheckSettings.consistency_check_execution_order ) );
        WindowPoolImplementation expectedPoolImplementation = !osIsWindows() ?
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

        String[] args = {storeDirectory.directory().getPath(), "-config", propertyFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new ConsistencyCheckTool( service, systemOut ).run( args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( anyString(), config.capture(),
                any( ProgressMonitorFactory.class ), any( StringLogger.class ));
        assertTrue( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    public void exitWithFailureIndicatingCorrectUsageIfNoArgumentsSupplied() throws Exception
    {
        // given
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        String[] args = {};
        PrintStream systemError = mock( PrintStream.class );

        try
        {
            // when
            new ConsistencyCheckTool( service, systemError ).run( args );
            fail( "should have thrown exception" );
        }
        catch ( ConsistencyCheckTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "USAGE:" ));
        }
    }
    
    @Test
    public void exitWithFailureIfConfigSpecifiedButPropertiesFileDoesNotExist() throws Exception
    {
        // given
        File propertyFile = TargetDirectory.forTest( getClass() ).file( "nonexistent_file" );
        String[] args = {storeDirectory.directory().getPath(), "-config", propertyFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );
        ConsistencyCheckTool ConsistencyCheckTool = new ConsistencyCheckTool( service, systemOut );

        try
        {
            // when
            ConsistencyCheckTool.run( args );
            fail( "should have thrown exception" );
        }
        catch ( ConsistencyCheckTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Could not read configuration properties file" ) );
            assertThat( e.getCause(), instanceOf( IOException.class ) );
        }

        verifyZeroInteractions( service );
    }

    @Rule
    public TargetDirectory.TestDirectory storeDirectory = TargetDirectory.testDirForTest( getClass() );
}
