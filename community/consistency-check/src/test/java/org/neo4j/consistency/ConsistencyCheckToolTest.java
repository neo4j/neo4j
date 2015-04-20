/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.Recovery;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

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
                any( ProgressMonitorFactory.class ), any( StringLogger.class ) );
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
        assertEquals( TaskExecutionOrder.MULTI_PASS,
                config.getValue().get( ConsistencyCheckSettings.consistency_check_execution_order ) );
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
                any( ProgressMonitorFactory.class ), any( StringLogger.class ) );
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
            assertThat( e.getMessage(), containsString( "USAGE:" ) );
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

    @Test
    public void shouldExecuteRecoveryWhenStoreWasNonCleanlyShutdown() throws Exception
    {
        // Given
        createGraphDbAndKillIt();

        Monitors monitors = new Monitors();
        Recovery.Monitor listener = mock( Recovery.Monitor.class );
        monitors.addMonitorListener( listener );

        ConsistencyCheckTool consistencyCheckTool = newConsistencyCheckToolWith( monitors,
                new StoreRecoverer( fs.get() ), mock( ConsistencyCheckTool.ExitHandle.class ) );

        // When
        consistencyCheckTool.run( "-recovery", storeDirectory.absolutePath() );

        // Then
        verify( listener ).recoveryRequired( anyLong() );
        verify( listener ).recoveryCompleted();
    }

    @Test
    public void shouldExitWhenRecoveryNeededButRecoveryFalseOptionSpecified() throws Exception
    {
        // Given
        createGraphDbAndKillIt();

        Monitors monitors = new Monitors();
        PhysicalLogFile.Monitor listener = mock( PhysicalLogFile.Monitor.class );
        monitors.addMonitorListener( listener );

        ConsistencyCheckTool.ExitHandle exitHandle = mock( ConsistencyCheckTool.ExitHandle.class );
        StoreRecoverer storeRecoverer = mock( StoreRecoverer.class );
        when( storeRecoverer.recoveryNeededAt( any( File.class ) ) ).thenReturn( true );

        ConsistencyCheckTool consistencyCheckTool = newConsistencyCheckToolWith( monitors, storeRecoverer, exitHandle );

        // When
        consistencyCheckTool.run( "-recovery=false", storeDirectory.absolutePath() );

        // Then
        verifyZeroInteractions( listener );
        verify( exitHandle ).pull();
    }

    private void createGraphDbAndKillIt()
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( storeDirectory.absolutePath() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "FOO" ) );
            db.createNode( label( "BAR" ) );
            tx.success();
        }

        fs.snapshot( shutdownDbAction( db ) );
    }

    private ConsistencyCheckTool newConsistencyCheckToolWith( Monitors monitors, StoreRecoverer storeRecoverer,
            ConsistencyCheckTool.ExitHandle exitHandle ) throws IOException
    {
        GraphDatabaseFactory graphDbFactory = new TestGraphDatabaseFactory()
        {
            @Override
            public GraphDatabaseService newEmbeddedDatabase( String path )
            {
                return newImpermanentDatabase( path );
            }
        }.setFileSystem( fs.get() ).setMonitors( monitors );

        return new ConsistencyCheckTool( mock( ConsistencyCheckService.class ), storeRecoverer,
                graphDbFactory, mock( PrintStream.class ), exitHandle );
    }

    @Rule
    public TargetDirectory.TestDirectory storeDirectory = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
