/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.neo4j.consistency.ConsistencyCheckTool.ToolFailureException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.SystemExitRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

public class ConsistencyCheckToolTest
{
    private SystemExitRule systemExitRule = SystemExitRule.none();
    private TargetDirectory.TestDirectory storeDirectory = TargetDirectory.testDirForTest( getClass() );
    private EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( systemExitRule ).around( storeDirectory ).around( fs );

    @Test
    public void runsConsistencyCheck() throws Exception
    {
        // given
        File storeDir = storeDirectory.directory();
        String[] args = {storeDir.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemError = mock( PrintStream.class );

        // when
        runConsistencyCheckToolWith( service, systemError, args );

        // then
        verify( service ).runFullConsistencyCheck( eq( storeDir ), any( Config.class ),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean() );
    }

    @Test
    public void appliesDefaultTuningConfigurationForConsistencyChecker() throws Exception
    {
        // given
        File storeDir = storeDirectory.directory();
        String[] args = {storeDir.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        runConsistencyCheckToolWith( service, systemOut, args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( eq( storeDir ), config.capture(),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean() );
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    public void passesOnConfigurationIfProvided() throws Exception
    {
        // given
        File storeDir = storeDirectory.directory();
        File configFile = storeDirectory.file( "neo4j.conf" );
        Properties properties = new Properties();
        properties.setProperty( ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        properties.store( new FileWriter( configFile ), null );

        String[] args = {storeDir.getPath(), "-config", configFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        runConsistencyCheckToolWith( service, systemOut, args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( eq( storeDir ), config.capture(),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean() );
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
            runConsistencyCheckToolWith( service, systemError, args );
            fail( "should have thrown exception" );
        }
        catch ( ConsistencyCheckTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "USAGE:" ) );
        }
    }

    @Test
    public void exitWithFailureIfConfigSpecifiedButConfigFileDoesNotExist() throws Exception
    {
        // given
        File configFile = storeDirectory.file( "nonexistent_file" );
        String[] args = {storeDirectory.directory().getPath(), "-config", configFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        PrintStream systemOut = mock( PrintStream.class );

        try
        {
            // when
            runConsistencyCheckToolWith( service, systemOut, args );
            fail( "should have thrown exception" );
        }
        catch ( ConsistencyCheckTool.ToolFailureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Could not read configuration file" ) );
            assertThat( e.getCause(), instanceOf( IOException.class ) );
        }

        verifyZeroInteractions( service );
    }

    @Test
    public void failWhenStoreWasNonCleanlyShutdown() throws Exception
    {
        systemExitRule.expectExit( 1 );
        createGraphDbAndKillIt();

        runConsistencyCheckToolWith( fs.get(), storeDirectory.graphDbDir().getAbsolutePath() );
    }

    private void createGraphDbAndKillIt()
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( storeDirectory.graphDbDir() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "FOO" ) );
            db.createNode( label( "BAR" ) );
            tx.success();
        }

        fs.snapshot( shutdownDbAction( db ) );
    }

    private void runConsistencyCheckToolWith( FileSystemAbstraction fileSystem, String... args )
            throws IOException, ToolFailureException
    {
        new ConsistencyCheckTool( mock( ConsistencyCheckService.class ), fileSystem, mock( PrintStream.class ) ).run( args );
    }

    private void runConsistencyCheckToolWith( ConsistencyCheckService
            consistencyCheckService, PrintStream systemError, String... args ) throws ToolFailureException, IOException
    {
        new ConsistencyCheckTool( consistencyCheckService, new DefaultFileSystemAbstraction(), systemError ).run( args );
    }
}
