/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.neo4j.consistency.ConsistencyCheckTool.ToolFailureException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.legacy.consistency.ConsistencyCheckTool.ExitHandle;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.consistency.ConsistencyCheckTool.USE_LEGACY_CHECKER;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.ArrayUtil.concat;
import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

@RunWith( Parameterized.class )
public class ConsistencyCheckToolTest
{
    private final boolean useLegacyChecker;

    @Parameters( name = "Experimental:{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[] { Boolean.FALSE } );
        data.add( new Object[] { Boolean.TRUE } );
        return data;
    }

    public ConsistencyCheckToolTest( boolean useLegacyChecker )
    {
        this.useLegacyChecker = useLegacyChecker;
    }

    @Test
    public void runsConsistencyCheck() throws Exception
    {
        // given
        assumeFalse( "This test runs with mocked ConsistencyCheckService, doesn't work with the legacy checker " +
                "since it creates its own", useLegacyChecker );
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
        assumeFalse( "This test runs with mocked ConsistencyCheckService, doesn't work with the legacy checker " +
                "since it creates its own", useLegacyChecker );
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
        assumeFalse( "This test runs with mocked ConsistencyCheckService, doesn't work with the legacy checker " +
                "since it creates its own", useLegacyChecker );
        File storeDir = storeDirectory.directory();
        File propertyFile = storeDirectory.file( "neo4j.properties" );
        Properties properties = new Properties();
        properties.setProperty( ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        properties.store( new FileWriter( propertyFile ), null );

        String[] args = {storeDir.getPath(), "-config", propertyFile.getPath()};
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
    public void exitWithFailureIfConfigSpecifiedButPropertiesFileDoesNotExist() throws Exception
    {
        // given
        File propertyFile = storeDirectory.file( "nonexistent_file" );
        String[] args = {storeDirectory.directory().getPath(), "-config", propertyFile.getPath()};
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

        // When
        runConsistencyCheckToolWith( monitors, mock( ExitHandle.class ), fs.get(),
                "-recovery", storeDirectory.graphDbDir().getAbsolutePath() );

        // Then
        verify( listener ).recoveryRequired( any( LogPosition.class ) );
        verify( listener ).recoveryCompleted( anyInt() );
    }

    @Test
    public void shouldExitWhenRecoveryNeededButRecoveryFalseOptionSpecified() throws Exception
    {
        // Given
        File storeDir = storeDirectory.graphDbDir();
        EphemeralFileSystemAbstraction fileSystem = createDataBaseWithStateThatNeedsRecovery( storeDir );

        Monitors monitors = new Monitors();
        PhysicalLogFile.Monitor listener = mock( PhysicalLogFile.Monitor.class );
        monitors.addMonitorListener( listener );

        ExitHandle exitHandle = mock( ExitHandle.class );

        doThrow( new TransientDatabaseFailureException( "Recovery required" ) ).when( exitHandle ).pull();

        // When
        try
        {
            runConsistencyCheckToolWith( monitors, exitHandle, fileSystem,
                    "-recovery=false", storeDir.getAbsolutePath() );
            fail("Recovery should be required and exit pull should be called.");
        }
        catch ( TransientDatabaseFailureException ignored )
        {
            // expected
        }

        // Then
        verifyZeroInteractions( listener );
        verify( exitHandle ).pull();
    }

    private EphemeralFileSystemAbstraction createDataBaseWithStateThatNeedsRecovery( File storeDir )
    {
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        final GraphDatabaseService db =
                new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
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

    private void runConsistencyCheckToolWith( Monitors monitors,
            ExitHandle exitHandle, FileSystemAbstraction fileSystem, String... args )
                    throws IOException, ToolFailureException
    {
        GraphDatabaseFactory graphDbFactory = new NonEphemeralImpermanentDatabaseFactory( fs.get() ).setMonitors( monitors );

        new ConsistencyCheckTool( mock( ConsistencyCheckService.class ),
                graphDbFactory, fileSystem, mock( PrintStream.class ), exitHandle ).run( augment( args ) );
    }

    private String[] augment( String[] args )
    {
        return concat( "-" + USE_LEGACY_CHECKER + "=" + useLegacyChecker, args );
    }

    private void runConsistencyCheckToolWith ( ConsistencyCheckService
        consistencyCheckService, PrintStream systemError, String... args ) throws ToolFailureException, IOException
    {
        new ConsistencyCheckTool( consistencyCheckService, new GraphDatabaseFactory(),
                new DefaultFileSystemAbstraction(), systemError, ExitHandle.SYSTEM_EXIT )
                .run( augment( args ) );
    }

    @Rule
    public TargetDirectory.TestDirectory storeDirectory = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private static class NonEphemeralImpermanentFacadeFactory extends CommunityFacadeFactory
    {
        private final FileSystemAbstraction consistencyCheckerFileSystem;

        NonEphemeralImpermanentFacadeFactory( FileSystemAbstraction consistencyCheckerFileSystem )
        {
            this.consistencyCheckerFileSystem = consistencyCheckerFileSystem;
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                OperationalMode operationalMode )
        {
            params.put( Configuration.ephemeral.name(), "false" );
            return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade, operationalMode )
            {
                @Override
                protected FileSystemAbstraction createFileSystemAbstraction()
                {
                    return consistencyCheckerFileSystem;
                }
            };
        }
    }

    private static class NonEphemeralImpermanentDatabaseCreator implements GraphDatabaseBuilder.DatabaseCreator
    {
        private final File storeDir;
        private final TestGraphDatabaseFactoryState state;
        private final FileSystemAbstraction consistencyCheckerFileSystem;

        NonEphemeralImpermanentDatabaseCreator( File storeDir, TestGraphDatabaseFactoryState state,
                FileSystemAbstraction consistencyCheckerFileSystem )
        {
            this.storeDir = storeDir;
            this.state = state;
            this.consistencyCheckerFileSystem = consistencyCheckerFileSystem;
        }

        @Override
        public GraphDatabaseService newDatabase( Map<String,String> config )
        {
            return new ImpermanentGraphDatabase( storeDir, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ))
            {
                @Override
                protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory
                        .Dependencies dependencies )
                {
                    new NonEphemeralImpermanentFacadeFactory( consistencyCheckerFileSystem )
                            .newFacade( storeDir, params, dependencies, this );
                }
            };
        }
    }

    private static class NonEphemeralImpermanentDatabaseFactory extends TestGraphDatabaseFactory
    {

        private final FileSystemAbstraction consistencyCheckerFileSystem;

        public NonEphemeralImpermanentDatabaseFactory( FileSystemAbstraction consistencyCheckerFileSystem )
        {
            this.consistencyCheckerFileSystem = consistencyCheckerFileSystem;
        }

        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
                final TestGraphDatabaseFactoryState state )
        {
            return new NonEphemeralImpermanentDatabaseCreator( storeDir, state, consistencyCheckerFileSystem );
        }

        @Override
        public GraphDatabaseService newEmbeddedDatabase( File storeDir )
        {
            return newImpermanentDatabase( storeDir );
        }

    }
}
