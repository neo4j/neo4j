/**
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
package org.neo4j.com.storecopy;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.TransactionStream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.StoreCopyMonitor;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RemoteStoreCopierTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldStoreLogFilesAndRunRecovery() throws Exception
    {
        // Given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();

        Config config = new Config( stringMap( store_dir.name(), copyDir ) );
        ConsoleLogger consoleLog = new ConsoleLogger( StringLogger.SYSTEM );
        Logging logging = new DevNullLoggingService();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( StoreCopyMonitor.NONE );

        RemoteStoreCopier copier = new RemoteStoreCopier( config, loadKernelExtensions(), consoleLog, logging, fs,
                monitors );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( originalDir );


        // When
        RemoteStoreCopier.StoreCopyRequester requester = spy( new RemoteStoreCopier.StoreCopyRequester()
        {
            public Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                // Data that should be available in the store files
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "BeforeCopyBegins" ) );
                    tx.success();
                }

                XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                        XaDataSourceManager.class );
                RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                        dsManager,
                        original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                        StringLogger.SYSTEM, false, writer, fs,
                        original.getDependencyResolver().resolveDependency( Monitors.class ).newMonitor(
                                StoreCopyMonitor.class )
                );

                // Data that should be made available as part of recovery
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "AfterCopy" ) );
                    tx.success();
                }

                response = spy( ServerUtil.packResponse( original.storeId(), dsManager, ctx, null, ServerUtil.ALL ) );
                return response;
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );
        copier.copyStore( requester, CancellationRequest.NONE );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );
            assertThat( Iterables.single( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0l ) );
            assertThat( Iterables.single( globalOps.getAllNodesWithLabel( label( "AfterCopy" ) ) ).getId(),
                    equalTo( 1l ) );
            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void shouldNotCloseAppendersOfProvidedLoggingOnFinish() throws Exception
    {
        // Given
        String dir = new File( testDir.directory(), "dir" ).getAbsolutePath();

        Config config = new Config( stringMap( store_dir.name(), dir ) );
        ConsoleLogger console = new ConsoleLogger( StringLogger.SYSTEM );

        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null );

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( StoreCopyMonitor.NONE );

        RemoteStoreCopier copier = spy( new RemoteStoreCopier( config, loadKernelExtensions(), console, logging, fs, monitors
                ) );
        when( copier.logConfigFileName() ).thenReturn( "neo4j-logback.xml" );

        Response response = mock( Response.class );
        when( response.transactions() ).thenReturn( TransactionStream.EMPTY ).getMock();
        RemoteStoreCopier.StoreCopyRequester requester = mock( RemoteStoreCopier.StoreCopyRequester.class );
        when( requester.copyStore( any( StoreWriter.class ) ) ).thenReturn( response );

        // When
        copier.copyStore( requester, CancellationRequest.NONE );

        // Then
        LoggerContext context = ReflectionUtil.getPrivateField( logging, "loggerContext", LoggerContext.class );
        List<Appender<ILoggingEvent>> appenders = asList( context.getLogger( "org.neo4j" ).iteratorForAppenders() );
        assertThat( appenders, not( empty() ) );
    }

    private List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }

    @Test
    public void shouldCopyStoreFilesAcrossIfACancellationRequestHappensAfterTheTempStoreHasBeenRecovered()
            throws IOException
    {
        // given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();

        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null );
        ConsoleLogger console = new ConsoleLogger( StringLogger.SYSTEM );



        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        CancellationRequest cancellationRequest = new CancellationRequest()
        {
            @Override
            public boolean cancellationRequested()
            {
                return cancelStoreCopy.get();
            }
        };

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new StoreCopyMonitor.Adaptor()
        {
            @Override
            public void recoveredStore()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        } );

        RemoteStoreCopier copier =
                new RemoteStoreCopier( config, loadKernelExtensions(), console, logging, fs,
                        monitors
                );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        RemoteStoreCopier.StoreCopyRequester requester = spy( new RemoteStoreCopier.StoreCopyRequester()
        {
            public Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                        XaDataSourceManager.class );
                RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                        dsManager,
                        original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                        StringLogger.SYSTEM, false, writer, fs,
                        original.getDependencyResolver().resolveDependency( Monitors.class ).newMonitor(
                                StoreCopyMonitor.class )
                );

                response = spy( ServerUtil.packResponse( original.storeId(), dsManager, ctx, null, ServerUtil.ALL ) );
                return response;
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );


        // when
        copier.copyStore( requester, cancellationRequest );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );

            long nodesCount = Iterables.count( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 1l ) );

            assertThat( Iterables.single( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0l ) );

            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }

    @Test
    public void shouldEndUpWithAnEmptyStoreIfCancellationRequestIssuedJustBeforeRecoveryTakesPlace()
            throws IOException
    {
        // given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();
        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );

        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null );


        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        CancellationRequest cancellationRequest = new CancellationRequest()
        {
            @Override
            public boolean cancellationRequested()
            {
                return cancelStoreCopy.get();
            }
        };

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new StoreCopyMonitor.Adaptor()
        {
            @Override
            public void finishedCopyingStoreFiles()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        } );


        RemoteStoreCopier copier =
                new RemoteStoreCopier( config, loadKernelExtensions(), new ConsoleLogger( StringLogger.SYSTEM ),
                        logging,
                        fs,
                        monitors );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        RemoteStoreCopier.StoreCopyRequester requester = spy( new RemoteStoreCopier.StoreCopyRequester()
        {
            public Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                        XaDataSourceManager.class );
                RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                        dsManager,
                        original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                        StringLogger.SYSTEM, false, writer, fs,
                        original.getDependencyResolver().resolveDependency( Monitors.class ).newMonitor(
                                StoreCopyMonitor.class )
                );

                response = spy( ServerUtil.packResponse( original.storeId(), dsManager, ctx, null, ServerUtil.ALL ) );
                return response;
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );


        // when
        copier.copyStore( requester, cancellationRequest );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );

            long nodesCount = Iterables.count( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 0l ) );
            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }

}
