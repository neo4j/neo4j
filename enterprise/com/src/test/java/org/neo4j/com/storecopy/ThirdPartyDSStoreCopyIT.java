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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.DirectLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.ResourceIterators;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.monitoring.StoreCopyMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;

/**
 * This is a somewhat tricky test, and one I am not super proud of, because it involves mocking some hairy parts of
 * neo. That said, it felt important to assert that this behavior is correct.
 *
 * The background here is that, for backups and HA COPY_STORE, we need to run recovery. Because of trickyness around
 * log rotations happening while we stream store files and so on, the transactions needed for recovery are provided
 * to the client as a stream of raw transactions, not as files. Recovery, however, needs an active logical log file
 * in the filesystem to get triggered. As such, we need to be able to create these active log files for arbitrary
 * data sources.
 *
 * This test ensures that you can provide a third-party data source that is able to safely participate in copy store
 * and recovery operations.
 */
public class ThirdPartyDSStoreCopyIT
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldStoreLogFilesAndRunRecovery() throws Exception
    {
        // Given
        final String copyDir = new File(testDir.directory(), "copy").getAbsolutePath();
        final String originalDir = new File(testDir.directory(), "original").getAbsolutePath();

        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        ConsoleLogger consoleLog = new ConsoleLogger( StringLogger.DEV_NULL );
        TestLogging logging = new TestLogging();
        RemoteStoreCopier copier = new RemoteStoreCopier( config, loadKernelExtensions(), consoleLog, logging, fs, new Monitors() );


        // When
        copier.copyStore( new RemoteStoreCopier.StoreCopyRequester()
        {
            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                GraphDatabaseAPI original = (GraphDatabaseAPI)new GraphDatabaseFactory()
                        .addKernelExtension( new MyDSExtension.Factory( new File( testDir.directory(), "irrelephant.log" ) ) )
                        .newEmbeddedDatabase( originalDir );
                try
                {
                    XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                            XaDataSourceManager.class );
                    RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                            dsManager,
                            original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                            StringLogger.SYSTEM, false, writer, fs,
                            original.getDependencyResolver().resolveDependency( Monitors.class ).newMonitor( StoreCopyMonitor.class ) );

                    return ServerUtil.packResponse( original.storeId(), dsManager, ctx, null, ServerUtil.ALL );
                } finally
                {
                    original.shutdown();
                }
            }

            @Override
            public void done()
            {

            }
        }, CancellationRequest.NONE );

        // Then the resulting file should contain the data we expect.
        FileChannel activeLog = FileChannel.open( generatedLogFile().toPath(), READ );
        ByteBuffer buffer = ByteBuffer.allocate( 512 );
        activeLog.read( buffer );
        activeLog.close();
        buffer.flip();
        assertThat( buffer.getLong(), equalTo(1338l));
        assertThat( buffer.getLong(), equalTo(1339l));
    }

    private List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        kernelExtensions.add( new MyDSExtension.Factory( generatedLogFile() ) );
        return kernelExtensions;
    }

    private File generatedLogFile()
    {
        return new File(testDir.directory(), "generated.log");
    }

    public static class MyDataSource extends XaDataSource
    {
        private final File logWriterTarget;

        public MyDataSource( File logWriterTarget )
        {
            super( "my-ds".getBytes(), "my-ds" );
            this.logWriterTarget = logWriterTarget;
        }

        @Override
        public long rotateLogicalLog() throws IOException
        {
            return 1337l;
        }

        @Override
        public long getLastCommittedTxId()
        {
            return 1339l;
        }

        @Override
        public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
        {
            assert startTxId == 1338l : startTxId;
            assert endTxIdHint == 1339l : endTxIdHint;

            // We need to mock these implementation components, since LogExtractor is not an interface.
            LogExtractor.LogLoader logLoader = mock( LogExtractor.LogLoader.class );
            when(logLoader.getHighestLogVersion()).thenReturn( 1l );
            when(logLoader.getLogicalLogOrMyselfCommitted( anyLong(), anyLong())).thenReturn( mock( ReadableByteChannel.class) );

            LogExtractor.LogPositionCache logPositionCache = mock( LogExtractor.LogPositionCache.class );
            when(logPositionCache.getHeader( anyLong() )).thenReturn( 1337l );
            when(logPositionCache.positionOf( 1338l )).thenReturn( new LogExtractor.TxPosition( 1, -1, 1, 0, 0 ) );
            when(logPositionCache.positionOf( 1339l )).thenReturn( new LogExtractor.TxPosition( 1, -1, 1, 10, 0 ) );

            return new LogExtractor( logPositionCache, logLoader, null, null, null, startTxId, endTxIdHint ){

                long txCounter = 1338l;

                @Override
                public long extractNext( LogBuffer target ) throws IOException
                {
                    if(txCounter <= 1339)
                    {
                        // This doesn't really matter, what we're interested in is putting something in the log
                        // that we can then verify in the test. This ensures the correct line of data flow is implemented,
                        // what the actual log format for this made up data source is makes no difference.
                        target.putLong( txCounter );
                    }
                    return txCounter <= 1339 ? txCounter++ : -1;
                }
            };
        }

        @Override
        public XaConnection getXaConnection()
        {
            return null;
        }

        @Override
        public ResourceIterator<File> listStoreFiles() throws IOException
        {
            return ResourceIterators.emptyResourceIterator( File.class );
        }

        @Override
        public LogBufferFactory createLogBufferFactory()
        {
            return new LogBufferFactory()
            {
                @Override
                public LogBuffer createActiveLogFile( Config config, long prevCommittedId ) throws IllegalStateException, IOException
                {
                    FileChannel channel = FileChannel.open( logWriterTarget.toPath(), CREATE, READ, WRITE );
                    return new DirectLogBuffer( new StoreFileChannel( channel ), ByteBuffer.allocate(512) );
                }
            };
        }
    }

    public static class MyDSExtension implements Lifecycle
    {
        public static class Factory extends KernelExtensionFactory<Factory.Dependencies>
        {
            private final File logWriterTarget;

            public interface Dependencies
            {
                XaDataSourceManager getXaDataSourceManager();
            }

            public Factory( File logWriterTarget )
            {
                super( "my-ds" );
                this.logWriterTarget = logWriterTarget;
            }

            @Override
            public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
            {
                return new MyDSExtension( dependencies.getXaDataSourceManager(), logWriterTarget );
            }
        }

        private final XaDataSourceManager xaDataSourceManager;
        private final File logWriterTarget;

        public MyDSExtension( XaDataSourceManager xaDataSourceManager, File logWriterTarget )
        {
            this.xaDataSourceManager = xaDataSourceManager;
            this.logWriterTarget = logWriterTarget;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
            xaDataSourceManager.registerDataSource( new MyDataSource(logWriterTarget) );
        }

        @Override
        public void stop() throws Throwable
        {
        }

        @Override
        public void shutdown() throws Throwable
        {
        }
    }

}
