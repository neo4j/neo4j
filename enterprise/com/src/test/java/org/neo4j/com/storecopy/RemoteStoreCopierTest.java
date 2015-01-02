/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TargetDirectory;

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
import static org.neo4j.com.ResourceReleaser.NO_OP;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.fs.FileUtils.getMostCanonicalFile;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class RemoteStoreCopierTest
{
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private static List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }

    @Test
    public void shouldCopyStoreAndStreamTransactionsHappeningWhileDoingSo() throws Exception
    {
        // Given
        final File originalDir = new File( testDir.directory(), "original" );
        final File copyDir = new File( testDir.directory(), "copy" );
        Config config = new Config( stringMap( store_dir.name(), copyDir.getAbsolutePath() ) );
        final GraphDatabaseAPI original = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabase( originalDir.getAbsolutePath() );
        final DependencyResolver resolver = original.getDependencyResolver();
        final FileSystemAbstraction fileSystem = resolver.resolveDependency( FileSystemAbstraction.class );
        Logging logging = new DevNullLoggingService();
        StoreCopyClient copier = new StoreCopyClient( config, loadKernelExtensions(),
                new ConsoleLogger( StringLogger.SYSTEM ), logging, fs );

        // When
        StoreCopyClient.StoreCopyRequester requester = spy( new StoreCopyClient.StoreCopyRequester()
        {
            private Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer ) throws IOException
            {
                // Data that should be available in the store files
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "BeforeCopyBegins" ) );
                    tx.success();
                }

                // TODO This code is sort-of-copied from DefaultMasterImplSPI. Please dedup that
                // <copy>
                final TransactionIdStore transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
                final long transactionIdWhenStartingCopy = transactionIdStore.getLastCommittedTransactionId();
                NeoStoreDataSource dataSource =
                        resolver.resolveDependency( DataSourceManager.class ).getDataSource();
                resolver.resolveDependency( LogRotationControl.class ).forceEverything();
                File baseDir = getMostCanonicalFile( originalDir );
                ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( 1024 * 1024 );

                // Copy the store files
                try ( ResourceIterator<File> files = dataSource.listStoreFiles() )
                {
                    while ( files.hasNext() )
                    {
                        File file = files.next();
                        try ( StoreChannel fileChannel = fileSystem.open( file, "r" ) )
                        {
                            writer.write( relativePath( baseDir, file ), fileChannel, temporaryBuffer,
                                    file.length() > 0 );
                        }
                    }
                }
                // </copy>

                // Data that should be made available as part of recovery
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "AfterCopy" ) );
                    tx.success();
                }

                // Stream committed transaction since the start of copying
                TransactionStream transactions = new TransactionStream()
                {
                    @Override
                    public void accept( Visitor<CommittedTransactionRepresentation,IOException> visitor )
                            throws IOException
                    {
//                        long highTransactionId = transactionIdStore.getLastCommittedTransactionId();
                        LogicalTransactionStore txStore = resolver.resolveDependency( LogicalTransactionStore.class );
                        try ( IOCursor<CommittedTransactionRepresentation> cursor = txStore
                                .getTransactions( transactionIdWhenStartingCopy + 1 ) )
                        {
                            while ( cursor.next() && !visitor.visit( cursor.get() ) )
                            {
                                ;
                            }
                        }
                    }
                };
                return response =
                        spy( new TransactionStreamResponse<>( null, original.storeId(), transactions, NO_OP ) );
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );
        copier.copyStore( requester, CancellationRequest.NEVER_CANCELLED );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir.getAbsolutePath() );

        try ( Transaction tx = copy.beginTx() )
        {
            assertThat( single( copy.findNodes( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0l ) );
            assertThat( single( copy.findNodes( label( "AfterCopy" ) ) ).getId(), equalTo( 1l ) );
            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }

    protected Predicate<CommittedTransactionRepresentation> upToAndIncluding( final long upToAndIncludingTxId )
    {
        return new Predicate<CommittedTransactionRepresentation>()
        {
            @Override
            public boolean accept( CommittedTransactionRepresentation transaction )
            {
                return transaction.getCommitEntry().getTxId() <= upToAndIncludingTxId;
            }
        };
    }

    @Test
    @SuppressWarnings( "rawtypes" )
    public void shouldNotCloseAppendersOfProvidedLoggingOnFinish() throws Exception
    {
        // Given
        String dir = new File( testDir.directory(), "dir" ).getAbsolutePath();

        Config config = new Config( stringMap( store_dir.name(), dir ) );
        ConsoleLogger console = new ConsoleLogger( StringLogger.SYSTEM );

        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null, new Monitors() );

        StoreCopyClient copier = spy( new StoreCopyClient( config, loadKernelExtensions(), console, logging, fs ) );
        when( copier.logConfigFileName() ).thenReturn( "neo4j-logback.xml" );

        Response response = mock( Response.class );
        StoreCopyClient.StoreCopyRequester requester = mock( StoreCopyClient.StoreCopyRequester.class );
        when( requester.copyStore( any( StoreWriter.class ) ) ).thenReturn( response );

        // When
        copier.copyStore( requester, CancellationRequest.NEVER_CANCELLED );

        // Then
        LoggerContext context = ReflectionUtil.getPrivateField( logging, "loggerContext", LoggerContext.class );
        List<Appender<ILoggingEvent>> appenders = asList( context.getLogger( "org.neo4j" ).iteratorForAppenders() );
        assertThat( appenders, not( empty() ) );
    }
}
