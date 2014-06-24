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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.com.AccumulatorVisitor;
import org.neo4j.com.Response;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.DataSourceManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.com.ResourceReleaser.NO_OP;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.fs.FileUtils.getMostCanonicalFile;
import static org.neo4j.io.fs.FileUtils.relativePath;
import static org.neo4j.kernel.impl.util.Cursors.exhaust;

public class RemoteStoreCopierTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldCopyStoreAndStreamTransactionsHappeningWhileDoingSo() throws Exception
    {
        // Given
        final File originalDir = new File( testDir.directory(), "original" );
        final File copyDir = new File( testDir.directory(), "copy" );
        Config config = new Config( stringMap( store_dir.name(), copyDir.getAbsolutePath() ) );
        final GraphDatabaseAPI original = (GraphDatabaseAPI)new GraphDatabaseFactory()
                .newEmbeddedDatabase( originalDir.getAbsolutePath() );
        final DependencyResolver resolver = original.getDependencyResolver();
        final FileSystemAbstraction fileSystem = resolver.resolveDependency( FileSystemAbstraction.class );
        LogVersionRepository logVersionRepository =
                original.getDependencyResolver().resolveDependency( LogVersionRepository.class );
        RemoteStoreCopier copier = new RemoteStoreCopier( config, loadKernelExtensions(),
                new ConsoleLogger( StringLogger.SYSTEM ), fs, logVersionRepository );

        // When
        RemoteStoreCopier.StoreCopyRequester requester = spy( new RemoteStoreCopier.StoreCopyRequester()
        {
            private Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer ) throws IOException
            {
                try
                {
                    // Data that should be available in the store files
                    try ( Transaction tx = original.beginTx() )
                    {
                        original.createNode( label( "BeforeCopyBegins" ) );
                        tx.success();
                    }

                    // TODO This code is sort-of-copied from DefaultMasterImplSPI. Please dedup that
                    // <copy>
                    TransactionIdStore transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
                    long transactionIdWhenStartingCopy = transactionIdStore.getLastCommittingTransactionId();
                    NeoStoreXaDataSource dataSource =
                            resolver.resolveDependency( DataSourceManager.class ).getDataSource();
                    dataSource.forceEverything();
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
                                writer.write( relativePath( baseDir, file ), fileChannel, temporaryBuffer, file.length() > 0 );
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
                    long highTransactionId = transactionIdStore.getLastCommittingTransactionId();
                    AccumulatorVisitor<CommittedTransactionRepresentation> accumulator = new AccumulatorVisitor<>(
                            upAndIncluding( highTransactionId ) );
                    LogicalTransactionStore txStore = resolver.resolveDependency( LogicalTransactionStore.class );
                    exhaust( txStore.getCursor( transactionIdWhenStartingCopy + 1, accumulator ) );
                    return response = spy( new Response<>( null, original.storeId(), accumulator.getAccumulator(), NO_OP ) );
                }
                catch ( final Throwable t )
                {
                    t.printStackTrace();
                    throw t;
                }
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );
        copier.copyStore( requester );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir.getAbsolutePath() );

        try ( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );
            assertThat( single( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) ).getId(), equalTo( 0l ) );
            assertThat( single( globalOps.getAllNodesWithLabel( label( "AfterCopy" ) ) ).getId(), equalTo( 1l ) );
            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }

    protected Predicate<CommittedTransactionRepresentation> upAndIncluding( final long upToAndIncludingTxId )
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

    private List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }
}
