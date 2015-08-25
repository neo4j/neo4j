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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Functions;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.DummyIndexImplementation;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CountCommittedTransactionThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DefaultFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TransactionRepresentationCommitProcessIT
{

    private static final String INDEX_NAME = "index";
    private static final int TOTAL_ACTIVE_THREADS = 6;
    private static final String TEST_PROVIDER_NAME = "testProvider";

    private static ExecutorService executorService;

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public LifeRule lifeRule = new LifeRule();

    private PageCache pageCache;
    private NeoStore neoStore;
    private DefaultFileSystemAbstraction fileSystem;
    private File storeDir;

    @BeforeClass
    public static void startExecutor()
    {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor()
    {
        executorService.shutdownNow();
    }

    @Before
    public void setUp()
    {
        fileSystem = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fileSystem );
        storeDir = testDirectory.graphDbDir();
        StoreFactory storeFactory = new StoreFactory( fileSystem, storeDir, pageCache,
                NullLogProvider.getInstance(), new Monitors() );
        neoStore = storeFactory.createNeoStore();

    }

    @After
    public void tearDown()
    {
        neoStore.close();
    }

    @Test(timeout = 5000)
    public void commitDuringContinuousCheckpointing() throws Exception
    {
        // prepare
        IndexConfigStore indexStore = new IndexConfigStore( storeDir, fileSystem );
        indexStore.set( Node.class, INDEX_NAME, stringMap( IndexManager.PROVIDER, TEST_PROVIDER_NAME ) );
        LegacyIndexApplierLookup legacyIndexApplierLookup = new LegacyIndexApplierLookup.Direct( Functions.map(
                MapUtil.<String,IndexImplementation>genericMap( TEST_PROVIDER_NAME, new DummyIndexImplementation() )
        ) );


        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 1000, 100_000 );
        IdOrderingQueue legacyIndexTransactionOrdering = new SynchronizedArrayIdOrderingQueue( 20 );

        final PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, PhysicalLogFile.DEFAULT_NAME,
                fileSystem );
        final PhysicalLogFile logFile = new PhysicalLogFile( fileSystem, logFiles,
                10000, neoStore, neoStore, new Monitors().newMonitor( PhysicalLogFile.Monitor.class ),
                transactionMetadataCache );

        KernelHealth kernelHealth = mock( KernelHealth.class );

        final TransactionRepresentationStoreApplier storeApplier = createStoreApplier( neoStore, indexStore,
                legacyIndexApplierLookup, legacyIndexTransactionOrdering, kernelHealth );

        final TransactionAppender appender = createTransactionAppender( neoStore, transactionMetadataCache,
                legacyIndexTransactionOrdering, logFile, kernelHealth );

        final CheckPointerImpl checkPointer = createCheckPointer( neoStore, kernelHealth, appender );

        lifeRule.add( logFile );
        lifeRule.add( indexStore );
        lifeRule.add( appender );
        lifeRule.start();

        neoStore.rebuildCountStoreIfNeeded();

        // when
        CountDownLatch completionLatch = new CountDownLatch( TOTAL_ACTIVE_THREADS );

        InsaneCheckPointer insaneCheckPointer = new InsaneCheckPointer( checkPointer, completionLatch );
        executorService.submit( insaneCheckPointer );

        List<TransactionalWorker> workers = createTransactionWorkers( 5, neoStore, appender, storeApplier, completionLatch );
        for ( TransactionalWorker worker : workers )
        {
            executorService.submit( worker );
        }

        executorService.invokeAll( workers, 0, TimeUnit.MILLISECONDS );

        // sleep for some time
        Thread.sleep( TimeUnit.SECONDS.toMillis( 2 ) );

        insaneCheckPointer.complete();
        for ( TransactionalWorker worker : workers )
        {
            worker.complete();
        }

        // wait all threads to finish
        completionLatch.await();

        // do checkpoint to catch up latest updates
        checkPointer.forceCheckPoint();

        // verify
        assertTrue( "All legacy index commands should be applied", legacyIndexTransactionOrdering.isEmpty() );
        assertEquals( "NeoStore last closed transaction id should be equal to count store transaction id.",
                neoStore.getLastClosedTransactionId(), neoStore.getCounts().txId());
    }

    private List<TransactionalWorker> createTransactionWorkers( int numberOfWorkers, NeoStore neoStore,
            TransactionAppender appender,
            TransactionRepresentationStoreApplier storeApplier, CountDownLatch completedLatch )
    {
        List<TransactionalWorker> workers = new ArrayList<>( numberOfWorkers );
        for ( int i = 0; i < numberOfWorkers; i++ )
        {
            workers.add( new TransactionalWorker( neoStore, appender, storeApplier, completedLatch ) );
        }
        return workers;
    }

    private BatchingTransactionAppender createTransactionAppender( NeoStore neoStore,
            TransactionMetadataCache transactionMetadataCache, IdOrderingQueue legacyIndexTransactionOrdering,
            PhysicalLogFile logFile, KernelHealth kernelHealth )
    {
        return new BatchingTransactionAppender( logFile, mock( LogRotation.class ),
                transactionMetadataCache, neoStore, legacyIndexTransactionOrdering, kernelHealth );
    }

    private TransactionRepresentationStoreApplier createStoreApplier( NeoStore neoStore, IndexConfigStore indexStore,
            LegacyIndexApplierLookup legacyIndexApplierLookup, IdOrderingQueue legacyIndexTransactionOrdering,
            KernelHealth kernelHealth )
    {
        return new TransactionRepresentationStoreApplier( mock( IndexingService.class ),
                mock( Provider.class ), neoStore, mock( CacheAccessBackDoor.class ), mock( LockService.class ),
                legacyIndexApplierLookup, indexStore, kernelHealth, legacyIndexTransactionOrdering );
    }

    private CheckPointerImpl createCheckPointer( NeoStore neoStore, KernelHealth kernelHealth,
            TransactionAppender appender )
    {
        CountCommittedTransactionThreshold committedTransactionThreshold =
                new CountCommittedTransactionThreshold( 1 );
        final StoreFlusher storeFlusher = new StoreFlusher( neoStore, mock( IndexingService.class ),
                mock( LabelScanStore.class ), Iterables.<IndexImplementation>empty() );
        LogProvider logProvider = mock( LogProvider.class );
        when( logProvider.getLog( any( Class.class ) ) ).thenReturn( mock( Log.class ) );
        return new CheckPointerImpl( neoStore, committedTransactionThreshold, storeFlusher,
                mock( LogPruning.class ),
                appender, kernelHealth, logProvider, mock( CheckPointTracer.class ) );
    }

    private static class InsaneCheckPointer implements Callable<Void>
    {

        private volatile boolean completed = false;
        private final CheckPointer checkPointer;
        private CountDownLatch completedLatch;

        public InsaneCheckPointer( CheckPointer checkPointer, CountDownLatch completedLatch )
        {
            this.checkPointer = checkPointer;
            this.completedLatch = completedLatch;
        }

        @Override
        public Void call()
        {
            while ( !isCompleted() )
            {
                try
                {
                    checkPointer.forceCheckPoint();
                    Thread.sleep( 10 );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
            completedLatch.countDown();
            return null;
        }

        public boolean isCompleted()
        {
            return completed;
        }

        public void complete()
        {
            this.completed = true;
        }
    }

    private static class TransactionalWorker implements Callable<Long>
    {

        private NeoStore neoStore;
        private final TransactionAppender appender;
        private final TransactionRepresentationStoreApplier storeApplier;
        private CountDownLatch completedLatch;
        private volatile boolean completed = false;

        public TransactionalWorker( NeoStore neoStore, TransactionAppender appender,
                TransactionRepresentationStoreApplier storeApplier, CountDownLatch completedLatch )
        {
            this.neoStore = neoStore;
            this.appender = appender;
            this.storeApplier = storeApplier;
            this.completedLatch = completedLatch;
        }

        @Override
        public Long call()
        {
            long lastCommittedTransaction = 0;
            while ( !isCompleted() )
            {
                try
                {
                    TransactionRepresentationCommitProcess transactionCommit = createTransactionCommitProcess();
                    PhysicalTransactionRepresentation transactionRepresentation =
                            createPhysicalTransactionRepresentation();

                    randomSleep();

                    lastCommittedTransaction = transactionCommit.commit( transactionRepresentation, new LockGroup(),
                        CommitEvent.NULL, TransactionApplicationMode.INTERNAL );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
            completedLatch.countDown();
            return lastCommittedTransaction;
        }

        private void randomSleep() throws InterruptedException
        {
            Thread.sleep( ThreadLocalRandom.current().nextInt( 50 ) );
        }

        private PhysicalTransactionRepresentation createPhysicalTransactionRepresentation()
        {

            long nextId = neoStore.getNodeStore().nextId();
            PhysicalTransactionRepresentation transactionRepresentation =
                    new PhysicalTransactionRepresentation( CommandHelper.createListOfCommands( nextId ) );
            transactionRepresentation.setHeader( new byte[0], 0, 0, System.currentTimeMillis(),
                    neoStore.getLastCommittedTransactionId(), 0, 0 );
            return transactionRepresentation;
        }

        private TransactionRepresentationCommitProcess createTransactionCommitProcess() throws IOException
        {
            IndexUpdatesValidator updatesValidator = mock( IndexUpdatesValidator.class );
            when( updatesValidator.validate( any( TransactionRepresentation.class ), any
                    ( TransactionApplicationMode.class ) ) ).thenReturn( mock( ValidatedIndexUpdates.class ) );

            return new TransactionRepresentationCommitProcess( appender, storeApplier, updatesValidator );
        }

        public boolean isCompleted()
        {
            return completed;
        }

        public void complete()
        {
            this.completed = true;
        }

    }

    private static final class CommandHelper {

        static List<Command> createListOfCommands(long highId) {
            IndexDefineCommand indexDefineCommand = createIndexDefinedCommand();
            return Arrays.asList( indexDefineCommand, createAddNodeCommand( indexDefineCommand ),
                    createNodeCommand( highId ));
        }

        private static IndexDefineCommand createIndexDefinedCommand()
        {
            Map<String,Integer> indexNames = MapUtil.genericMap( INDEX_NAME, 0 );
            IndexDefineCommand indexDefineCommand = new IndexDefineCommand();
            indexDefineCommand.init( indexNames, Collections.<String,Integer>emptyMap() );
            return indexDefineCommand;
        }

        private static Command createAddNodeCommand( IndexDefineCommand indexDefineCommand )
        {
            IndexCommand.AddNodeCommand addNodeCommand = new IndexCommand.AddNodeCommand();
            addNodeCommand.init( indexDefineCommand.getOrAssignIndexNameId( INDEX_NAME ), 0, 0, "test" );
            return addNodeCommand;
        }

        private static Command createNodeCommand(long currentHighId)
        {
            Command.NodeCommand nodeCommand = new Command.NodeCommand();
            NodeRecord beforeRecord = new NodeRecord( currentHighId - 1);
            NodeRecord afterRecord = new NodeRecord( currentHighId );
            nodeCommand.init( beforeRecord, afterRecord );
            return nodeCommand;
        }
    }
}
