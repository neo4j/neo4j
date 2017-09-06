/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.zip.ZipOutputStream;

import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DatabaseIndexIntegrationTest
{
    private static final int THREAD_NUMBER = 5;
    private static ExecutorService workers;

    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final RepeatRule repeatRule = new RepeatRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( repeatRule )
            .around( fileSystemRule );

    private final CountDownLatch raceSignal = new CountDownLatch( 1 );
    private SyncNotifierDirectoryFactory directoryFactory;
    private WritableTestDatabaseIndex luceneIndex;

    @BeforeClass
    public static void initExecutors()
    {
        workers = Executors.newFixedThreadPool( THREAD_NUMBER );
    }

    @AfterClass
    public static void shutDownExecutor()
    {
        workers.shutdownNow();
    }

    @Before
    public void setUp() throws IOException
    {
        directoryFactory = new SyncNotifierDirectoryFactory( raceSignal );
        luceneIndex = createTestLuceneIndex( directoryFactory, testDirectory.directory() );
    }

    @After
    public void tearDown()
    {
        directoryFactory.close();
    }

    @Test( timeout = 10000 )
    @RepeatRule.Repeat( times = 2 )
    public void testSaveCallCommitAndCloseFromMultipleThreads() throws Exception
    {
        generateInitialData();
        Supplier<Runnable> closeTaskSupplier = () -> createConcurrentCloseTask( raceSignal );
        List<Future<?>> closeFutures = submitTasks( closeTaskSupplier );

        for ( Future<?> closeFuture : closeFutures )
        {
            closeFuture.get();
        }

        assertFalse( luceneIndex.isOpen() );
    }

    @Test( timeout = 10000 )
    @RepeatRule.Repeat( times = 2 )
    public void saveCallCloseAndDropFromMultipleThreads() throws Exception
    {
        generateInitialData();
        Supplier<Runnable> dropTaskSupplier = () -> createConcurrentDropTask( raceSignal );
        List<Future<?>> futures = submitTasks( dropTaskSupplier );

        for ( Future<?> future : futures )
        {
            future.get();
        }

        assertFalse( luceneIndex.isOpen() );
    }

    private WritableTestDatabaseIndex createTestLuceneIndex( DirectoryFactory dirFactory, File folder ) throws IOException
    {
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage(
                dirFactory, fileSystemRule.get(), folder, false );
        WritableTestDatabaseIndex index = new WritableTestDatabaseIndex( indexStorage );
        index.create();
        index.open();
        return index;
    }

    private List<Future<?>> submitTasks( Supplier<Runnable> taskSupplier )
    {
        List<Future<?>> futures = new ArrayList<>( THREAD_NUMBER );
        futures.add( workers.submit( createMainCloseTask() ) );
        for ( int i = 0; i < THREAD_NUMBER - 1; i++ )
        {
            futures.add( workers.submit( taskSupplier.get() ) );
        }
        return futures;
    }

    private void generateInitialData() throws IOException
    {
        IndexWriter indexWriter = firstPartitionWriter();
        for ( int i = 0; i < 10; i++ )
        {
            indexWriter.addDocument( createTestDocument() );
        }
    }

    private Runnable createConcurrentDropTask( CountDownLatch dropRaceSignal )
    {
        return () ->
        {
            try
            {
                dropRaceSignal.await();
                Thread.yield();
                luceneIndex.drop();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Runnable createConcurrentCloseTask( CountDownLatch closeRaceSignal )
    {
        return () ->
        {
            try
            {
                closeRaceSignal.await();
                Thread.yield();
                luceneIndex.close();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Runnable createMainCloseTask()
    {
        return () ->
        {
            try
            {
                luceneIndex.close();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Document createTestDocument()
    {
        Document document = new Document();
        document.add( new TextField( "text", "textValue", Field.Store.YES ) );
        document.add( new LongField( "long", 1, Field.Store.YES ) );
        return document;
    }

    private IndexWriter firstPartitionWriter()
    {
        List<AbstractIndexPartition> partitions = luceneIndex.getPartitions();
        assertEquals( 1, partitions.size() );
        AbstractIndexPartition partition = partitions.get( 0 );
        return partition.getIndexWriter();
    }

    private static class WritableTestDatabaseIndex extends WritableAbstractDatabaseIndex<TestLuceneIndex>
    {
        WritableTestDatabaseIndex( PartitionedIndexStorage indexStorage )
        {
            super( new TestLuceneIndex( indexStorage,
                    new WritableIndexPartitionFactory( IndexWriterConfigs::standard ) ) );
        }
    }

    private static class TestLuceneIndex extends AbstractLuceneIndex
    {

        TestLuceneIndex( PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory )
        {
            super( indexStorage, partitionFactory );
        }
    }

    private static class SyncNotifierDirectoryFactory implements DirectoryFactory
    {
        final CountDownLatch signal;

        SyncNotifierDirectoryFactory( CountDownLatch signal )
        {
            this.signal = signal;
        }

        public Directory open( File dir, CountDownLatch signal ) throws IOException
        {
            Directory directory = open( dir );
            return new SyncNotifierDirectory( directory, signal );
        }

        @Override
        public Directory open( File dir ) throws IOException
        {
            dir.mkdirs();
            FSDirectory fsDir = FSDirectory.open( dir.toPath() );
            return new SyncNotifierDirectory( fsDir, signal );
        }

        @Override
        public void close()
        {
        }

        @Override
        public void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException
        {
        }

        private class SyncNotifierDirectory extends Directory
        {
            private final Directory delegate;
            private final CountDownLatch signal;

            SyncNotifierDirectory( Directory delegate, CountDownLatch signal )
            {
                this.delegate = delegate;
                this.signal = signal;
            }

            @Override
            public String[] listAll() throws IOException
            {
                return delegate.listAll();
            }

            @Override
            public void deleteFile( String name ) throws IOException
            {
                delegate.deleteFile( name );
            }

            @Override
            public long fileLength( String name ) throws IOException
            {
                return delegate.fileLength( name );
            }

            @Override
            public IndexOutput createOutput( String name, IOContext context ) throws IOException
            {
                return delegate.createOutput( name, context );
            }

            @Override
            public void sync( Collection<String> names ) throws IOException
            {
                // where are waiting for a specific sync during index commit process inside lucene
                // as soon as we will reach it - we will fail into sleep to give chance for concurrent close calls
                if ( names.stream().noneMatch( name -> name.startsWith( IndexFileNames.PENDING_SEGMENTS ) ) )
                {
                    try
                    {
                        signal.countDown();
                        Thread.sleep( 500 );
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( e );
                    }
                }

                delegate.sync( names );
            }

            @Override
            public void renameFile( String source, String dest ) throws IOException
            {
                delegate.renameFile( source, dest );
            }

            @Override
            public IndexInput openInput( String name, IOContext context ) throws IOException
            {
                return delegate.openInput( name, context );
            }

            @Override
            public Lock obtainLock( String name ) throws IOException
            {
                return delegate.obtainLock( name );
            }

            @Override
            public void close() throws IOException
            {
                delegate.close();
            }
        }
    }
}
