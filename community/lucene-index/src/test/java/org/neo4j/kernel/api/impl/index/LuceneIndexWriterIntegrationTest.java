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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipOutputStream;

import org.neo4j.test.RepeatRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertFalse;

public class LuceneIndexWriterIntegrationTest
{
    private static final int THREAD_NUMBER = 5;
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    private SyncNotifierDirectoryFactory directoryFactory;
    private ExecutorService workers;

    @Before
    public void setUp()
    {
        directoryFactory = new SyncNotifierDirectoryFactory();
        workers = Executors.newFixedThreadPool( THREAD_NUMBER );
    }

    @After
    public void tearDown()
    {
        workers.shutdownNow();
        directoryFactory.close();
    }

    @Test( timeout = 10000 )
    @RepeatRule.Repeat( times = 4 )
    public void testSaveCallCommitAndCloseFromMultipleThreads() throws Exception
    {
        CountDownLatch closeRaceSignal = new CountDownLatch( 1 );
        Directory writerDirectory = directoryFactory.open( testDir.directory(), closeRaceSignal );
        LuceneIndexWriter indexWriter = IndexWriterFactories.standard().create( writerDirectory );

        generateIndexData( indexWriter );
        List<Future> closeFutures = submitCloseTasks( indexWriter, closeRaceSignal );


        for ( Future closeFuture : closeFutures )
        {
            closeFuture.get();
        }
        assertFalse( indexWriter.writer.isOpen() );
    }

    private List<Future> submitCloseTasks( LuceneIndexWriter indexWriter, CountDownLatch closeRaceSignal )
    {
        List<Future> closeFutures = new ArrayList<>( THREAD_NUMBER );
        closeFutures.add( workers.submit( createMainCloseTask( indexWriter ) ) );
        for ( int i = 0; i < THREAD_NUMBER - 1; i++ )
        {
            closeFutures.add( workers.submit( createConcurrentCloseTask( indexWriter, closeRaceSignal ) ) );
        }
        return closeFutures;
    }

    private void generateIndexData( LuceneIndexWriter indexWriter ) throws IOException
    {
        for ( int i = 0; i < 10; i++ )
        {
            indexWriter.addDocument( createTestDocument() );
        }
    }

    private Runnable createConcurrentCloseTask( LuceneIndexWriter writer, CountDownLatch closeRaceSignal )
    {
        return () -> {
            try
            {
                closeRaceSignal.await();
                writer.close();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Runnable createMainCloseTask( LuceneIndexWriter writer )
    {
        return () -> {
            try
            {
                writer.close();
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

    private static class SyncNotifierDirectoryFactory implements DirectoryFactory
    {
        public Directory open( File dir, CountDownLatch signal ) throws IOException
        {
            Directory directory = open( dir );
            return new SyncNotifierDirectory( directory, signal );
        }

        @Override
        public Directory open( File dir ) throws IOException
        {
            dir.mkdirs();
            return FSDirectory.open( dir.toPath() );
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

            public SyncNotifierDirectory( Directory delegate, CountDownLatch signal )
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
