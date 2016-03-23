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
package org.neo4j.kernel.impl.store;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV3_0;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionStringToLong;

public class MetaDataStoreTest
{
    private static final File STORE_DIR = new File( "store" );
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( false );

    private EphemeralFileSystemAbstraction fs;
    private PageCache pageCache;

    @AfterClass
    public static void shutDownExecutor()
    {
        executor.shutdown();
    }

    @Before
    public void setUp()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    private MetaDataStore newMetaDataStore() throws IOException
    {
        StoreFactory storeFactory = new StoreFactory( fs, STORE_DIR, pageCache, LowLimitV3_0.RECORD_FORMATS,
                NullLogProvider.getInstance() );
        return storeFactory.openNeoStores( true, StoreType.META_DATA ).getMetaDataStore();
    }

    @Test
    public void getCreationTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getCreationTime();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getCurrentLogVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getCurrentLogVersion();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getGraphNextPropShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getGraphNextProp();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastClosedTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastCommittedTransaction();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastCommittedTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLatestConstraintIntroducingTxShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLatestConstraintIntroducingTx();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getRandomNumberShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getRandomNumber();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getStoreVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getStoreVersion();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getUpgradeTime();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getUpgradeTransaction();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void nextCommittingTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.nextCommittingTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void setLastCommittedAndClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.setLastCommittedAndClosedTransactionId( 1, 1, 1, 1 );
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void transactionCommittedShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.transactionCommitted( 1, 1 );
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void testRecordTransactionClosed() throws Exception
    {
        // GIVEN
        MetaDataStore metaDataStore = newMetaDataStore();
        long[] originalClosedTransaction = metaDataStore.getLastClosedTransaction();
        long transactionId = originalClosedTransaction[0] + 1;
        long version = 1L;
        long byteOffset = 777L;

        // WHEN
        metaDataStore.transactionClosed( transactionId, version, byteOffset );
        // long[] with the highest offered gap-free number and its meta data.
        long[] closedTransactionFlags = metaDataStore.getLastClosedTransaction();

        //EXPECT
        assertEquals( version, closedTransactionFlags[1] );
        assertEquals( byteOffset, closedTransactionFlags[2] );

        // WHEN
        metaDataStore.close();
        metaDataStore = newMetaDataStore();

        // EXPECT
        long[] lastClosedTransactionFlags = metaDataStore.getLastClosedTransaction();
        assertEquals( version, lastClosedTransactionFlags[1] );
        assertEquals( byteOffset, lastClosedTransactionFlags[2] );

        metaDataStore.close();
    }

    @Test
    public void setUpgradeTransactionMustBeAtomic() throws Exception
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.storeFile;
            store.setUpgradeTransaction( 0, 0 );
            CountDownLatch runLatch = new CountDownLatch( 1 );
            AtomicBoolean stopped = new AtomicBoolean();
            AtomicLong counter = new AtomicLong();

            Runnable writer = untilStopped( stopped, runLatch, () -> {
                long count = counter.incrementAndGet();
                store.setUpgradeTransaction( count, count );
            } );

            Runnable fileReader = untilStopped( stopped, runLatch, () -> {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long id, checksum;
                    do
                    {
                        id = store.getRecordValue( cursor, MetaDataStore.Position.UPGRADE_TRANSACTION_ID );
                        checksum = store.getRecordValue( cursor, MetaDataStore.Position.UPGRADE_TRANSACTION_CHECKSUM );
                    }
                    while ( cursor.shouldRetry() );
                    assertIdEqualsChecksum( id, checksum, "file" );
                }
            } );

            Runnable apiReader = untilStopped( stopped, runLatch, () -> {
                TransactionId transaction = store.getUpgradeTransaction();
                assertIdEqualsChecksum( transaction.transactionId(), transaction.checksum(), "API" );
            } );

            forkMultiple( 10, writer );
            List<Future<?>> readerFutures = forkMultiple( 5, fileReader );
            readerFutures.addAll( forkMultiple( 5, apiReader ) );

            runLatch.await( 1, TimeUnit.SECONDS );
            stopped.set( true );

            for ( Future<?> future : readerFutures )
            {
                future.get(); // We assert that this does not throw
            }
        }
    }

    private static Runnable untilStopped(
            AtomicBoolean stopped, CountDownLatch runLatch, ThrowingAction<? extends Exception> runnable )
    {
        return () -> {
            try
            {
                while ( !stopped.get() )
                {
                    runnable.apply();
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                runLatch.countDown();
            }
        };
    }

    private static void assertIdEqualsChecksum( long id, long checksum, String source )
    {
        if ( id != checksum )
        {
            throw new AssertionError(
                    "id (" + id + ") and checksum (" + checksum + ") from " + source + " should be identical" );
        }
    }

    private static List<Future<?>> forkMultiple( int forks, Runnable runnable )
    {
        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 0; i < forks; i++ )
        {
            futures.add( executor.submit( runnable ) );
        }
        return futures;
    }

    @Test
    public void incrementAndGetVersionMustBeAtomic() throws Exception
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            long initialVersion = store.incrementAndGetVersion();
            int threads = 10, iterations = 2_000;
            Semaphore startLatch = new Semaphore( 0 );
            Runnable incrementer = () -> {
                startLatch.acquireUninterruptibly();
                for ( int i = 0; i < iterations; i++ )
                {
                    store.incrementAndGetVersion();
                }
            };
            List<Future<?>> futures = forkMultiple( threads, incrementer );
            startLatch.release( threads );
            for ( Future<?> future : futures )
            {
                future.get();
            }
            assertThat( store.incrementAndGetVersion(), is( initialVersion + (threads * iterations) + 1 ) );
        }
    }

    @Test
    public void transactionCommittedMustBeAtomic() throws Exception
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.storeFile;
            store.transactionCommitted( 2, 2 );
            CountDownLatch runLatch = new CountDownLatch( 1 );
            AtomicBoolean stopped = new AtomicBoolean();
            AtomicLong counter = new AtomicLong( 2 );

            Runnable writer = untilStopped( stopped, runLatch, () -> {
                long count = counter.incrementAndGet();
                store.transactionCommitted( count, count );
            } );

            Runnable fileReader = untilStopped( stopped, runLatch, () -> {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long id, checksum;
                    do
                    {
                        id = store.getRecordValue( cursor, MetaDataStore.Position.LAST_TRANSACTION_ID );
                        checksum = store.getRecordValue( cursor, MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM );
                    }
                    while ( cursor.shouldRetry() );
                    assertIdEqualsChecksum( id, checksum, "file" );
                }
            } );

            Runnable apiReader = untilStopped( stopped, runLatch, () -> {
                TransactionId transaction = store.getLastCommittedTransaction();
                assertIdEqualsChecksum( transaction.transactionId(), transaction.checksum(), "API" );
            } );

            forkMultiple( 10, writer );
            List<Future<?>> readerFutures = forkMultiple( 5, fileReader );
            readerFutures.addAll( forkMultiple( 5, apiReader ) );

            runLatch.await( 1, TimeUnit.SECONDS );
            stopped.set( true );

            for ( Future<?> future : readerFutures )
            {
                future.get(); // We assert that this does not throw
            }
        }
    }

    @Test
    public void transactionClosedMustBeAtomic() throws Exception
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.storeFile;
            int initialValue = 2;
            store.transactionClosed( initialValue, initialValue, initialValue );
            CountDownLatch runLatch = new CountDownLatch( 1 );
            AtomicBoolean stopped = new AtomicBoolean();
            AtomicLong counter = new AtomicLong( initialValue );

            Runnable writer = untilStopped( stopped, runLatch, () -> {
                long count = counter.incrementAndGet();
                store.transactionCommitted( count, count );
            } );

            Runnable fileReader = untilStopped( stopped, runLatch, () -> {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long logVersion, byteOffset;
                    do
                    {
                        logVersion = store.getRecordValue( cursor,
                                MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
                        byteOffset = store.getRecordValue( cursor,
                                MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
                    }
                    while ( cursor.shouldRetry() );
                    assertLogVersionEqualsByteOffset( logVersion, byteOffset, "file" );
                }
            } );

            Runnable apiReader = untilStopped( stopped, runLatch, () -> {
                long[] transaction = store.getLastClosedTransaction();
                assertLogVersionEqualsByteOffset( transaction[0], transaction[1], "API" );
            } );

            forkMultiple( 0, writer );
            List<Future<?>> readerFutures = forkMultiple( 5, fileReader );
            readerFutures.addAll( forkMultiple( 5, apiReader ) );

            runLatch.await( 1, TimeUnit.SECONDS );
            stopped.set( true );

            for ( Future<?> future : readerFutures )
            {
                future.get(); // We assert that this does not throw
            }
        }
    }

    private static void assertLogVersionEqualsByteOffset( long logVersion, long byteOffset, String source )
    {
        if ( logVersion != byteOffset )
        {
            throw new AssertionError(
                    "logVersion (" + logVersion + ") and byteOffset (" + byteOffset + ") from " + source +
                    " should be identical" );
        }
    }

    @Test
    public void mustSupportScanningAllRecords() throws Exception
    {
        File file = new File( STORE_DIR, MetaDataStore.DEFAULT_NAME );
        fs.mkdir( STORE_DIR );
        fs.create( file ).close();
        MetaDataStore.Position[] positions = MetaDataStore.Position.values();
        long storeVersion = versionStringToLong( LowLimitV3_0.RECORD_FORMATS.storeVersion());
        writeCorrectMetaDataRecord( file, positions, storeVersion );

        List<Long> actualValues = new ArrayList<>();
        try ( MetaDataStore store = newMetaDataStore() )
        {
            store.scanAllRecords( record -> {
                actualValues.add( record.getValue() );
                return false;
            } );
        }

        List<Long> expectedValues = Arrays.stream( positions ).map( p -> {
            if ( p == MetaDataStore.Position.STORE_VERSION )
            {
                return storeVersion;
            }
            else
            {
                return p.ordinal() + 1L;
            }
        } ).collect( Collectors.toList() );

        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void mustSupportScanningAllRecordsWithRecordCursor() throws Exception
    {
        File file = new File( STORE_DIR, MetaDataStore.DEFAULT_NAME );
        fs.mkdir( STORE_DIR );
        fs.create( file ).close();
        MetaDataStore.Position[] positions = MetaDataStore.Position.values();
        long storeVersion = versionStringToLong( LowLimitV3_0.RECORD_FORMATS.storeVersion());
        writeCorrectMetaDataRecord( file, positions, storeVersion );

        List<Long> actualValues = new ArrayList<>();
        try ( MetaDataStore store = newMetaDataStore() )
        {
            MetaDataRecord record = store.newRecord();
            try ( RecordCursor<MetaDataRecord> cursor = store.newRecordCursor( record ) )
            {
                cursor.acquire( 0, RecordLoad.NORMAL );
                long highId = store.getHighId();
                for ( long id = 0; id < highId; id++ )
                {
                    if ( cursor.next( id ) )
                    {
                        actualValues.add( record.getValue() );
                    }
                }
            }
        }

        List<Long> expectedValues = Arrays.stream( positions ).map( p -> {
            if ( p == MetaDataStore.Position.STORE_VERSION )
            {
                return storeVersion;
            }
            else
            {
                return p.ordinal() + 1L;
            }
        } ).collect( Collectors.toList() );


        assertThat( actualValues, is( expectedValues ) );
    }

    private void writeCorrectMetaDataRecord( File file, MetaDataStore.Position[] positions, long storeVersion )
            throws IOException
    {
        for ( MetaDataStore.Position position : positions )
        {
            if ( position == MetaDataStore.Position.STORE_VERSION )
            {
                MetaDataStore.setRecord( pageCache, file, position, storeVersion );
            }
            else
            {
                MetaDataStore.setRecord( pageCache, file, position, position.ordinal() + 1 );
            }
        }
    }
}
