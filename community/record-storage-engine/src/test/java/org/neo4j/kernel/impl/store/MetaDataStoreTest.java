/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.NullLogger;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionStringToLong;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.rule.PageCacheConfig.config;

@EphemeralTestDirectoryExtension
class MetaDataStoreTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withInconsistentReads( false ) );
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private PageCache pageCache;
    private boolean fakePageCursorOverflow;
    private PageCache pageCacheWithFakeOverflow;

    @BeforeEach
    void setUp()
    {
        pageCache = pageCacheExtension.getPageCache( fs );
        fakePageCursorOverflow = false;
        pageCacheWithFakeOverflow = new DelegatingPageCache( pageCache )
        {
            @Override
            public PagedFile map( File file, VersionContextSupplier versionContextSupplier, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, versionContextSupplier, pageSize, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags ) throws IOException
                    {
                        return new DelegatingPageCursor( super.io( pageId, pf_flags ) )
                        {
                            @Override
                            public boolean checkAndClearBoundsFlag()
                            {
                                return fakePageCursorOverflow | super.checkAndClearBoundsFlag();
                            }
                        };
                    }
                };
            }
        };
    }

    @Test
    void getCreationTimeShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getCreationTime );
    }

    @Test
    void getCurrentLogVersionShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getCurrentLogVersion );
    }

    @Test
    void getLastClosedTransactionIdShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getLastClosedTransactionId );
    }

    @Test
    void getLastClosedTransactionShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getLastClosedTransaction );
    }

    @Test
    void getLastCommittedTransactionShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getLastCommittedTransaction );
    }

    @Test
    void getLastCommittedTransactionIdShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getLastCommittedTransactionId );
    }

    @Test
    void getLatestConstraintIntroducingTxShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getLatestConstraintIntroducingTx );
    }

    @Test
    void getRandomNumberShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getRandomNumber );
    }

    @Test
    void getStoreVersionShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getStoreVersion );
    }

    @Test
    void getUpgradeTimeShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getUpgradeTime );
    }

    @Test
    void getUpgradeTransactionShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::getUpgradeTransaction );
    }

    @Test
    void nextCommittingTransactionIdShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, metaDataStore::nextCommittingTransactionId );
    }

    @Test
    void currentCommittingTransactionId()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.nextCommittingTransactionId();
        long lastCommittingTxId = metaDataStore.nextCommittingTransactionId();
        assertEquals( lastCommittingTxId, metaDataStore.committingTransactionId() );

        metaDataStore.nextCommittingTransactionId();
        metaDataStore.nextCommittingTransactionId();

        lastCommittingTxId = metaDataStore.nextCommittingTransactionId();
        assertEquals( lastCommittingTxId, metaDataStore.committingTransactionId() );
        metaDataStore.close();
    }

    @Test
    void setLastCommittedAndClosedTransactionIdShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, () -> metaDataStore.setLastCommittedAndClosedTransactionId( 1, 2, BASE_TX_COMMIT_TIMESTAMP, 3, 4 ) );
    }

    @Test
    void setLastClosedTransactionFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, () -> metaDataStore.resetLastClosedTransaction( 1, 2, 3, true ) );
    }

    @Test
    void setLastClosedTransactionOverridesLastClosedTransactionInformation() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.resetLastClosedTransaction( 3, 4, 5, true );

        assertEquals( 3L, metaDataStore.getLastClosedTransactionId() );
        assertArrayEquals( new long[]{3, 4, 5}, metaDataStore.getLastClosedTransaction() );
        MetaDataRecord record = metaDataStore.getRecord( LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP.id(), new MetaDataRecord(), FORCE );
        assertThat( record.getValue(), greaterThan( 0L ) );
    }

    @Test
    void setLastClosedTransactionOverridesLastClosedTransactionInformationWithoutMissingLogsUpdate() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.resetLastClosedTransaction( 3, 4, 5, false );

        assertEquals( 3L, metaDataStore.getLastClosedTransactionId() );
        assertArrayEquals( new long[]{3, 4, 5}, metaDataStore.getLastClosedTransaction() );
        MetaDataRecord record = metaDataStore.getRecord( LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP.id(), new MetaDataRecord(), FORCE );
        assertEquals( -1, record.getValue() );
    }

    @Test
    void transactionCommittedShouldFailWhenStoreIsClosed()
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows( StoreFileClosedException.class, () -> metaDataStore.transactionCommitted( 1, 1, BASE_TX_COMMIT_TIMESTAMP ) );
    }

    @Test
    void testRecordTransactionClosed()
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
    void setUpgradeTransactionMustBeAtomic() throws Throwable
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.pagedFile;
            store.setUpgradeTransaction( 0, 0, 0 );
            AtomicLong writeCount = new AtomicLong();
            AtomicLong fileReadCount = new AtomicLong();
            AtomicLong apiReadCount = new AtomicLong();
            int upperLimit = 10_000;
            int lowerLimit = 100;
            long endTime = currentTimeMillis() + SECONDS.toMillis( 10 );

            Race race = new Race();
            race.withEndCondition( () -> writeCount.get() >= upperLimit &&
                    fileReadCount.get() >= upperLimit && apiReadCount.get() >= upperLimit );
            race.withEndCondition( () -> writeCount.get() >= lowerLimit &&
                    fileReadCount.get() >= lowerLimit && apiReadCount.get() >= lowerLimit &&
                    currentTimeMillis() >= endTime );
            // writers
            race.addContestants( 3, () ->
            {
                long count = writeCount.incrementAndGet();
                store.setUpgradeTransaction( count, count, count );
            } );

            // file readers
            race.addContestants( 3, throwing( () ->
            {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long id;
                    long checksum;
                    do
                    {
                        id = store.getRecordValue( cursor, MetaDataStore.Position.UPGRADE_TRANSACTION_ID );
                        checksum = store.getRecordValue( cursor, MetaDataStore.Position.UPGRADE_TRANSACTION_CHECKSUM );
                    }
                    while ( cursor.shouldRetry() );
                    assertIdEqualsChecksum( id, checksum, "file" );
                    fileReadCount.incrementAndGet();
                }
            } ) );

            race.addContestants( 3, () ->
            {
                TransactionId transaction = store.getUpgradeTransaction();
                assertIdEqualsChecksum( transaction.transactionId(), transaction.checksum(), "API" );
                apiReadCount.incrementAndGet();
            } );
            race.go();
        }
    }

    private static void assertIdEqualsChecksum( long id, long checksum, String source )
    {
        if ( id != checksum )
        {
            throw new AssertionError(
                    "id (" + id + ") and checksum (" + checksum + ") from " + source + " should be identical" );
        }
    }

    @Test
    void incrementAndGetVersionMustBeAtomic() throws Throwable
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            long initialVersion = store.incrementAndGetVersion();
            int threads = Runtime.getRuntime().availableProcessors();
            int iterations = 500;
            Race race = new Race();
            race.addContestants( threads, () ->
            {
                for ( int i = 0; i < iterations; i++ )
                {
                    store.incrementAndGetVersion();
                }
            } );
            race.go();
            assertThat( store.incrementAndGetVersion(), is( initialVersion + (threads * iterations) + 1 ) );
        }
    }

    @Test
    void transactionCommittedMustBeAtomic() throws Throwable
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.pagedFile;
            store.transactionCommitted( 2, 2, 2 );
            AtomicLong writeCount = new AtomicLong();
            AtomicLong fileReadCount = new AtomicLong();
            AtomicLong apiReadCount = new AtomicLong();
            int upperLimit = 10_000;
            int lowerLimit = 100;
            long endTime = currentTimeMillis() + SECONDS.toMillis( 10 );

            Race race = new Race();
            race.withEndCondition( () -> writeCount.get() >= upperLimit &&
                    fileReadCount.get() >= upperLimit && apiReadCount.get() >= upperLimit );
            race.withEndCondition( () -> writeCount.get() >= lowerLimit &&
                    fileReadCount.get() >= lowerLimit && apiReadCount.get() >= lowerLimit &&
                    currentTimeMillis() >= endTime );
            race.addContestants( 3, () ->
            {
                long count = writeCount.incrementAndGet();
                store.transactionCommitted( count, count, count );
            } );

            race.addContestants( 3, throwing( () ->
            {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long id;
                    long checksum;
                    do
                    {
                        id = store.getRecordValue( cursor, MetaDataStore.Position.LAST_TRANSACTION_ID );
                        checksum = store.getRecordValue( cursor, MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM );
                    }
                    while ( cursor.shouldRetry() );
                    assertIdEqualsChecksum( id, checksum, "file" );
                    fileReadCount.incrementAndGet();
                }
            } ) );

            race.addContestants( 3, () ->
            {
                TransactionId transaction = store.getLastCommittedTransaction();
                assertIdEqualsChecksum( transaction.transactionId(), transaction.checksum(), "API" );
                apiReadCount.incrementAndGet();
            } );

            race.go();
        }
    }

    @Test
    void transactionClosedMustBeAtomic() throws Throwable
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            PagedFile pf = store.pagedFile;
            int initialValue = 2;
            store.transactionClosed( initialValue, initialValue, initialValue );
            AtomicLong writeCount = new AtomicLong();
            AtomicLong fileReadCount = new AtomicLong();
            AtomicLong apiReadCount = new AtomicLong();
            int upperLimit = 10_000;
            int lowerLimit = 100;
            long endTime = currentTimeMillis() + SECONDS.toMillis( 10 );

            Race race = new Race();
            race.withEndCondition( () -> writeCount.get() >= upperLimit &&
                    fileReadCount.get() >= upperLimit && apiReadCount.get() >= upperLimit );
            race.withEndCondition( () -> writeCount.get() >= lowerLimit &&
                    fileReadCount.get() >= lowerLimit && apiReadCount.get() >= lowerLimit &&
                    currentTimeMillis() >= endTime );
            race.addContestants( 3, () ->
            {
                long count = writeCount.incrementAndGet();
                store.transactionCommitted( count, count, count );
            } );

            race.addContestants( 3, throwing( () ->
            {
                try ( PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    long logVersion;
                    long byteOffset;
                    do
                    {
                        logVersion = store.getRecordValue( cursor,
                                MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
                        byteOffset = store.getRecordValue( cursor,
                                MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
                    }
                    while ( cursor.shouldRetry() );
                    assertLogVersionEqualsByteOffset( logVersion, byteOffset, "file" );
                    fileReadCount.incrementAndGet();
                }
            } ) );

            race.addContestants( 3, () ->
            {
                long[] transaction = store.getLastClosedTransaction();
                assertLogVersionEqualsByteOffset( transaction[0], transaction[1], "API" );
                apiReadCount.incrementAndGet();
            } );
            race.go();
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
    void mustSupportScanningAllRecords()
    {
        MetaDataStore.Position[] positions = MetaDataStore.Position.values();
        long storeVersion = versionStringToLong( Standard.LATEST_RECORD_FORMATS.storeVersion() );
        try ( MetaDataStore store = newMetaDataStore() )
        {
            writeCorrectMetaDataRecord( store, positions, storeVersion );
            store.flush();
        }

        List<Long> actualValues = new ArrayList<>();
        try ( MetaDataStore store = newMetaDataStore() )
        {
            store.scanAllRecords( record ->
            {
                actualValues.add( record.getValue() );
                return false;
            } );
        }

        List<Long> expectedValues = Arrays.stream( positions ).map( p ->
        {
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

    private File createMetaDataFile() throws IOException
    {
        File file = testDirectory.databaseLayout().metadataStore();
        fs.write( file ).close();
        return file;
    }

    @Test
    void mustSupportScanningAllRecordsWithRecordCursor()
    {
        MetaDataStore.Position[] positions = MetaDataStore.Position.values();
        long storeVersion = versionStringToLong( Standard.LATEST_RECORD_FORMATS.storeVersion());
        try ( MetaDataStore store = newMetaDataStore() )
        {
            writeCorrectMetaDataRecord( store, positions, storeVersion );
        }

        List<Long> actualValues = new ArrayList<>();
        try ( MetaDataStore store = newMetaDataStore() )
        {
            MetaDataRecord record = store.newRecord();
            try ( PageCursor cursor = store.openPageCursorForReading( 0 ) )
            {
                long highId = store.getHighId();
                for ( long id = 0; id < highId; id++ )
                {
                    store.getRecordByCursor( id, record, RecordLoad.NORMAL, cursor );
                    if ( record.inUse() )
                    {
                        actualValues.add( record.getValue() );
                    }
                }
            }
        }

        List<Long> expectedValues = Arrays.stream( positions ).map( p ->
        {
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

    private void writeCorrectMetaDataRecord( MetaDataStore store, MetaDataStore.Position[] positions, long storeVersion )
    {
        MetaDataRecord record = store.newRecord();
        for ( MetaDataStore.Position position : positions )
        {
            record.setId( position.id() );
            if ( position == MetaDataStore.Position.STORE_VERSION )
            {
                record.initialize( true, storeVersion );
            }
            else
            {
                record.initialize( true, position.ordinal() + 1 );
            }
            store.updateRecord( record );
        }
    }

    @Test
    void staticSetRecordMustThrowOnPageOverflow()
    {
        fakePageCursorOverflow = true;
        assertThrows( UnderlyingStorageException.class,
                () -> MetaDataStore.setRecord( pageCacheWithFakeOverflow, createMetaDataFile(), MetaDataStore.Position.STORE_VERSION, 4242 ) );
    }

    @Test
    void staticGetRecordMustThrowOnPageOverflow() throws Exception
    {
        File metaDataFile = createMetaDataFile();
        MetaDataStore.setRecord(
                pageCacheWithFakeOverflow, metaDataFile, MetaDataStore.Position.STORE_VERSION, 4242 );
        fakePageCursorOverflow = true;
        assertThrows( UnderlyingStorageException.class,
                () -> MetaDataStore.getRecord( pageCacheWithFakeOverflow, metaDataFile, MetaDataStore.Position.STORE_VERSION ) );

    }

    @Test
    void incrementVersionMustThrowOnPageOverflow()
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            fakePageCursorOverflow = true;
            assertThrows( UnderlyingStorageException.class, store::incrementAndGetVersion );
            fakePageCursorOverflow = false;
        }
    }

    @Test
    void lastTxCommitTimestampShouldBeBaseInNewStore()
    {
        try ( MetaDataStore metaDataStore = newMetaDataStore() )
        {
            long timestamp = metaDataStore.getLastCommittedTransaction().commitTimestamp();
            assertThat( timestamp, equalTo( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP ) );
        }
    }

    @Test
    void readAllFieldsMustThrowOnPageOverflow()
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            // Apparently this is possible, and will trick MetaDataStore into thinking the field is not initialised.
            // Thus it will reload all fields from the file, even though this ends up being the actual value in the
            // file. We do this because creating a proper MetaDataStore automatically initialises all fields.
            store.setUpgradeTime( MetaDataStore.FIELD_NOT_INITIALIZED );
            fakePageCursorOverflow = true;
            assertThrows( UnderlyingStorageException.class, store::getUpgradeTime );
            fakePageCursorOverflow = false;
        }
    }

    @Test
    void setRecordMustThrowOnPageOverflow()
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            fakePageCursorOverflow = true;
            assertThrows( UnderlyingStorageException.class, () -> store.setUpgradeTransaction( 13, 42, 42 ) );
            fakePageCursorOverflow = false;
        }
    }

    @Test
    void logRecordsMustIgnorePageOverflow()
    {
        try ( MetaDataStore store = newMetaDataStore() )
        {
            fakePageCursorOverflow = true;
            store.logRecords( NullLogger.getInstance() );
            fakePageCursorOverflow = false;
        }
    }

    @Test
    void throwsWhenClosed()
    {
        MetaDataStore store = newMetaDataStore();

        store.close();

        assertThrows( StoreFileClosedException.class, store::getLastCommittedTransactionId );
    }

    private MetaDataStore newMetaDataStore()
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory =
                new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( fs, immediate() ),
                        pageCacheWithFakeOverflow, fs, logProvider );
        return storeFactory.openNeoStores( true, StoreType.META_DATA ).getMetaDataStore();
    }

}
