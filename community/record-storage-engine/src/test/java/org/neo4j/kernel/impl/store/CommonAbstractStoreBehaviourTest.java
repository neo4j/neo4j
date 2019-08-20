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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Test for {@link CommonAbstractStore}, but without using mocks.
 * @see CommonAbstractStoreTest for testing with mocks.
 */
@EphemeralPageCacheExtension
class CommonAbstractStoreBehaviourTest
{
    /**
     * Note that tests MUST use the non-modifying methods, to make alternate copies
     * of this settings class.
     */
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    private final Queue<Long> nextPageId = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> nextPageOffset = new ConcurrentLinkedQueue<>();
    private int cursorErrorOnRecord;
    private int intsPerRecord = 1;

    private MyStore store;
    private final Config config = Config.defaults();

    @AfterEach
    void tearDown()
    {
        if ( store != null )
        {
            store.close();
            store = null;
        }
        nextPageOffset.clear();
        nextPageId.clear();
    }

    private static void assertThrowsUnderlyingStorageException( ThrowingAction<Exception> action )
    {
        assertThrows( UnderlyingStorageException.class, action::apply );
    }

    private static void assertThrowsInvalidRecordException( ThrowingAction<Exception> action )
    {
        assertThrows( InvalidRecordException.class, action::apply );
    }

    private void verifyExceptionOnOutOfBoundsAccess( ThrowingAction<Exception> access )
    {
        prepareStoreForOutOfBoundsAccess();
        assertThrowsUnderlyingStorageException( access );
    }

    private void prepareStoreForOutOfBoundsAccess()
    {
        createStore();
        nextPageOffset.add( PAGE_SIZE - 2 );
    }

    private void verifyExceptionOnCursorError( ThrowingAction<Exception> access )
    {
        prepareStoreForCursorError();
        assertThrowsInvalidRecordException( access );
    }

    private void prepareStoreForCursorError()
    {
        createStore();
        cursorErrorOnRecord = 5;
    }

    private void createStore()
    {
        store = new MyStore( config, pageCache, 8 );
        store.initialise( true );
    }

    @Test
    void writingOfHeaderRecordDuringInitialiseNewStoreFileMustThrowOnPageOverflow()
    {
        // 16-byte header will overflow an 8-byte page size
        MyStore store = new MyStore( config, pageCache, PAGE_SIZE + 1 );
        assertThrowsUnderlyingStorageException( () -> store.initialise( true ) );
    }

    @Test
    void extractHeaderRecordDuringLoadStorageMustThrowOnPageOverflow()
    {
        MyStore first = new MyStore( config, pageCache, 8 );
        first.initialise( true );
        first.close();

        MyStore second = new MyStore( config, pageCache, PAGE_SIZE + 1 );
        assertThrowsUnderlyingStorageException( () -> second.initialise( false ) );
    }

    @Test
    void getRawRecordDataMustNotThrowOnPageOverflow() throws Exception
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRawRecordData( 5 );
    }

    @Test
    void isInUseMustThrowOnPageOverflow()
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.isInUse( 5 ) );
    }

    @Test
    void isInUseMustThrowOnCursorError()
    {
        verifyExceptionOnCursorError( () -> store.isInUse( 5 ) );
    }

    @Test
    void getRecordMustThrowOnPageOverflow()
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.getRecord( 5, new IntRecord( 5 ), NORMAL ) );
    }

    @Test
    void getRecordMustNotThrowOnPageOverflowWithCheckLoadMode()
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRecord( 5, new IntRecord( 5 ), CHECK );
    }

    @Test
    void getRecordMustNotThrowOnPageOverflowWithForceLoadMode()
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRecord( 5, new IntRecord( 5 ), FORCE );
    }

    @Test
    void updateRecordMustThrowOnPageOverflow()
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.updateRecord( new IntRecord( 5 ) ) );
    }

    @Test
    void getRecordMustThrowOnCursorError()
    {
        verifyExceptionOnCursorError( () -> store.getRecord( 5, new IntRecord( 5 ), NORMAL ) );
    }

    @Test
    void getRecordMustNotThrowOnCursorErrorWithCheckLoadMode()
    {
        prepareStoreForCursorError();
        store.getRecord( 5, new IntRecord( 5 ), CHECK );
    }

    @Test
    void getRecordMustNotThrowOnCursorErrorWithForceLoadMode()
    {
        prepareStoreForCursorError();
        store.getRecord( 5, new IntRecord( 5 ), FORCE );
    }

    @Test
    void scanForHighIdMustThrowOnPageOverflow()
    {
        createStore();
        store.setStoreNotOk( new RuntimeException() );
        IntRecord record = new IntRecord( 200 );
        record.value = 0xCAFEBABE;
        store.updateRecord( record );
        intsPerRecord = 8192;
        assertThrowsUnderlyingStorageException( () -> store.start() );
    }

    @Test
    void mustFinishInitialisationOfIncompleteStoreHeader() throws IOException
    {
        createStore();
        int headerSizeInRecords = store.getNumberOfReservedLowIds();
        int headerSizeInBytes = headerSizeInRecords * store.getRecordSize();
        try ( PageCursor cursor = store.pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next() );
            for ( int i = 0; i < headerSizeInBytes; i++ )
            {
                cursor.putByte( (byte) 0 );
            }
        }
        int pageSize = store.pagedFile.pageSize();
        store.close();
        store.pageCache.map( store.getStorageFile(), pageSize, StandardOpenOption.TRUNCATE_EXISTING ).close();
        createStore();
    }

    @Test
    void shouldProvideFreeIdsToMissingIdGenerator() throws IOException
    {
        // given
        createStore();
        store.start();
        MutableLongSet holes = LongSets.mutable.empty();
        holes.add( store.nextId() );
        holes.add( store.nextId() );
        store.updateRecord( new IntRecord( store.nextId(), 1 ) );
        holes.add( store.nextId() );
        store.updateRecord( new IntRecord( store.nextId(), 1 ) );

        // when
        store.close();
        fs.deleteFile( new File( MyStore.ID_FILENAME ) );
        createStore();
        store.start();

        // then
        int numberOfHoles = holes.size();
        for ( int i = 0; i < numberOfHoles; i++ )
        {
            assertTrue( holes.remove( store.nextId() ) );
        }
        assertTrue( holes.isEmpty() );
    }

    @Test
    void shouldOverwriteExistingIdGeneratorOnMissingStore() throws IOException
    {
        // given
        createStore();
        store.start();
        MutableLongSet holes = LongSets.mutable.empty();
        store.updateRecord( new IntRecord( store.nextId(), 1 ) );
        holes.add( store.nextId() );
        holes.add( store.nextId() );
        store.updateRecord( new IntRecord( store.nextId(), 1 ) );
        holes.add( store.nextId() );
        store.updateRecord( new IntRecord( store.nextId(), 1 ) );

        // when
        store.close();
        fs.deleteFile( new File( MyStore.STORE_FILENAME ) );
        createStore();
        store.start();

        // then
        int numberOfReservedLowIds = store.getNumberOfReservedLowIds();
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( numberOfReservedLowIds + i, store.nextId() );
        }
    }

    private static class IntRecord extends AbstractBaseRecord
    {
        public int value;

        IntRecord( long id )
        {
            this( id, 0 );
        }

        IntRecord( long id, int value )
        {
            super( id );
            setInUse( true );
            this.value = value;
        }

        @Override
        public String toString()
        {
            return "IntRecord[" + getId() + "](" + value + ")";
        }
    }

    private static class LongLongHeader implements StoreHeader
    {
    }

    private class MyFormat extends BaseRecordFormat<IntRecord> implements StoreHeaderFormat<LongLongHeader>
    {
        MyFormat( int recordHeaderSize )
        {
            super( x -> 4, recordHeaderSize, 32 );
        }

        @Override
        public IntRecord newRecord()
        {
            return new IntRecord( 0 );
        }

        @Override
        public boolean isInUse( PageCursor cursor )
        {
            int offset = cursor.getOffset();
            long pageId = cursor.getCurrentPageId();
            long recordId = (offset + pageId * cursor.getCurrentPageSize()) / 4;
            boolean inUse = false;
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                inUse |= cursor.getInt() != 0;
            }
            maybeSetCursorError( cursor, recordId );
            return inUse;
        }

        @Override
        public void read( IntRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
        {
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                record.value = cursor.getInt();
            }
            record.setInUse( true );
            maybeSetCursorError( cursor, record.getId() );
        }

        private void maybeSetCursorError( PageCursor cursor, long id )
        {
            if ( cursorErrorOnRecord == id )
            {
                cursor.setCursorException( "boom" );
            }
        }

        @Override
        public void write( IntRecord record, PageCursor cursor, int recordSize )
        {
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                cursor.putInt( record.value );
            }
        }

        @Override
        public int numberOfReservedRecords()
        {
            return 4; // 2 longs occupy 4 int records
        }

        @Override
        public LongLongHeader generateHeader()
        {
            return new LongLongHeader();
        }

        @Override
        public void writeHeader( PageCursor cursor )
        {
            for ( int i = 0; i < getRecordHeaderSize(); i++ )
            {
                cursor.putByte( (byte) ThreadLocalRandom.current().nextInt() );
            }
        }

        @Override
        public LongLongHeader readHeader( PageCursor cursor )
        {
            LongLongHeader header = new LongLongHeader();
            for ( int i = 0; i < getRecordHeaderSize(); i++ )
            {
                // pretend to read fields into the header
                cursor.getByte();
            }
            return header;
        }
    }

    private class MyStore extends CommonAbstractStore<IntRecord,LongLongHeader>
    {
        static final String STORE_FILENAME = "store";
        static final String ID_FILENAME = "idFile";

        MyStore( Config config, PageCache pageCache, int recordHeaderSize )
        {
            this( config, pageCache, new MyFormat( recordHeaderSize ) );
        }

        MyStore( Config config, PageCache pageCache, MyFormat format )
        {
            super( new File( STORE_FILENAME ), new File( ID_FILENAME ), config, IdType.NODE,
                    new DefaultIdGeneratorFactory( fs, immediate() ), pageCache,
                    NullLogProvider.getInstance(), "T", format, format, "XYZ" );
        }

        @Override
        public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, IntRecord record )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long pageIdForRecord( long id )
        {
            Long override = nextPageId.poll();
            return override != null ? override : super.pageIdForRecord( id );
        }

        @Override
        protected int offsetForId( long id )
        {
            Integer override = nextPageOffset.poll();
            return override != null ? override : super.offsetForId( id );
        }
    }
}
