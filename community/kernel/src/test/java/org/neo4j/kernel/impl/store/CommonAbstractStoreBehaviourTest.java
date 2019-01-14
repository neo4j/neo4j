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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.ConfigurablePageCacheRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Test for {@link CommonAbstractStore}, but without using mocks.
 * @see CommonAbstractStoreTest for testing with mocks.
 */
public class CommonAbstractStoreBehaviourTest
{
    /**
     * Note that tests MUST use the non-modifying methods, to make alternate copies
     * of this settings class.
     */
    private static final Config CONFIG = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8M" );

    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final ConfigurablePageCacheRule pageCacheRule = new ConfigurablePageCacheRule();

    @Rule
    public final TestRule rules = RuleChain.outerRule( fs ).around( pageCacheRule );

    private final Queue<Long> nextPageId = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> nextPageOffset = new ConcurrentLinkedQueue<>();
    private int cursorErrorOnRecord;
    private int intsPerRecord = 1;

    private MyStore store;
    private Config config = CONFIG;

    @After
    public void tearDown()
    {
        if ( store != null )
        {
            store.close();
            store = null;
        }
        nextPageOffset.clear();
        nextPageId.clear();
    }

    private void assertThrowsUnderlyingStorageException( ThrowingAction<Exception> action ) throws Exception
    {
        try
        {
            action.apply();
            fail( "expected an UnderlyingStorageException exception" );
        }
        catch ( UnderlyingStorageException exception )
        {
            // Good!
        }
    }

    private void assertThrowsInvalidRecordException( ThrowingAction<Exception> action ) throws Exception
    {
        try
        {
            action.apply();
            fail( "expected an InvalidRecordException exception" );
        }
        catch ( InvalidRecordException exception )
        {
            // Good!
        }
    }

    private void verifyExceptionOnOutOfBoundsAccess( ThrowingAction<Exception> access ) throws Exception
    {
        prepareStoreForOutOfBoundsAccess();
        assertThrowsUnderlyingStorageException( access );
    }

    private void prepareStoreForOutOfBoundsAccess()
    {
        createStore();
        nextPageOffset.add( PAGE_SIZE - 2 );
    }

    private void verifyExceptionOnCursorError( ThrowingAction<Exception> access ) throws Exception
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
        store = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ), 8 );
        store.initialise( true );
    }

    @Test
    public void writingOfHeaderRecordDuringInitialiseNewStoreFileMustThrowOnPageOverflow() throws Exception
    {
        // 16-byte header will overflow an 8-byte page size
        PageCacheRule.PageCacheConfig pageCacheConfig = PageCacheRule.config();
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), pageCacheConfig, config );
        MyStore store = new MyStore( config, pageCache, PAGE_SIZE + 1 );
        assertThrowsUnderlyingStorageException( () -> store.initialise( true ) );
    }

    @Test
    public void extractHeaderRecordDuringLoadStorageMustThrowOnPageOverflow() throws Exception
    {
        MyStore first = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ), 8 );
        first.initialise( true );
        first.close();

        PageCacheRule.PageCacheConfig pageCacheConfig = PageCacheRule.config();
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), pageCacheConfig, config );
        MyStore second = new MyStore( config, pageCache, PAGE_SIZE + 1 );
        assertThrowsUnderlyingStorageException( () -> second.initialise( false ) );
    }

    @Test
    public void getRawRecordDataMustNotThrowOnPageOverflow() throws Exception
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRawRecordData( 5 );
    }

    @Test
    public void isInUseMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.isInUse( 5 ) );
    }

    @Test
    public void isInUseMustThrowOnCursorError() throws Exception
    {
        verifyExceptionOnCursorError( () -> store.isInUse( 5 ) );
    }

    @Test
    public void getRecordMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.getRecord( 5, new IntRecord( 5 ), NORMAL ) );
    }

    @Test
    public void getRecordMustNotThrowOnPageOverflowWithCheckLoadMode()
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRecord( 5, new IntRecord( 5 ), CHECK );
    }

    @Test
    public void getRecordMustNotThrowOnPageOverflowWithForceLoadMode()
    {
        prepareStoreForOutOfBoundsAccess();
        store.getRecord( 5, new IntRecord( 5 ), FORCE );
    }

    @Test
    public void updateRecordMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnOutOfBoundsAccess( () -> store.updateRecord( new IntRecord( 5 ) ) );
    }

    @Test
    public void getRecordMustThrowOnCursorError() throws Exception
    {
        verifyExceptionOnCursorError( () -> store.getRecord( 5, new IntRecord( 5 ), NORMAL ) );
    }

    @Test
    public void getRecordMustNotThrowOnCursorErrorWithCheckLoadMode()
    {
        prepareStoreForCursorError();
        store.getRecord( 5, new IntRecord( 5 ), CHECK );
    }

    @Test
    public void getRecordMustNotThrowOnCursorErrorWithForceLoadMode()
    {
        prepareStoreForCursorError();
        store.getRecord( 5, new IntRecord( 5 ), FORCE );
    }

    @Test
    public void rebuildIdGeneratorSlowMustThrowOnPageOverflow() throws Exception
    {
        config.augment( GraphDatabaseSettings.rebuild_idgenerators_fast, "false" );
        createStore();
        store.setStoreNotOk( new RuntimeException() );
        IntRecord record = new IntRecord( 200 );
        record.value = 0xCAFEBABE;
        store.updateRecord( record );
        intsPerRecord = 8192;
        assertThrowsUnderlyingStorageException( () -> store.makeStoreOk() );
    }

    @Test
    public void scanForHighIdMustThrowOnPageOverflow() throws Exception
    {
        createStore();
        store.setStoreNotOk( new RuntimeException() );
        IntRecord record = new IntRecord( 200 );
        record.value = 0xCAFEBABE;
        store.updateRecord( record );
        intsPerRecord = 8192;
        assertThrowsUnderlyingStorageException( () -> store.makeStoreOk() );
    }

    @Test
    public void mustFinishInitialisationOfIncompleteStoreHeader() throws IOException
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

    private static class IntRecord extends AbstractBaseRecord
    {
        public int value;

        IntRecord( long id )
        {
            super( id );
            setInUse( true );
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
        MyStore( Config config, PageCache pageCache, int recordHeaderSize )
        {
            this( config, pageCache, new MyFormat( recordHeaderSize ) );
        }

        MyStore( Config config, PageCache pageCache, MyFormat format )
        {
            super( new File( "store" ), new File( "idFile" ), config, IdType.NODE,
                    new DefaultIdGeneratorFactory( fs.get() ), pageCache,
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
