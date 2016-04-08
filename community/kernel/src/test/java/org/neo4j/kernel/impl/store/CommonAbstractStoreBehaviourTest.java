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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Test for {@link CommonAbstractStore}, but without using mocks.
 * @see CommonAbstractStoreTest for testing with mocks.
 */
public class CommonAbstractStoreBehaviourTest
{
    /**
     * Note that tests MUST use the non-modifying {@link Config#with(Map, Class[])} method, to make alternate copies
     * of this settings class.
     */
    private static final Config CONFIG = Config.empty().augment( stringMap(
            GraphDatabaseSettings.pagecache_memory.name(), "8M" ) );

    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final TestRule rules = RuleChain.outerRule( fs ).around( pageCacheRule );

    private final Queue<Long> nextPageId = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> nextPageOffset = new ConcurrentLinkedQueue<>();
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

    private void assertThrows( ThrowingAction<Exception> action ) throws Exception
    {
        try
        {
            action.apply();
            fail( "expected an exception" );
        }
        catch ( UnderlyingStorageException exception )
        {
            // Good!
        }
    }

    private void verifyExceptionOnAccess( ThrowingAction<Exception> access ) throws Exception
    {
        createStore();
        nextPageOffset.add( 8190 );
        assertThrows( access );
    }

    private void createStore()
    {
        store = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ) );
        store.initialise( true );
    }

    @Test
    public void writingOfHeaderRecordDuringInitialiseNewStoreFileMustThrowOnPageOverflow() throws Exception
    {
        // 16-byte header will overflow an 8-byte page size
        Config config = CONFIG.with( stringMap( GraphDatabaseSettings.mapped_memory_page_size.name(), "8" ) );
        MyStore store = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ) );
        assertThrows( () -> store.initialise( true ) );
    }

    @Test
    public void extractHeaderRecordDuringLoadStorageMustThrowOnPageOverflow() throws Exception
    {
        MyStore first = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ) );
        first.initialise( true );
        first.close();

        config = CONFIG.with( stringMap( GraphDatabaseSettings.mapped_memory_page_size.name(), "8" ) );
        MyStore second = new MyStore( config, pageCacheRule.getPageCache( fs.get(), config ) );
        assertThrows( () -> second.initialise( false ) );
    }

    @Test
    public void getRawRecordDataMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnAccess( () -> store.getRawRecordData( 5 ) );
    }

    @Test
    public void isInUseMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnAccess( () -> store.isInUse( 5 ) );
    }

    @Test
    public void getRecordMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnAccess( () -> store.getRecord( 5, new IntRecord( 5 ), NORMAL ) );
    }

    @Test
    public void updateRecordMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnAccess( () -> store.updateRecord( new IntRecord( 5 ) ) );
    }

    @Test
    public void recordCursorNextMustThrowOnPageOverflow() throws Exception
    {
        verifyExceptionOnAccess( () -> {
            try ( RecordCursor<IntRecord> cursor = store.newRecordCursor( new IntRecord( 0 ) ).acquire( 5, NORMAL ) )
            {
                cursor.next();
            }
        } );
    }

    @Test
    public void rebuildIdGeneratorSlowMustThrowOnPageOverflow() throws Exception
    {
        config = config.with( stringMap(
                CommonAbstractStore.Configuration.rebuild_idgenerators_fast.name(), "false" ) );
        createStore();
        store.setStoreNotOk( new Exception() );
        IntRecord record = new IntRecord( 200 );
        record.value = 0xCAFEBABE;
        store.updateRecord( record );
        intsPerRecord = 8192;
        assertThrows( () -> store.makeStoreOk() );
    }

    @Test
    public void scanForHighIdMustThrowOnPageOverflow() throws Exception
    {
        createStore();
        store.setStoreNotOk( new Exception() );
        IntRecord record = new IntRecord( 200 );
        record.value = 0xCAFEBABE;
        store.updateRecord( record );
        intsPerRecord = 8192;
        assertThrows( () -> store.makeStoreOk() );
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
        MyFormat()
        {
            super( (x) -> 4, 8, 32 );
        }

        @Override
        public IntRecord newRecord()
        {
            return new IntRecord( 0 );
        }

        @Override
        public boolean isInUse( PageCursor cursor )
        {
            boolean inUse = false;
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                inUse |= cursor.getInt() != 0;
            }
            return inUse;
        }

        @Override
        public void read( IntRecord record, PageCursor cursor, RecordLoad mode, int recordSize, PagedFile storeFile )
                throws IOException
        {
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                record.value = cursor.getInt();
            }
            record.setInUse( true );
        }

        @Override
        public void write( IntRecord record, PageCursor cursor, int recordSize, PagedFile storeFile ) throws IOException
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
            cursor.putLong( 0xA5A5A5_7E7E7EL );
            cursor.putLong( 0x3B3B3B_1A1A1AL );
        }

        @Override
        public LongLongHeader readHeader( PageCursor cursor )
        {
            LongLongHeader header = new LongLongHeader();
            cursor.getLong(); // pretend to read fields into the header
            cursor.getLong();
            return header;
        }
    }

    private class MyStore extends CommonAbstractStore<IntRecord,LongLongHeader>
    {
        MyStore( Config config, PageCache pageCache )
        {
            this( config, pageCache, new MyFormat() );
        }

        MyStore( Config config, PageCache pageCache, MyFormat format )
        {
            super( new File( "store" ), config, IdType.NODE, new DefaultIdGeneratorFactory( fs.get() ), pageCache,
                    NullLogProvider.getInstance(), "T", format, format, "XYZ" );
        }

        @Override
        public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, IntRecord record )
                throws FAILURE
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
