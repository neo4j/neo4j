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

import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class AbstractDynamicStoreTest
{
    private static final int BLOCK_SIZE = 60;

    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private final File fileName = new File( "store" );
    private final RecordFormats formats = Standard.LATEST_RECORD_FORMATS;
    private PageCache pageCache;
    private FileSystemAbstraction fs;

    @Before
    public void before() throws IOException
    {
        fs = fsr.get();
        pageCache = pageCacheRule.getPageCache( fsr.get() );
        try ( StoreChannel channel = fs.create( fileName ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 4 );
            buffer.putInt( BLOCK_SIZE );
            buffer.flip();
            channel.write( buffer );
        }
    }

    @Test
    public void dynamicRecordCursorReadsInUseRecords()
    {
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            DynamicRecord first = createDynamicRecord( 1, store, 0 );
            DynamicRecord second = createDynamicRecord( 2, store, 0 );
            DynamicRecord third = createDynamicRecord( 3, store, 10 );
            store.setHighId( 3 );

            first.setNextBlock( second.getId() );
            store.updateRecord( first );
            second.setNextBlock( third.getId() );
            store.updateRecord( second );

            RecordCursor<DynamicRecord> recordsCursor = store.newRecordCursor( store.newRecord() ).acquire( 1, NORMAL );
            assertTrue( recordsCursor.next() );
            assertEquals( first, recordsCursor.get() );
            assertTrue( recordsCursor.next() );
            assertEquals( second, recordsCursor.get() );
            assertTrue( recordsCursor.next() );
            assertEquals( third, recordsCursor.get() );
            assertFalse( recordsCursor.next() );
        }
    }

    @Test
    public void dynamicRecordCursorReadsNotInUseRecords()
    {
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            DynamicRecord first = createDynamicRecord( 1, store, 0 );
            DynamicRecord second = createDynamicRecord( 2, store, 0 );
            DynamicRecord third = createDynamicRecord( 3, store, 10 );
            store.setHighId( 3 );

            first.setNextBlock( second.getId() );
            store.updateRecord( first );
            second.setNextBlock( third.getId() );
            store.updateRecord( second );
            second.setInUse( false );
            store.updateRecord( second );

            RecordCursor<DynamicRecord> recordsCursor = store.newRecordCursor( store.newRecord() ).acquire( 1, FORCE );
            assertTrue( recordsCursor.next() );
            assertEquals( first, recordsCursor.get() );
            assertFalse( recordsCursor.next() );
            assertEquals( second, recordsCursor.get() );
            // because mode == FORCE we can still move through the chain
            assertTrue( recordsCursor.next() );
            assertEquals( third, recordsCursor.get() );
            assertFalse( recordsCursor.next() );
        }
    }

    private DynamicRecord createDynamicRecord( long id, AbstractDynamicStore store, int dataSize )
    {
        DynamicRecord first = new DynamicRecord( id );
        first.setInUse( true );
        first.setData( RandomUtils.nextBytes( dataSize == 0 ? BLOCK_SIZE - formats.dynamic().getRecordHeaderSize() : 10 ) );
        store.updateRecord( first );
        return first;
    }

    private AbstractDynamicStore newTestableDynamicStore()
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        AbstractDynamicStore store = new AbstractDynamicStore( fileName, Config.defaults(), IdType.ARRAY_BLOCK,
                idGeneratorFactory, pageCache, NullLogProvider.getInstance(), "test", BLOCK_SIZE,
                formats.dynamic(), formats.storeVersion() )
        {
            @Override
            public void accept( Processor processor, DynamicRecord record )
            {   // Ignore
            }

            @Override
            public String getTypeDescriptor()
            {
                return "TestDynamicStore";
            }
        };
        store.initialise( true );
        return store;
    }
}
