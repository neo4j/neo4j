/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractDynamicStoreTest
{
    private static final int BLOCK_SIZE = 60;

    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private final File fileName = new File( "store" );
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
    public void shouldRecognizeDesignatedInUseBit() throws Exception
    {
        // GIVEN
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            // WHEN
            byte otherBitsInTheInUseByte = 0;
            for ( int i = 0; i < 8; i++ )
            {
                // THEN
                assertRecognizesByteAsInUse( store, otherBitsInTheInUseByte );
                otherBitsInTheInUseByte <<= 1;
                otherBitsInTheInUseByte |= 1;
            }
        }
    }

    @Test
    public void dynamicRecordCursorReadsInUseRecords()
    {
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            DynamicRecord first = createDynamicRecord( 1, store );
            DynamicRecord second = createDynamicRecord( 2, store );
            DynamicRecord third = createDynamicRecord( 3, store );

            first.setNextBlock( second.getId() );
            store.forceUpdateRecord( first );
            second.setNextBlock( third.getId() );
            store.forceUpdateRecord( second );

            AbstractDynamicStore.DynamicRecordCursor recordsCursor = store.getRecordsCursor( 1 );
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
            DynamicRecord first = createDynamicRecord( 1, store );
            DynamicRecord second = createDynamicRecord( 2, store );
            DynamicRecord third = createDynamicRecord( 3, store );

            first.setNextBlock( second.getId() );
            store.forceUpdateRecord( first );
            second.setNextBlock( third.getId() );
            store.forceUpdateRecord( second );
            second.setInUse( false );
            store.forceUpdateRecord( second );

            AbstractDynamicStore.DynamicRecordCursor recordsCursor = store.getRecordsCursor( 1 );
            assertTrue( recordsCursor.next() );
            assertEquals( first, recordsCursor.get() );
            assertTrue( recordsCursor.next() );
            assertEquals( second, recordsCursor.get() );
            assertTrue( recordsCursor.next() );
            assertEquals( third, recordsCursor.get() );
            assertFalse( recordsCursor.next() );
        }
    }

    private static DynamicRecord createDynamicRecord( long id, AbstractDynamicStore store )
    {
        DynamicRecord first = new DynamicRecord( id );
        first.setInUse( true );
        first.setData( RandomUtils.nextBytes( 10 ) );
        store.forceUpdateRecord( first );
        return first;
    }

    private void assertRecognizesByteAsInUse( AbstractDynamicStore store, byte inUseByte )
    {
        assertTrue(  store.isInUse( (byte) (inUseByte | 0x10) ) );
        assertFalse( store.isInUse( (byte) (inUseByte & ~0x10) ) );
    }

    private AbstractDynamicStore newTestableDynamicStore()
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        AbstractDynamicStore store = new AbstractDynamicStore( fileName, new Config(), IdType.ARRAY_BLOCK,
                idGeneratorFactory, pageCache, NullLogProvider.getInstance(), BLOCK_SIZE )
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
