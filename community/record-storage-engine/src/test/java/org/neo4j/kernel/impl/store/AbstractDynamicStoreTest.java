/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralPageCacheExtension
class AbstractDynamicStoreTest
{
    private static final int BLOCK_SIZE = 60;

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    private final File storeFile = new File( "store" );
    private final File idFile = new File( "idStore" );
    private final RecordFormats formats = Standard.LATEST_RECORD_FORMATS;

    @BeforeEach
    void before() throws IOException
    {
        try ( StoreChannel channel = fs.write( storeFile ) )
        {
            ByteBuffer buffer = ByteBuffers.allocate( 4, INSTANCE );
            buffer.putInt( BLOCK_SIZE );
            buffer.flip();
            channel.writeAll( buffer );
        }
    }

    @Test
    void tracePageCacheAccessOnNextRecord()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnNextRecord" );
              var store = newTestableDynamicStore() )
        {
            assertZeroCursor( cursorTracer );
            prepareDirtyGenerator( store );

            store.nextRecord( cursorTracer );

            assertOneCursor( cursorTracer );
        }
    }

    @Test
    void noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTreeOnNext()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTreeOnNext" );
              var store = newTestableDynamicStore() )
        {
            assertZeroCursor( cursorTracer );

            store.nextRecord( cursorTracer );

            assertZeroCursor( cursorTracer );
        }
    }

    @Test
    void tracePageCacheAccessOnRecordsAllocation()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnRecordsAllocation" );
              var store = newTestableDynamicStore() )
        {
            assertZeroCursor( cursorTracer );
            prepareDirtyGenerator( store );

            store.allocateRecordsFromBytes( new ArrayList<>(), new byte[]{0, 1, 2, 3, 4}, cursorTracer, INSTANCE );

            assertOneCursor( cursorTracer );
        }
    }

    @Test
    void noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTree()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTree" );
              var store = newTestableDynamicStore() )
        {
            assertZeroCursor( cursorTracer );

            store.allocateRecordsFromBytes( new ArrayList<>(), new byte[]{0, 1, 2, 3, 4}, cursorTracer, INSTANCE );

            assertZeroCursor( cursorTracer );
        }
    }

    @Test
    void dynamicRecordCursorReadsInUseRecords()
    {
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            DynamicRecord first = createDynamicRecord( 1, store, 0 );
            DynamicRecord second = createDynamicRecord( 2, store, 0 );
            DynamicRecord third = createDynamicRecord( 3, store, 10 );
            store.setHighId( 3 );

            first.setNextBlock( second.getId() );
            store.updateRecord( first, NULL );
            second.setNextBlock( third.getId() );
            store.updateRecord( second, NULL );

            Iterator<DynamicRecord> records = store.getRecords( 1, NORMAL, false, NULL ).iterator();
            assertTrue( records.hasNext() );
            assertEquals( first, records.next() );
            assertTrue( records.hasNext() );
            assertEquals( second, records.next() );
            assertTrue( records.hasNext() );
            assertEquals( third, records.next() );
            assertFalse( records.hasNext() );
        }
    }

    @Test
    void dynamicRecordCursorReadsNotInUseRecords()
    {
        try ( AbstractDynamicStore store = newTestableDynamicStore() )
        {
            DynamicRecord first = createDynamicRecord( 1, store, 0 );
            DynamicRecord second = createDynamicRecord( 2, store, 0 );
            DynamicRecord third = createDynamicRecord( 3, store, 10 );
            store.setHighId( 3 );

            first.setNextBlock( second.getId() );
            store.updateRecord( first, NULL );
            second.setNextBlock( third.getId() );
            store.updateRecord( second, NULL );
            second.setInUse( false );
            store.updateRecord( second, NULL );

            Iterator<DynamicRecord> records = store.getRecords( 1, FORCE, false, NULL ).iterator();
            assertTrue( records.hasNext() );
            assertEquals( first, records.next() );
            assertTrue( records.hasNext() );
            DynamicRecord secondReadRecord = records.next();
            assertEquals( second, secondReadRecord );
            assertFalse( secondReadRecord.inUse() );
            // because mode == FORCE we can still move through the chain
            assertTrue( records.hasNext() );
            assertEquals( third, records.next() );
            assertFalse( records.hasNext() );
        }
    }

    private void prepareDirtyGenerator( AbstractDynamicStore store )
    {
        var idGenerator = store.getIdGenerator();
        var marker = idGenerator.marker( NULL );
        marker.markDeleted( 1L );
        idGenerator.clearCache( NULL );
    }

    private void assertOneCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isOne();
        assertThat( cursorTracer.pins() ).isOne();
        assertThat( cursorTracer.unpins() ).isOne();
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
    }

    private DynamicRecord createDynamicRecord( long id, AbstractDynamicStore store, int dataSize )
    {
        DynamicRecord first = new DynamicRecord( id );
        first.setInUse( true );
        first.setData( RandomUtils.nextBytes( dataSize == 0 ? BLOCK_SIZE - formats.dynamic().getRecordHeaderSize() : 10 ) );
        store.updateRecord( first, NULL );
        return first;
    }

    private AbstractDynamicStore newTestableDynamicStore()
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        AbstractDynamicStore store = new AbstractDynamicStore( storeFile, idFile, Config.defaults(), IdType.ARRAY_BLOCK,
                idGeneratorFactory, pageCache, NullLogProvider.getInstance(), "test", BLOCK_SIZE,
                formats.dynamic(), formats.storeVersion(), immutable.empty() )
        {
            @Override
            public void accept( Processor processor, DynamicRecord record, PageCursorTracer cursorTracer )
            {   // Ignore
            }

            @Override
            public String getTypeDescriptor()
            {
                return "TestDynamicStore";
            }
        };
        store.initialise( true, NULL );
        return store;
    }
}
