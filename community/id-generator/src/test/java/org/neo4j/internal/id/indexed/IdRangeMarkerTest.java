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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;

@PageCacheExtension
class IdRangeMarkerTest
{
    @Inject
    PageCache pageCache;

    @Inject
    TestDirectory directory;

    private final int idsPerEntry = 128;
    private final IdRangeLayout layout = new IdRangeLayout( idsPerEntry );
    private final AtomicLong highestWritternId = new AtomicLong();
    private GBPTree<IdRangeKey, IdRange> tree;

    @BeforeEach
    void instantiateTree()
    {
        this.tree = new GBPTreeBuilder<>( pageCache, directory.file( "file.id" ), layout ).build();
    }

    @AfterEach
    void close() throws IOException
    {
        tree.close();
    }

    @Test
    void shouldCreateEntryOnFirstAddition() throws IOException
    {
        // given
        ValueMerger merger = mock( ValueMerger.class );

        // when
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), merger ) )
        {
            marker.markDeleted( 0 );
        }

        // then
        verifyNoMoreInteractions( merger );
        try ( Seeker<IdRangeKey, IdRange> seek = tree.seek( new IdRangeKey( 0 ), new IdRangeKey( 1 ) ) )
        {
            assertTrue( seek.next() );
            assertEquals( 0, seek.key().getIdRangeIdx() );
        }
    }

    @Test
    void shouldMergeAdditionIntoExistingEntry() throws IOException
    {
        // given
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), mock( ValueMerger.class ) ) )
        {
            marker.markDeleted( 0 );
        }

        // when
        ValueMerger merger = realMergerMock();
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), merger ) )
        {
            marker.markDeleted( 1 );
        }

        // then
        verify( merger ).merge( any(), any(), any(), any() );
        try ( Seeker<IdRangeKey, IdRange> seek = tree.seek( new IdRangeKey( 0 ), new IdRangeKey( 1 ) ) )
        {
            assertTrue( seek.next() );
            assertEquals( 0, seek.key().getIdRangeIdx() );
            assertEquals( IdRange.IdState.DELETED, seek.value().getState( 0 ) );
            assertEquals( IdRange.IdState.DELETED, seek.value().getState( 1 ) );
            assertEquals( IdRange.IdState.USED, seek.value().getState( 2 ) );
        }
    }

    @Test
    void shouldNotCreateEntryOnFirstRemoval() throws IOException
    {
        // when
        ValueMerger merger = mock( ValueMerger.class );
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), merger ) )
        {
            marker.markUsed( 0 );
        }

        // then
        verifyNoMoreInteractions( merger );
        try ( Seeker<IdRangeKey, IdRange> seek = tree.seek( new IdRangeKey( 0 ), new IdRangeKey( Long.MAX_VALUE ) ) )
        {
            assertFalse( seek.next() );
        }
    }

    @Test
    void shouldRemoveEntryOnLastRemoval() throws IOException
    {
        // given
        int ids = 5;
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), IdRangeMerger.DEFAULT ) )
        {
            for ( long id = 0; id < ids; id++ )
            {
                // let the id go through the desired states
                marker.markUsed( id );
                marker.markDeleted( id );
                marker.markFree( id );
                marker.markReserved( id );
            }
        }
        AtomicBoolean exists = new AtomicBoolean();
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            @Override
            public void key( IdRangeKey key, boolean isLeaf )
            {
                if ( isLeaf )
                {
                    assertEquals( 0, key.getIdRangeIdx() );
                    exists.set( true );
                }
            }
        } );

        // when
        try ( IdRangeMarker marker = instantiateMarker( mock( Lock.class ), IdRangeMerger.DEFAULT ) )
        {
            for ( long id = 0; id < ids; id++ )
            {
                marker.markUsed( id );
            }
        }

        // then
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            @Override
            public void key( IdRangeKey key, boolean isLeaf )
            {
                assertFalse( isLeaf, "Should not have any key still in the tree, but got: " + key );
            }
        } );
    }

    @Test
    void shouldUnlockOnClose() throws IOException
    {
        // given
        Lock lock = mock( Lock.class );

        // when
        try ( IdRangeMarker marker = instantiateMarker( lock, mock( ValueMerger.class ) ) )
        {
            verifyNoMoreInteractions( lock );
        }

        // then
        verify( lock ).unlock();
    }

    @Test
    void shouldCloseWriterOnClose() throws IOException
    {
        // when
        Writer writer = mock( Writer.class );
        try ( IdRangeMarker marker = new IdRangeMarker( idsPerEntry, layout, writer, mock( Lock.class ), mock( ValueMerger.class ), true,
                new AtomicBoolean(), 1, new AtomicLong( 0 ), true ) )
        {
            verify( writer, never() ).close();
        }

        // then
        verify( writer ).close();
    }

    @Test
    void shouldIgnoreReservedIds() throws IOException
    {
        // given
        long reservedId = IdValidator.INTEGER_MINUS_ONE;

        // when
        MutableLongSet expectedIds = LongSets.mutable.empty();
        try ( IdRangeMarker marker = new IdRangeMarker( idsPerEntry, layout, tree.writer(), mock( Lock.class ), IdRangeMerger.DEFAULT, true,
                new AtomicBoolean(), 1, new AtomicLong( reservedId - 1 ), true ) )
        {
            for ( long id = reservedId - 1; id <= reservedId + 1; id++ )
            {
                marker.markDeleted( id );
                if ( id != reservedId )
                {
                    expectedIds.add( id );
                }
            }
        }

        // then
        MutableLongSet deletedIdsInTree = LongSets.mutable.empty();
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            private IdRangeKey idRangeKey;

            @Override
            public void key( IdRangeKey idRangeKey, boolean isLeaf )
            {
                this.idRangeKey = idRangeKey;
            }

            @Override
            public void value( IdRange idRange )
            {
                for ( int i = 0; i < idsPerEntry; i++ )
                {
                    if ( idRange.getState( i ) == DELETED )
                    {
                        deletedIdsInTree.add( idRangeKey.getIdRangeIdx() * idsPerEntry + i );
                    }
                }
            }
        } );
        assertEquals( expectedIds, deletedIdsInTree );
    }

    @Test
    void shouldBatchWriteIdBridging()
    {
        // given
        Writer<IdRangeKey,IdRange> writer = mock( Writer.class );
        try ( IdRangeMarker marker = new IdRangeMarker( idsPerEntry, layout, writer, mock( Lock.class ), IdRangeMerger.DEFAULT, true,
                new AtomicBoolean(), 1, new AtomicLong( 0 ), true ) )
        {
            // when
            marker.markDeleted( 10 );
        }

        // then
        // one time for the bridging and one time for the delete
        verify( writer, times( 2 ) ).merge( any(), any(), any() );
    }

    private ValueMerger realMergerMock()
    {
        ValueMerger merger = mock( ValueMerger.class );
        when( merger.merge( any(), any(), any(), any() ) ).thenAnswer( invocation -> IdRangeMerger.DEFAULT.merge(
                invocation.getArgument( 0 ), invocation.getArgument( 1 ), invocation.getArgument( 2 ), invocation.getArgument( 3 ) ) );
        return merger;
    }

    private IdRangeMarker instantiateMarker( Lock lock, ValueMerger merger ) throws IOException
    {
        return new IdRangeMarker( idsPerEntry, layout, tree.writer(), lock, merger, true, new AtomicBoolean(), 1, highestWritternId, true );
    }
}
