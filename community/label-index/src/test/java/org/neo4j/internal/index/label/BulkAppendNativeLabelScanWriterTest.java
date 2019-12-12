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
package org.neo4j.internal.index.label;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

@PageCacheExtension
class BulkAppendNativeLabelScanWriterTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    @Test
    void shouldWritePendingChangesInClose() throws IOException
    {
        // given
        TrackingWriter treeWriter = new TrackingWriter();
        try ( BulkAppendNativeLabelScanWriter writer = new BulkAppendNativeLabelScanWriter( treeWriter ) )
        {
            // when
            long nodeId = 5;
            writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

            // then
            treeWriter.verifyNoMorePuts();
        }
        treeWriter.verifyMerged( key( 1, 0 ), value( 0b100000 ) );
        treeWriter.verifyMerged( key( 2, 0 ), value( 0b100000 ) );
        treeWriter.verifyMerged( key( 3, 0 ), value( 0b100000 ) );
        treeWriter.verifyNoMorePuts();
    }

    @Test
    void shouldQueueUpdatesPerRangeAndLabelId() throws IOException
    {
        // given
        TrackingWriter treeWriter = new TrackingWriter();
        try ( BulkAppendNativeLabelScanWriter writer = new BulkAppendNativeLabelScanWriter( treeWriter ) )
        {
            // when
            long nodeId1 = 5;
            long nodeId2 = 7;
            writer.write( labelChanges( nodeId1, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );
            writer.write( labelChanges( nodeId2, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

            // then
            treeWriter.verifyNoMorePuts();

            // when
            long nodeId3 = LabelScanValue.RANGE_SIZE + 5;
            writer.write( labelChanges( nodeId3, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

            // then
            treeWriter.verifyMerged( key( 1, 0 ), value( 0b10100000 ) );
            treeWriter.verifyMerged( key( 2, 0 ), value( 0b10100000 ) );
            treeWriter.verifyMerged( key( 3, 0 ), value( 0b10100000 ) );
            treeWriter.verifyNoMorePuts();
        }
        treeWriter.verifyMerged( key( 1, 1 ), value( 0b100000 ) );
        treeWriter.verifyMerged( key( 2, 1 ), value( 0b100000 ) );
        treeWriter.verifyMerged( key( 3, 1 ), value( 0b100000 ) );
        treeWriter.verifyNoMorePuts();
    }

    @Test
    void shouldFailOnUpdatingTryingToRemoveLabelFromNode() throws IOException
    {
        // given
        TrackingWriter treeWriter = new TrackingWriter();
        try ( BulkAppendNativeLabelScanWriter writer = new BulkAppendNativeLabelScanWriter( treeWriter ) )
        {
            // when/then
            IllegalArgumentException failure =
                    assertThrows( IllegalArgumentException.class, () -> writer.write( labelChanges( 3, new long[]{1, 2}, new long[]{2, 3} ) ) );
            assertThat( failure.getMessage(), containsString( "Was expecting no labels before" ) );
        }
    }

    @Test
    void shouldMergeExistingNode() throws IOException
    {
        // given
        long nodeId = 3;
        try ( BulkAppendNativeLabelScanWriter writer = new BulkAppendNativeLabelScanWriter( new TrackingWriter() ) )
        {
            writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{2} ) );
        }

        // when
        TrackingWriter treeWriter = new TrackingWriter();
        try ( BulkAppendNativeLabelScanWriter writer = new BulkAppendNativeLabelScanWriter( treeWriter ) )
        {
            writer.write( labelChanges( 3, EMPTY_LONG_ARRAY, new long[]{3} ) );
        }

        // then
        treeWriter.verifyMerged( key( 3, 0 ), value( 0b1000 ) );
    }

    private static LabelScanKey key( int labelId, long idRange )
    {
        return new LabelScanKey( labelId, idRange );
    }

    private static LabelScanValue value( long bits )
    {
        LabelScanValue value = new LabelScanValue();
        value.bits = bits;
        return value;
    }

    private static class TrackingWriter implements Writer<LabelScanKey,LabelScanValue>
    {
        final List<Pair<LabelScanKey,LabelScanValue>> merged = new ArrayList<>();

        @Override
        public void put( LabelScanKey key, LabelScanValue value )
        {
            throw new UnsupportedOperationException();
        }

        void verifyMerged( LabelScanKey key, LabelScanValue value )
        {
            Pair<LabelScanKey,LabelScanValue> entry = merged.remove( 0 );
            assertEquals( key.idRange, entry.getKey().idRange, "Wrong id range" );
            assertEquals( key.labelId, entry.getKey().labelId, "Wrong label id" );
            assertEquals( value.bits, entry.getValue().bits, "Wrong bits" );
        }

        void verifyNoMorePuts()
        {
            assertTrue( merged.isEmpty() );
        }

        @Override
        public void merge( LabelScanKey key, LabelScanValue value, ValueMerger<LabelScanKey,LabelScanValue> valueMerger )
        {
            merged.add( Pair.of( key( key.labelId, key.idRange ), value( value.bits ) ) );
        }

        @Override
        public void mergeIfExists( LabelScanKey key, LabelScanValue value, ValueMerger<LabelScanKey,LabelScanValue> valueMerger )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public LabelScanValue remove( LabelScanKey key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
