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
package org.neo4j.internal.index.label;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;

@PageCacheExtension
class BulkAppendNativeTokenScanWriterTest
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
        try ( BulkAppendNativeTokenScanWriter writer = new BulkAppendNativeTokenScanWriter( treeWriter ) )
        {
            // when
            long nodeId = 5;
            writer.write( EntityTokenUpdate.tokenChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

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
        try ( BulkAppendNativeTokenScanWriter writer = new BulkAppendNativeTokenScanWriter( treeWriter ) )
        {
            // when
            long nodeId1 = 5;
            long nodeId2 = 7;
            writer.write( EntityTokenUpdate.tokenChanges( nodeId1, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );
            writer.write( EntityTokenUpdate.tokenChanges( nodeId2, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

            // then
            treeWriter.verifyNoMorePuts();

            // when
            long nodeId3 = TokenScanValue.RANGE_SIZE + 5;
            writer.write( EntityTokenUpdate.tokenChanges( nodeId3, EMPTY_LONG_ARRAY, new long[]{1, 2, 3} ) );

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
        try ( BulkAppendNativeTokenScanWriter writer = new BulkAppendNativeTokenScanWriter( treeWriter ) )
        {
            // when/then
            IllegalArgumentException failure =
                    assertThrows( IllegalArgumentException.class, () -> writer.write( tokenChanges( 3, new long[]{1, 2}, new long[]{2, 3} ) ) );
            assertThat( failure.getMessage() ).contains( "Was expecting no tokens before" );
        }
    }

    @Test
    void shouldMergeExistingNode() throws IOException
    {
        // given
        long nodeId = 3;
        try ( BulkAppendNativeTokenScanWriter writer = new BulkAppendNativeTokenScanWriter( new TrackingWriter() ) )
        {
            writer.write( EntityTokenUpdate.tokenChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{2} ) );
        }

        // when
        TrackingWriter treeWriter = new TrackingWriter();
        try ( BulkAppendNativeTokenScanWriter writer = new BulkAppendNativeTokenScanWriter( treeWriter ) )
        {
            writer.write( EntityTokenUpdate.tokenChanges( 3, EMPTY_LONG_ARRAY, new long[]{3} ) );
        }

        // then
        treeWriter.verifyMerged( key( 3, 0 ), value( 0b1000 ) );
    }

    private static TokenScanKey key( int labelId, long idRange )
    {
        return new TokenScanKey( labelId, idRange );
    }

    private static TokenScanValue value( long bits )
    {
        TokenScanValue value = new TokenScanValue();
        value.bits = bits;
        return value;
    }

    private static class TrackingWriter implements Writer<TokenScanKey,TokenScanValue>
    {
        final List<Pair<TokenScanKey,TokenScanValue>> merged = new ArrayList<>();

        @Override
        public void put( TokenScanKey key, TokenScanValue value )
        {
            throw new UnsupportedOperationException();
        }

        void verifyMerged( TokenScanKey key, TokenScanValue value )
        {
            Pair<TokenScanKey,TokenScanValue> entry = merged.remove( 0 );
            assertEquals( key.idRange, entry.getKey().idRange, "Wrong id range" );
            assertEquals( key.tokenId, entry.getKey().tokenId, "Wrong label id" );
            assertEquals( value.bits, entry.getValue().bits, "Wrong bits" );
        }

        void verifyNoMorePuts()
        {
            assertTrue( merged.isEmpty() );
        }

        @Override
        public void merge( TokenScanKey key, TokenScanValue value, ValueMerger<TokenScanKey,TokenScanValue> valueMerger )
        {
            merged.add( Pair.of( key( key.tokenId, key.idRange ), value( value.bits ) ) );
        }

        @Override
        public void mergeIfExists( TokenScanKey key, TokenScanValue value, ValueMerger<TokenScanKey,TokenScanValue> valueMerger )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TokenScanValue remove( TokenScanKey key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
