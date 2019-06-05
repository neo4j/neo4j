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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class MergingBlockEntryReaderTest
{
    @Inject
    protected RandomRule rnd;

    private static final SimpleLongLayout layout = SimpleLongLayout.longLayout().build();
    private static final Comparator<BlockEntry<MutableLong,MutableLong>> blockEntryComparator = ( b1, b2 ) -> layout.compare( b1.key(), b2.key() );

    @Test
    void shouldMergeSingleReader() throws IOException
    {
        // given
        MergingBlockEntryReader<MutableLong,MutableLong> merger = new MergingBlockEntryReader<>( layout );
        List<BlockEntry<MutableLong,MutableLong>> data = someBlockEntries( new HashSet<>() );

        // when
        merger.addSource( newReader( data ) );

        // then
        List<BlockEntry<MutableLong,MutableLong>> expected = sortAll( singleton( data ) );
        verifyMerged( expected, merger );
    }

    @Test
    void shouldMergeSingleEmptyReader() throws IOException
    {
        // given
        MergingBlockEntryReader<MutableLong,MutableLong> merger = new MergingBlockEntryReader<>( layout );
        List<BlockEntry<MutableLong,MutableLong>> data = emptyList();

        // when
        merger.addSource( newReader( data ) );

        // then
        assertFalse( merger.next() );
    }

    @Test
    void shouldMergeMultipleReaders() throws IOException
    {
        // given
        MergingBlockEntryReader<MutableLong,MutableLong> merger = new MergingBlockEntryReader<>( layout );
        List<List<BlockEntry<MutableLong,MutableLong>>> datas = new ArrayList<>();
        Set<MutableLong> uniqueKeys = new HashSet<>();
        int nbrOfReaders = rnd.nextInt( 10 ) + 1;
        for ( int i = 0; i < nbrOfReaders; i++ )
        {
            // when
            List<BlockEntry<MutableLong,MutableLong>> data = someBlockEntries( uniqueKeys );
            datas.add( data );
            merger.addSource( newReader( data ) );
        }

        // then
        List<BlockEntry<MutableLong,MutableLong>> expected = sortAll( datas );
        verifyMerged( expected, merger );
    }

    @Test
    void shouldCloseAllReaderEvenEmpty() throws IOException
    {
        // given
        MergingBlockEntryReader<MutableLong,MutableLong> merger = new MergingBlockEntryReader<>( layout );
        CloseTrackingBlockEntryCursor empty = newReader( emptyList() );
        CloseTrackingBlockEntryCursor nonEmpty = newReader( someBlockEntries( new HashSet<>() ) );
        merger.addSource( empty );
        merger.addSource( nonEmpty );

        // when
        merger.close();

        // then
        assertTrue( empty.closed );
        assertTrue( nonEmpty.closed );
    }

    @Test
    void shouldCloseAllReaderEvenEmptyAndExhausted() throws IOException
    {
        // given
        MergingBlockEntryReader<MutableLong,MutableLong> merger = new MergingBlockEntryReader<>( layout );
        CloseTrackingBlockEntryCursor empty = newReader( emptyList() );
        CloseTrackingBlockEntryCursor nonEmpty = newReader( someBlockEntries( new HashSet<>() ) );
        merger.addSource( empty );
        merger.addSource( nonEmpty );

        // when
        while ( merger.next() )
        {   // exhaust
        }
        merger.close();

        // then
        assertTrue( empty.closed );
        assertTrue( nonEmpty.closed );
    }

    private static void verifyMerged( List<BlockEntry<MutableLong,MutableLong>> expected, MergingBlockEntryReader<MutableLong,MutableLong> merger )
            throws IOException
    {
        for ( BlockEntry<MutableLong,MutableLong> expectedEntry : expected )
        {
            assertTrue( merger.next() );
            assertEquals( 0, layout.compare( expectedEntry.key(), merger.key() ) );
            assertEquals( expectedEntry.value(), merger.value() );
        }
        assertFalse( merger.next() );
    }

    private static List<BlockEntry<MutableLong,MutableLong>> sortAll( Iterable<List<BlockEntry<MutableLong,MutableLong>>> data )
    {
        List<BlockEntry<MutableLong,MutableLong>> result = new ArrayList<>();
        for ( List<BlockEntry<MutableLong,MutableLong>> list : data )
        {
            result.addAll( list );
        }
        result.sort( blockEntryComparator );
        return result;
    }

    private static CloseTrackingBlockEntryCursor newReader( List<BlockEntry<MutableLong,MutableLong>> expected )
    {
        return new CloseTrackingBlockEntryCursor( expected );
    }

    private List<BlockEntry<MutableLong,MutableLong>> someBlockEntries( Set<MutableLong> uniqueKeys )
    {
        List<BlockEntry<MutableLong,MutableLong>> entries = new ArrayList<>();
        int size = rnd.nextInt( 10 );
        for ( int i = 0; i < size; i++ )
        {
            MutableLong key;
            do
            {
                key = layout.key( rnd.nextLong( 10_000 ) );
            }
            while ( !uniqueKeys.add( key ) );
            MutableLong value = layout.value( rnd.nextLong( 10_000 ) );
            entries.add( new BlockEntry<>( key, value ) );
        }
        entries.sort( blockEntryComparator );
        return entries;
    }

    private static class CloseTrackingBlockEntryCursor extends ListBasedBlockEntryCursor<MutableLong,MutableLong>
    {
        private boolean closed;

        CloseTrackingBlockEntryCursor( Iterable<BlockEntry<MutableLong,MutableLong>> blockEntries )
        {
            super( blockEntries );
        }

        @Override
        public void close()
        {
            super.close();
            closed = true;
        }
    }
}
