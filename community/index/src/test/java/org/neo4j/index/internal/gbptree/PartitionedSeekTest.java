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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.test.Race.throwing;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class PartitionedSeekTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;

    private SimpleLongLayout layout = SimpleLongLayout.longLayout().build();

    @Test
    void shouldPartitionTreeWithLeafRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int to = insertEntries( tree, 0, 5, 1 );
            assertEquals( 0, depthOf( tree ) );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers = tree.partitionedSeek( layout.key( 0 ), layout.key( to ), 4, NULL );

            // then
            assertEquals( 1, seekers.size() );
            assertEntries( 0, 5, seekers );
        }
    }

    @Test
    void shouldPartitionTreeWithFewerNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 3, 4, 3 );
    }

    @Test
    void shouldPartitionTreeWithPreciseNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 5, 5, 5 );
    }

    @Test
    void shouldPartitionTreeWithMoreNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 12, 6, 6 );
    }

    @Test
    void shouldPartitionTreeWithRandomKeysAndFindAll() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int numberOfRootChildren = random.nextInt( 10, 20 );
            int high = insertEntriesUntil( tree, numberOfRootChildren );
            long from = random.nextLong( 0, high - 1 );
            long to = random.nextLong( from, high );
            int numberOfDesiredPartitions = random.nextInt( 1, numberOfRootChildren );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers =
                    tree.partitionedSeek( layout.key( from ), layout.key( to ), numberOfDesiredPartitions, NULL );

            // then
            IntList counts = assertEntries( from, to, seekers );
            // verify that partitions have some sort of fair distribution
            // First and last partition may have varying number of entries, but the middle ones should be (at least in this test case)
            // max a factor two from each other, entry-count wise
            if ( counts.size() > 1 )
            {
                int reference = counts.get( 1 );
                for ( int i = 2; i < counts.size() - 1; i++ )
                {
                    assertTrue( abs( reference - counts.get( i ) ) <= reference );
                }
            }
        }
    }

    @Test
    void shouldPartitionSeekersDuringTreeModifications() throws IOException
    {
        TreeNodeFixedSize<MutableLong,MutableLong> treeNode = new TreeNodeFixedSize<>( pageCache.pageSize(), layout );
        int internalMaxKeyCount = treeNode.internalMaxKeyCount();
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given a tree with root with 10 children in it
            int stride = 15;
            int high = insertEntriesUntil( tree, internalMaxKeyCount / 2, stride /*holes between each key*/ );
            int count = high / stride;

            // when calling partitionedSeek while concurrently modifying
            // modifications go something like this:
            // - initial state is a tree with keys like 0, 15 (stride), 30, 45, 60... a.s.o.
            // - each round creates all keys+1 and while doing so calling partitioned seek
            // there will be racing between changing the root, even splitting the root, and calling partitionedSeek
            MutableLong min = layout.key( 0 );
            MutableLong max = layout.key( Long.MAX_VALUE );
            for ( int i = 0; i < stride - 1; i++ )
            {
                int offset = i + 1;
                AtomicReference<Collection<Seeker<MutableLong,MutableLong>>> partitions = new AtomicReference<>();
                Race race = new Race();
                race.addContestant( throwing( () -> insertEntries( tree, offset, count, stride ) ) );
                race.addContestant( throwing( () -> partitions.set( tree.partitionedSeek( min, max, random.nextInt( 2, 20 ), NULL ) ) ) );
                race.goUnchecked();

                // then
                long nextExpected = 0;
                for ( Seeker<MutableLong,MutableLong> seeker : partitions.get() )
                {
                    while ( seeker.next() )
                    {
                        assertEquals( nextExpected, seeker.key().longValue() );
                        nextExpected++;
                        if ( nextExpected % stride > offset )
                        {
                            nextExpected += stride - nextExpected % stride;
                        }
                    }
                }
                assertEquals( high, nextExpected );
            }
        }
    }

    @Test
    void shouldThrowOnAttemptBackwardPartitionedSeek() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            assertThrows( IllegalArgumentException.class, () -> tree.partitionedSeek( layout.key( 10 ), layout.key( 0 ), 5, NULL ) );
        }
    }

    private GBPTree<MutableLong,MutableLong> instantiateTree()
    {
        return new GBPTreeBuilder<>( pageCache, testDirectory.file( "tree" ), layout ).build();
    }

    private void shouldPartitionTree( int numberOfDesiredRootChildren, int numberOfDesiredPartitions, int expectedNumberOfPartitions ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int to = insertEntriesUntil( tree, numberOfDesiredRootChildren );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers =
                    tree.partitionedSeek( layout.key( 0 ), layout.key( to ), numberOfDesiredPartitions, NULL );

            // then
            assertEquals( expectedNumberOfPartitions, seekers.size() );
            assertEntries( 0, to, seekers );
        }
    }

    private IntList assertEntries( long from, long to, Collection<Seeker<MutableLong,MutableLong>> seekers ) throws IOException
    {
        long nextExpected = from;
        MutableIntList entryCountPerSeeker = IntLists.mutable.empty();
        for ( Seeker<MutableLong,MutableLong> seeker : seekers )
        {
            int count = 0;
            while ( nextExpected < to && seeker.next() )
            {
                assertEquals( nextExpected, seeker.key().longValue() );
                nextExpected++;
                count++;
            }
            entryCountPerSeeker.add( count );
        }
        assertEquals( to, nextExpected );
        return entryCountPerSeeker;
    }

    private int insertEntriesUntil( GBPTree<MutableLong,MutableLong> tree, int numberOfDesiredRootKeys ) throws IOException
    {
        return insertEntriesUntil( tree, numberOfDesiredRootKeys, 1 );
    }

    private int insertEntriesUntil( GBPTree<MutableLong,MutableLong> tree, int numberOfDesiredRootKeys, int stride ) throws IOException
    {
        int id = 0;
        while ( numberOfRootChildren( tree ) < numberOfDesiredRootKeys )
        {
            id = insertEntries( tree, id, 100, stride );
        }
        return id;
    }

    private int insertEntries( GBPTree<MutableLong,MutableLong> tree, int startId, int count, int stride ) throws IOException
    {
        int id = startId;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            MutableLong value = layout.value( 0 );
            for ( int i = 0; i < count; i++, id += stride )
            {
                MutableLong key = layout.key( id );
                writer.put( key, value );
            }
        }
        return id;
    }

    private int numberOfRootChildren( GBPTree<MutableLong,MutableLong> tree ) throws IOException
    {
        MutableInt rootChildCount = new MutableInt();
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            private int level;

            @Override
            public void beginLevel( int level )
            {
                this.level = level;
            }

            @Override
            public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
            {
                // The first call is for the root
                if ( level == 0 && !isLeaf )
                {
                    rootChildCount.setValue( keyCount + 1 );
                }
            }
        }, NULL );
        return rootChildCount.getValue();
    }

    private int depthOf( GBPTree<MutableLong,MutableLong> tree ) throws IOException
    {
        MutableInt highestLevel = new MutableInt();
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            @Override
            public void beginLevel( int level )
            {
                highestLevel.setValue( Integer.max( highestLevel.getValue(), level ) );
            }
        }, NULL );
        return highestLevel.getValue();
    }
}
