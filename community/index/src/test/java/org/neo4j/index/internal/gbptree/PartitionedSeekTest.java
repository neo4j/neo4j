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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Race;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.test.Race.throwing;

@ExtendWith( {RandomExtension.class, DefaultFileSystemExtension.class, TestDirectorySupportExtension.class} )
class PartitionedSeekTest
{
    private static final int PAGE_SIZE = 512;

    @RegisterExtension
    static PageCacheSupportExtension pageCacheSupportExtension = new PageCacheSupportExtension( PageCacheConfig.config().withPageSize( PAGE_SIZE ) );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;
    @Inject
    private PageCache pageCache;
    private SimpleLongLayout layout;
    private Path treeFile;

    @BeforeEach
    void setup()
    {
        // Make keys larger with padding so they fill up tree faster, but not beyond entry limit.
        layout = SimpleLongLayout.longLayout().build();
        treeFile = testDirectory.filePath( "tree" );
    }

    @Test
    void shouldPartitionTreeWithLeafRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int to = insertEntries( tree, 0, 5, 1 );
            DepthAndRootVisitor visit = visit( tree );
            assertEquals( 1, visit.numberOfLevels );

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
        shouldPartitionTree( 2, 3, 4, 3 );
    }

    @Test
    void shouldPartitionTreeWithPreciseNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 2, 5, 5, 5 );
    }

    @Test
    void shouldPartitionTreeWithMoreNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 2, 12, 6, 6 );
    }

    @Test
    void shouldPartitionTreeOnLevel1() throws IOException
    {
        shouldPartitionTree( 3, 3, 4, 4 );
    }

    @Test
    void shouldPartitionTreeWithRandomKeysAndFindAll() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int numberOfRootChildren = random.nextInt( 1, 10 );
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt( 2, 4 );
            int numberOfDesiredPartitions = random.nextInt( 1, 10 );
            int high = insertEntriesUntil( tree, numberOfDesiredLevels, numberOfRootChildren );
            long from = random.nextLong( 0, high - 1 );
            long to = random.nextLong( from, high );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers =
                    tree.partitionedSeek( layout.key( from ), layout.key( to ), numberOfDesiredPartitions, NULL );

            // then
            IntList entryCountPerPartition = assertEntries( from, to, seekers );
            verifyEntryCountPerPartition( entryCountPerPartition );
        }
    }

    @Test
    void shouldCreateReasonablePartitionsWhenFromInclusiveMatchKeyInRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int numberOfRootChildren = random.nextInt( 1, 10 );
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt( 2, 4 );
            int high = insertEntriesUntil( tree, numberOfDesiredLevels, numberOfRootChildren );

            List<MutableLong> rootKeys = getKeysOnLevel( tree, 0 );
            int numberOfDesiredPartitions = random.nextInt( 1, rootKeys.size() );
            long from = layout.keySeed( rootKeys.get( 0 ) );
            long to = random.nextLong( from, high );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers =
                    tree.partitionedSeek( layout.key( from ), layout.key( to ), numberOfDesiredPartitions, NULL );

            // then
            IntList entryCountPerPartition = assertEntries( from, to, seekers );
            verifyEntryCountPerPartition( entryCountPerPartition );
        }
    }

    @Test
    void shouldCreateReasonablePartitionsWhenToExclusiveMatchKeyInRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int numberOfRootChildren = random.nextInt( 1, 10 );
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt( 2, 4 );
            int high = insertEntriesUntil( tree, numberOfDesiredLevels, numberOfRootChildren );

            List<MutableLong> rootKeys = getKeysOnLevel( tree, 0 );
            int numberOfDesiredPartitions = random.nextInt( 1, rootKeys.size() );
            long to = layout.keySeed( rootKeys.get( rootKeys.size() - 1 ) );
            long from = random.nextLong( 0, to );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers =
                    tree.partitionedSeek( layout.key( from ), layout.key( to ), numberOfDesiredPartitions, NULL );

            // then
            IntList entryCountPerSeeker = assertEntries( from, to, seekers );
            verifyEntryCountPerPartition( entryCountPerSeeker );
        }
    }

    @Test
    void shouldPartitionSeekersDuringTreeModifications() throws IOException
    {
        TreeNodeFixedSize<MutableLong,MutableLong> treeNode = new TreeNodeFixedSize<>( pageCache.pageSize(), layout );
        int internalMaxKeyCount = treeNode.internalMaxKeyCount();
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given a tree with half filled root
            int stride = 15;
            int high = insertEntriesUntil( tree, 2, internalMaxKeyCount / 2, stride /*holes between each key*/ );
            int count = high / stride;

            // when calling partitionedSeek while concurrently inserting
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

            // when calling partitionedSeek while concurrently removing
            // removing go something like this:
            // - initial state is a tree with keys like 0, 1, 2... high
            // - each round removes all keys on given offset modulo stride, starting from the high end, starting by removing 14, 29, 44,...
            //   and while doing so calling partitioned seek
            // there will be racing between changing the root, merging the leaves and calling partitionedSeek
            for ( int i = stride - 2; i >= 0; i-- )
            {
                int offset = i + 1;
                AtomicReference<Collection<Seeker<MutableLong,MutableLong>>> partitions = new AtomicReference<>();
                Race race = new Race();
                race.addContestant( throwing( () -> removeEntries( tree, offset, count, stride ) ) );
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
                        if ( nextExpected % stride >= offset )
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
        return new GBPTreeBuilder<>( pageCache, treeFile, layout ).build();
    }

    private void shouldPartitionTree( int numberOfDesiredLevels, int numberOfDesiredRootChildren, int numberOfDesiredPartitions,
            int expectedNumberOfPartitions ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            int to = insertEntriesUntil( tree, numberOfDesiredLevels, numberOfDesiredRootChildren );

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

    private void verifyEntryCountPerPartition( IntList entryCountPerSeeker )
    {
        // verify that partitions have some sort of fair distribution
        // First and last partition may have varying number of entries, but the middle ones should be (at least in this test case)
        // max a factor two from each other, entry-count wise
        if ( entryCountPerSeeker.size() > 1 )
        {
            int reference = entryCountPerSeeker.get( 1 );
            for ( int i = 2; i < entryCountPerSeeker.size() - 1; i++ )
            {
                int difference = abs( reference - entryCountPerSeeker.get( i ) );
                assertThat( difference ).isLessThanOrEqualTo( reference );
            }
        }
    }

    private int insertEntriesUntil( GBPTree<MutableLong,MutableLong> tree, int numberOfDesiredLevels, int numberOfDesiredRootKeys ) throws IOException
    {
        return insertEntriesUntil( tree, numberOfDesiredLevels, numberOfDesiredRootKeys, 1 );
    }

    private int insertEntriesUntil( GBPTree<MutableLong,MutableLong> tree, int numberOfDesiredLevels, int numberOfDesiredRootKeys, int stride )
            throws IOException
    {
        int id = 0;
        DepthAndRootVisitor result;
        while ( (result = visit( tree )) != null && (result.numberOfLevels < numberOfDesiredLevels || result.rootChildCount < numberOfDesiredRootKeys) )
        {
            id = insertEntries( tree, id, 10, stride );
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

    private void removeEntries( GBPTree<MutableLong,MutableLong> tree, int startId, int count, int stride ) throws IOException
    {
        int id = startId;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            for ( int i = 0; i < count; i++, id += stride )
            {
                MutableLong key = layout.key( id );
                writer.remove( key );
            }
        }
    }

    private List<MutableLong> getKeysOnLevel( GBPTree<MutableLong,MutableLong> tree, int level ) throws IOException
    {
        List<MutableLong> keysOnLevel = new ArrayList<>();
        GBPTreeVisitor.Adaptor<MutableLong,MutableLong> visitor = new GBPTreeVisitor.Adaptor<>()
        {
            private int currentLevel;

            @Override
            public void beginLevel( int level )
            {
                currentLevel = level;
            }

            @Override
            public void key( MutableLong key, boolean isLeaf, long offloadId )
            {
                if ( currentLevel == level )
                {
                    MutableLong into = layout.newKey();
                    layout.copyKey( key, into );
                    keysOnLevel.add( into );
                }
            }
        };
        tree.visit( visitor, NULL );
        return keysOnLevel;
    }

    private static DepthAndRootVisitor visit( GBPTree<MutableLong,MutableLong> tree ) throws IOException
    {
        DepthAndRootVisitor visitor = new DepthAndRootVisitor();
        tree.visit( visitor, NULL );
        return visitor;
    }

    private static class DepthAndRootVisitor extends GBPTreeVisitor.Adaptor<MutableLong,MutableLong>
    {
        private int numberOfLevels;
        private int currentLevel;
        private int rootChildCount;

        @Override
        public void beginLevel( int level )
        {
            currentLevel = level;
            numberOfLevels++;
        }

        @Override
        public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
        {
            if ( currentLevel == 0 && !isLeaf )
            {
                rootChildCount = keyCount + 1;
            }
        }
    }
}
