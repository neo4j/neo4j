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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        shouldPartitionTree( 5, 4, 1 );
    }

    @Test
    void shouldPartitionTreeWithFewerNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 1_000, 4, 3 );
    }

    @Test
    void shouldPartitionTreeWithPreciseNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 1_200, 4, 4 );
    }

    @Test
    void shouldPartitionTreeWithMoreNumberOfRootKeys() throws IOException
    {
        shouldPartitionTree( 5_000, 4, 4 );
    }

    @Test
    void shouldPartitionTreeWithRandomKeys() throws IOException
    {
        int numberOfKeys = random.nextInt( 0, 100_000 );
        long from = random.nextLong( 0, numberOfKeys - 1 );
        long to = random.nextLong( from, numberOfKeys );
        shouldPartitionTree( from, to, numberOfKeys, random.nextInt( 1, 20 ), -1 );
    }

    private GBPTree<MutableLong,MutableLong> instantiateTree()
    {
        return new GBPTreeBuilder<>( pageCache, testDirectory.file( "tree" ), layout ).build();
    }

    private void shouldPartitionTree( int numberOfKeys, int numberOfPartitions, int expectedNumberOfSeekers ) throws IOException
    {
        shouldPartitionTree( 0, numberOfKeys, numberOfKeys, numberOfPartitions, expectedNumberOfSeekers );
    }

    private void shouldPartitionTree( long from, long to, int numberOfKeys, int numberOfPartitions, int expectedNumberOfSeekers ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = instantiateTree() )
        {
            // given
            insertEntries( tree, numberOfKeys );

            // when
            Collection<Seeker<MutableLong,MutableLong>> seekers = tree.partitionedSeek( new MutableLong( from ), new MutableLong( to ), numberOfPartitions );

            // then
            if ( expectedNumberOfSeekers != -1 )
            {
                assertEquals( expectedNumberOfSeekers, seekers.size() );
            }
            assertEntries( from, to, seekers );
        }
    }

    private void assertEntries( long from, long to, Collection<Seeker<MutableLong,MutableLong>> seekers ) throws IOException
    {
        long nextExpected = from;
        for ( Seeker<MutableLong,MutableLong> seeker : seekers )
        {
            while ( seeker.next() )
            {
                assertEquals( nextExpected, seeker.key().longValue() );
                nextExpected++;
            }
        }
        assertEquals( to, nextExpected );
    }

    private void insertEntries( GBPTree<MutableLong,MutableLong> tree, int count ) throws IOException
    {
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            MutableLong key = new MutableLong();
            MutableLong value = new MutableLong();
            for ( int i = 0; i < count; i++ )
            {
                key.setValue( i );
                writer.put( key, value );
            }
        }
    }
}
