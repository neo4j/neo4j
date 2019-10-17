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
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class SizeEstimationTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;

    @Test
    void shouldEstimateSizeOnFixedSizeKeys() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> tree = new GBPTreeBuilder<>( pageCache, testDirectory.file( "tree" ),
                SimpleLongLayout.longLayout().build() ).build() )
        {
            // given
            int count = random.nextInt( 100_000, 1_000_000 );
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

            // when/then
            assertEstimateWithinMargin( tree, count, 0.1 );
        }
    }

    @Test
    void shouldEstimateSizeOnDynamicSizeKeys() throws IOException
    {
        int maxKeySize = 64;
        try ( GBPTree<RawBytes,RawBytes> tree =
                new GBPTreeBuilder<>( pageCache, testDirectory.file( "tree" ), new SimpleByteArrayLayout( maxKeySize, 0 ) ).build() )
        {
            // given
            int count = random.nextInt( 100_000, 1_000_000 );
            try ( Writer<RawBytes,RawBytes> writer = tree.writer() )
            {
                RawBytes key = new RawBytes();
                RawBytes value = new RawBytes();
                value.bytes = new byte[2];
                for ( int i = 0; i < count; i++ )
                {
                    int keySize = random.nextInt( Long.BYTES, maxKeySize );
                    key.bytes = new byte[keySize];
                    ByteBuffer buffer = ByteBuffer.wrap( key.bytes );
                    buffer.putLong( i );
                    writer.put( key, value );
                }
            }

            // when/then
            assertEstimateWithinMargin( tree, count, 0.1 );
        }
    }

    private void assertEstimateWithinMargin( GBPTree<?,?> tree, int actualCount, double margin ) throws IOException
    {
        // when
        int estimate = toIntExact( tree.estimateNumberOfEntriesInTree() );

        // then
        assertThat( abs( 1D - (double) max( actualCount, estimate ) / min( actualCount, estimate ) ), lessThan( margin ) );
    }
}
