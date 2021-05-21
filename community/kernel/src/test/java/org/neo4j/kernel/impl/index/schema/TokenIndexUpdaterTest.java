/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Random;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Integer.max;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;
import static org.neo4j.kernel.impl.index.schema.TokenScanValueIterator.NO_ID;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class TokenIndexUpdaterTest
{
    private static final int LABEL_COUNT = 5;
    private static final int NODE_COUNT = 10_000;

    @Inject
    private RandomRule random;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    private GBPTree<TokenScanKey,TokenScanValue> tree;

    @BeforeEach
    void openTree()
    {
        tree = new GBPTreeBuilder<>( pageCache, directory.file( "file" ), new TokenScanLayout() ).build();
    }

    @AfterEach
    void closeTree() throws IOException
    {
        tree.close();
    }

    @Test
    void shouldAddAndRemoveLabels() throws Exception
    {
        // GIVEN
        long[] expected = new long[NODE_COUNT];
        try ( TokenIndexUpdater writer = new TokenIndexUpdater( max( 5, NODE_COUNT / 100 ), TokenIndex.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );

            // WHEN
            for ( int i = 0; i < NODE_COUNT * 3; i++ )
            {
                TokenIndexEntryUpdate<?> update = randomUpdate( expected );
                writer.process( update );
            }
        }

        // THEN
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] expectedNodeIds = nodesWithLabel( expected, i );
            long[] actualNodeIds = asArray( new TokenScanValueIterator(
                    tree.seek( new TokenScanKey( i, 0 ), new TokenScanKey( i, Long.MAX_VALUE ), NULL ), NO_ID ) );
            assertArrayEquals( expectedNodeIds, actualNodeIds, "For label " + i );
        }
    }

    @Test
    void shouldNotAcceptUnsortedLabels()
    {
        // GIVEN
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () ->
        {
            try ( TokenIndexUpdater writer = new TokenIndexUpdater( 1, TokenIndex.EMPTY ) )
            {
                writer.initialize( tree.writer( NULL ) );

                // WHEN
                writer.process( TokenIndexEntryUpdate.change( 0, null, EMPTY_LONG_ARRAY, new long[]{2, 1} ) );
                // we can't do the usual "fail( blabla )" here since the actual write will happen
                // when closing this writer, i.e. in the curly bracket below.
            }
        } );
        assertTrue( exception.getMessage().contains( "unsorted" ) );
    }

    @Test
    void shouldRemoveEmptyTreeEntries() throws Exception
    {
        // given
        int numberOfTreeEntries = 3;
        int numberOfNodesInEach = 5;
        int labelId = 1;
        long[] labels = {labelId};
        try ( TokenIndexUpdater writer = new TokenIndexUpdater( max( 5, NODE_COUNT / 100 ), TokenIndex.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );

            // a couple of tree entries with a couple of nodes each
            // concept art: [xxxx          ][xxxx          ][xxxx          ] where x is used node.
            for ( int i = 0; i < numberOfTreeEntries; i++ )
            {
                long baseNodeId = i * RANGE_SIZE;
                for ( int j = 0; j < numberOfNodesInEach; j++ )
                {
                    writer.process( TokenIndexEntryUpdate.change( baseNodeId + j, null, EMPTY_LONG_ARRAY, labels ) );
                }
            }
        }
        assertTreeHasKeysRepresentingIdRanges( setOfRange( 0, numberOfTreeEntries ) );

        // when removing all the nodes from one of the tree nodes
        int treeEntryToRemoveFrom = 1;
        try ( TokenIndexUpdater writer = new TokenIndexUpdater( max( 5, NODE_COUNT / 100 ), TokenIndex.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );
            long baseNodeId = treeEntryToRemoveFrom * RANGE_SIZE;
            for ( int i = 0; i < numberOfNodesInEach; i++ )
            {
                writer.process( TokenIndexEntryUpdate.change( baseNodeId + i, null, labels, EMPTY_LONG_ARRAY ) );
            }
        }

        // then
        MutableLongSet expected = setOfRange( 0, numberOfTreeEntries );
        expected.remove( treeEntryToRemoveFrom );
        assertTreeHasKeysRepresentingIdRanges( expected );
    }

    private TokenIndexEntryUpdate<?> randomUpdate( long[] expected )
    {
        int nodeId = random.nextInt( expected.length );
        long labels = expected[nodeId];
        long[] before = getLabels( labels );
        int changeCount = random.nextInt( 4 ) + 1;
        for ( int i = 0; i < changeCount; i++ )
        {
            labels = flipRandom( labels, LABEL_COUNT, random.random() );
        }
        expected[nodeId] = labels;
        return TokenIndexEntryUpdate.change( nodeId, null, before, getLabels( labels ) );
    }

    private void assertTreeHasKeysRepresentingIdRanges( MutableLongSet expected ) throws IOException
    {
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            @Override
            public void key( TokenScanKey tokenScanKey, boolean isLeaf, long offloadId )
            {
                if ( isLeaf )
                {
                    assertTrue( expected.remove( tokenScanKey.idRange ) );
                }
            }
        }, NULL );
        assertTrue( expected.isEmpty() );
    }

    private static MutableLongSet setOfRange( long from, long to )
    {
        MutableLongSet set = LongSets.mutable.empty();
        for ( long i = from; i < to; i++ )
        {
            set.add( i );
        }
        return set;
    }

    static long[] nodesWithLabel( long[] expected, int labelId )
    {
        int mask = 1 << labelId;
        int count = 0;
        for ( long labels : expected )
        {
            if ( (labels & mask) != 0 )
            {
                count++;
            }
        }

        long[] result = new long[count];
        int cursor = 0;
        for ( int nodeId = 0; nodeId < expected.length; nodeId++ )
        {
            long labels = expected[nodeId];
            if ( (labels & mask) != 0 )
            {
                result[cursor++] = nodeId;
            }
        }
        return result;
    }

    static long flipRandom( long existingLabels, int highLabelId, Random random )
    {
        return existingLabels ^ (1L << random.nextInt( highLabelId ));
    }

    public static long[] getLabels( long bits )
    {
        long[] result = new long[Long.bitCount( bits )];
        for ( int labelId = 0, c = 0; labelId < LABEL_COUNT; labelId++ )
        {
            int mask = 1 << labelId;
            if ( (bits & mask) != 0 )
            {
                result[c++] = labelId;
            }
        }
        return result;
    }
}
