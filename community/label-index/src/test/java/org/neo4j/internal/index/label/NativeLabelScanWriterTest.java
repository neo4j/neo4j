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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.api.NodeLabelUpdate;
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
import static org.neo4j.internal.index.label.LabelScanReader.NO_ID;
import static org.neo4j.internal.index.label.LabelScanValue.RANGE_SIZE;
import static org.neo4j.internal.index.label.NativeLabelScanStoreIT.flipRandom;
import static org.neo4j.internal.index.label.NativeLabelScanStoreIT.getLabels;
import static org.neo4j.internal.index.label.NativeLabelScanStoreIT.nodesWithLabel;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class NativeLabelScanWriterTest
{
    private static final int LABEL_COUNT = 5;
    private static final int NODE_COUNT = 10_000;

    @Inject
    private RandomRule random;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    private GBPTree<LabelScanKey,LabelScanValue> tree;

    @BeforeEach
    void openTree()
    {
        tree = new GBPTreeBuilder<>( pageCache, directory.file( "file" ), new LabelScanLayout() ).build();
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
        try ( NativeLabelScanWriter writer = new NativeLabelScanWriter( max( 5, NODE_COUNT / 100 ), NativeLabelScanWriter.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );

            // WHEN
            for ( int i = 0; i < NODE_COUNT * 3; i++ )
            {
                NodeLabelUpdate update = randomUpdate( expected );
                writer.write( update );
            }
        }

        // THEN
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] expectedNodeIds = nodesWithLabel( expected, i );
            long[] actualNodeIds = asArray( new LabelScanValueIterator(
                    tree.seek( new LabelScanKey( i, 0 ), new LabelScanKey( i, Long.MAX_VALUE ), NULL ), NO_ID ) );
            assertArrayEquals( expectedNodeIds, actualNodeIds, "For label " + i );
        }
    }

    @Test
    void shouldNotAcceptUnsortedLabels()
    {
        // GIVEN
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () ->
        {
            try ( NativeLabelScanWriter writer = new NativeLabelScanWriter( 1, NativeLabelScanWriter.EMPTY ) )
            {
                writer.initialize( tree.writer( NULL ) );

                // WHEN
                writer.write( NodeLabelUpdate.labelChanges( 0, EMPTY_LONG_ARRAY, new long[]{2, 1} ) );
                // we can't do the usual "fail( blabla )" here since the actual write will happen
                // when closing this writer, i.e. in the curly bracket below.
            }
        } );
        assertTrue( exception.getMessage().contains( "unsorted" ) );
    }

    @Test
    void shouldRemoveTreeEmptyTreeEntries() throws IOException
    {
        // given
        int numberOfTreeEntries = 3;
        int numberOfNodesInEach = 5;
        int labelId = 1;
        long[] labels = {labelId};
        try ( NativeLabelScanWriter writer = new NativeLabelScanWriter( max( 5, NODE_COUNT / 100 ), NativeLabelScanWriter.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );

            // a couple of entries with a couple of entries each
            // concept art: [xxxx          ][xxxx          ][xxxx          ] where x is used node.
            for ( int i = 0; i < numberOfTreeEntries; i++ )
            {
                long baseNodeId = i * RANGE_SIZE;
                for ( int j = 0; j < numberOfNodesInEach; j++ )
                {
                    writer.write( NodeLabelUpdate.labelChanges( baseNodeId + j, EMPTY_LONG_ARRAY, labels ) );
                }
            }
        }
        assertTreeHasKeysRepresentingIdRanges( setOfRange( 0, numberOfTreeEntries ) );

        // when removing all the nodes from one of the tree nodes
        int treeEntryToRemoveFrom = 1;
        try ( NativeLabelScanWriter writer = new NativeLabelScanWriter( max( 5, NODE_COUNT / 100 ), NativeLabelScanWriter.EMPTY ) )
        {
            writer.initialize( tree.writer( NULL ) );
            long baseNodeId = treeEntryToRemoveFrom * RANGE_SIZE;
            for ( int i = 0; i < numberOfNodesInEach; i++ )
            {
                writer.write( NodeLabelUpdate.labelChanges( baseNodeId + i, labels, EMPTY_LONG_ARRAY ) );
            }
        }

        // then
        MutableLongSet expected = setOfRange( 0, numberOfTreeEntries );
        expected.remove( treeEntryToRemoveFrom );
        assertTreeHasKeysRepresentingIdRanges( expected );
    }

    private NodeLabelUpdate randomUpdate( long[] expected )
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
        return NodeLabelUpdate.labelChanges( nodeId, before, getLabels( labels ) );
    }

    private void assertTreeHasKeysRepresentingIdRanges( MutableLongSet expected ) throws IOException
    {
        tree.visit( new GBPTreeVisitor.Adaptor<>()
        {
            @Override
            public void key( LabelScanKey labelScanKey, boolean isLeaf, long offloadId )
            {
                if ( isLeaf )
                {
                    assertTrue( expected.remove( labelScanKey.idRange ) );
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
}
