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
package org.neo4j.internal.index.label;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.index.label.FullStoreChangeStream.EMPTY;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( {RandomExtension.class, LifeExtension.class} )
class NativeLabelScanStoreIT
{
    @Inject
    private RandomRule random;
    @Inject
    private LifeSupport life;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;

    private NativeLabelScanStore store;

    private static final int NODE_COUNT = 10_000;
    private static final int LABEL_COUNT = 12;

    @BeforeEach
    void before() throws IOException
    {
        newLabelScanStore();
    }

    @Test
    void shouldRandomlyTestIt() throws Exception
    {
        // GIVEN
        long[] expected = new long[NODE_COUNT];
        randomModifications( expected, NODE_COUNT );

        // WHEN/THEN
        for ( int i = 0; i < 100; i++ )
        {
            verifyReads( expected );
            randomModifications( expected, NODE_COUNT / 10 );
        }
    }

    @Test
    public void shouldRemoveEmptyBitMaps() throws IOException
    {
        LabelScanLayout labelScanLayout = new LabelScanLayout();
        int nodeId = 1;
        long nodeIdRange = NativeLabelScanWriter.rangeOf( nodeId );
        int labelId = 1;
        long[] noLabels = new long[0];
        long[] singleLabel = new long[]{labelId};
        LabelScanKey key = labelScanLayout.newKey();
        key.set( labelId, nodeIdRange );

        // Add
        try ( LabelScanWriter writer = store.newWriter() )
        {
            writer.write( NodeLabelUpdate.labelChanges( nodeId, noLabels, singleLabel ) );
        }
        store.force( IOLimiter.UNLIMITED );
        store.shutdown();

        // Verify exists
        try ( GBPTree<LabelScanKey,LabelScanValue> tree = openReadOnlyGBPTree( labelScanLayout ) )
        {
            try ( Seeker<LabelScanKey,LabelScanValue> seek = tree.seek( key, key ) )
            {
                assertTrue( seek.next(), "Expected to find the newly inserted entry" );
            }
        }

        // Remove
        newLabelScanStore();
        try ( LabelScanWriter writer = store.newWriter() )
        {
            writer.write( NodeLabelUpdate.labelChanges( nodeId, singleLabel, noLabels ) );
        }
        store.force( IOLimiter.UNLIMITED );
        store.shutdown();

        // Verify don't exists
        try ( GBPTree<LabelScanKey,LabelScanValue> tree = openReadOnlyGBPTree( labelScanLayout ) )
        {
            try ( Seeker<LabelScanKey,LabelScanValue> seek = tree.seek( key, key ) )
            {
                assertFalse( seek.next(), "Expected tree to be empty after removing the last label" );
            }
        }
    }

    private GBPTree<LabelScanKey,LabelScanValue> openReadOnlyGBPTree( LabelScanLayout labelScanLayout )
    {
        return new GBPTree<>( pageCache, databaseLayout.labelScanStore(), labelScanLayout, 0, NO_MONITOR, NO_HEADER_READER,
                NO_HEADER_WRITER, ignore(), true );
    }

    private void newLabelScanStore() throws IOException
    {
        if ( store != null )
        {
            store.shutdown();
        }

        store = life.add( new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, EMPTY, false, new Monitors(), immediate() ) );
    }

    private void verifyReads( long[] expected )
    {
        LabelScanReader reader = store.newReader();
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] actualNodes = closingAsArray( reader.nodesWithLabel( i ) );
            long[] expectedNodes = nodesWithLabel( expected, i );
            assertArrayEquals( expectedNodes, actualNodes );
        }
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

    private void randomModifications( long[] expected, int count ) throws IOException
    {
        BitSet editedNodes = new BitSet();
        try ( LabelScanWriter writer = store.newWriter() )
        {
            for ( int i = 0; i < count; i++ )
            {
                int nodeId = random.nextInt( NODE_COUNT );
                if ( editedNodes.get( nodeId ) )
                {
                    i--;
                    continue;
                }

                int changeSize = random.nextInt( 3 ) + 1;
                long labels = expected[nodeId];
                long[] labelsBefore = getLabels( labels );
                for ( int j = 0; j < changeSize; j++ )
                {
                    labels = flipRandom( labels, LABEL_COUNT, random.random() );
                }
                long[] labelsAfter = getLabels( labels );
                editedNodes.set( nodeId );

                NodeLabelUpdate labelChanges = labelChanges( nodeId, labelsBefore, labelsAfter );
                writer.write( labelChanges );
                expected[nodeId] = labels;
            }
        }
    }

    static long flipRandom( long existingLabels, int highLabelId, Random random )
    {
        return existingLabels ^ (1 << random.nextInt( highLabelId ));
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
