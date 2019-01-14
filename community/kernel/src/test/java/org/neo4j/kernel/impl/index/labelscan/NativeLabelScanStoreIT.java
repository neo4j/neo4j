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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

public class NativeLabelScanStoreIT
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final LifeRule life = new LifeRule( true );
    private final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( life ).around( random );
    private NativeLabelScanStore store;

    private static final int NODE_COUNT = 10_000;
    private static final int LABEL_COUNT = 12;

    @Before
    public void before()
    {
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        store = life.add( new NativeLabelScanStore( pageCache, fs, directory.absolutePath(), FullStoreChangeStream.EMPTY,
                false, new Monitors(), RecoveryCleanupWorkCollector.immediate(),
                // a bit of random pageSize
                Math.min( pageCache.pageSize(), 256 << random.nextInt( 5 ) ) ) );
    }

    @Test
    public void shouldRandomlyTestIt() throws Exception
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

    private void verifyReads( long[] expected )
    {
        try ( LabelScanReader reader = store.newReader() )
        {
            for ( int i = 0; i < LABEL_COUNT; i++ )
            {
                long[] actualNodes = asArray( reader.nodesWithLabel( i ) );
                long[] expectedNodes = nodesWithLabel( expected, i );
                assertArrayEquals( expectedNodes, actualNodes );
            }
        }
    }

    public static long[] nodesWithLabel( long[] expected, int labelId )
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

    public static long flipRandom( long existingLabels, int highLabelId, Random random )
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
