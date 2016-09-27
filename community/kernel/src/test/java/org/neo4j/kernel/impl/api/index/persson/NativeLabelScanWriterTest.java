/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.index.persson;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cursor.Cursor;
import org.neo4j.index.BTreeHit;
import org.neo4j.index.SCInserter;
import org.neo4j.index.ValueAmender;
import org.neo4j.index.btree.CompactLabelScanLayout;
import org.neo4j.index.btree.LabelScanKey;
import org.neo4j.index.btree.LabelScanValue;
import org.neo4j.index.btree.MutableBTreeHit;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertArrayEquals;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.impl.api.index.persson.NativeLabelScanStoreTest.flipRandom;
import static org.neo4j.kernel.impl.api.index.persson.NativeLabelScanStoreTest.getLabels;
import static org.neo4j.kernel.impl.api.index.persson.NativeLabelScanStoreTest.nodesWithLabel;

public class NativeLabelScanWriterTest
{
    private static final int LABEL_COUNT = 5;
    private static final int RANGE_SIZE = 16;

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldAddLabels() throws Exception
    {
        // GIVEN
        ControlledInserter inserter = new ControlledInserter();
        long[] expected = new long[10_000];
        try ( NativeLabelScanWriter writer = new NativeLabelScanWriter( inserter, RANGE_SIZE, 100 ) )
        {
            // WHEN
            for ( int i = 0; i < 10_000; i++ )
            {
                NodeLabelUpdate update = randomUpdate( expected );
                writer.write( update );
            }
        }

        // THEN
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] expectedNodeIds = nodesWithLabel( expected, i );
            long[] actualNodeIds = asArray( new LabelScanValueIterator( RANGE_SIZE, inserter.nodesFor( i ) ) );
            assertArrayEquals( expectedNodeIds, actualNodeIds );
        }
    }

    private NodeLabelUpdate randomUpdate( long[] expected )
    {
        int nodeId = random.nextInt( expected.length );
        long labels = expected[nodeId];
        long[] before = getLabels( labels );
        int changeCount = random.nextInt( 4 ) + 1;
        for ( int i = 0; i < changeCount; i++ )
        {
            labels = flipRandom( labels, random.random() );
        }
        expected[nodeId] = labels;
        return NodeLabelUpdate.labelChanges( nodeId, before, getLabels( labels ) );
    }

    private static class ControlledInserter implements SCInserter<LabelScanKey,LabelScanValue>
    {
        private final Map<Integer,Map<LabelScanKey,LabelScanValue>> data = new HashMap<>();

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public void insert( LabelScanKey key, LabelScanValue value, ValueAmender<LabelScanValue> amender )
                throws IOException
        {
            System.out.println( key + " - " + value );
            Map<LabelScanKey,LabelScanValue> forLabel = data.get( key.labelId );
            if ( forLabel == null )
            {
                data.put( key.labelId, forLabel = new HashMap<>() );
            }
            LabelScanValue existing = forLabel.get( key );
            if ( existing == null )
            {
                forLabel.put( key, value );
            }
            else
            {
                amender.amend( existing, value );
            }
        }

        @Override
        public LabelScanValue remove( LabelScanKey key ) throws IOException
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }

        @SuppressWarnings( "unchecked" )
        Cursor<BTreeHit<LabelScanKey,LabelScanValue>> nodesFor( int labelId )
        {
            Comparator<LabelScanKey> comparator = new CompactLabelScanLayout( RANGE_SIZE );
            Map<LabelScanKey,LabelScanValue> forLabel = data.get( labelId );
            if ( forLabel == null )
            {
                forLabel = Collections.emptyMap();
            }

            Map.Entry<LabelScanKey,LabelScanValue>[] entries =
                    forLabel.entrySet().toArray( new Map.Entry[forLabel.size()] );
            Arrays.sort( entries, (o1,o2) -> comparator.compare( o1.getKey(), o2.getKey() ) );
            return new Cursor<BTreeHit<LabelScanKey,LabelScanValue>>()
            {
                private int arrayIndex = -1;

                @Override
                public BTreeHit<LabelScanKey,LabelScanValue> get()
                {
                    Entry<LabelScanKey,LabelScanValue> entry = entries[arrayIndex];
                    return new MutableBTreeHit<>( entry.getKey(), entry.getValue() );
                }

                @Override
                public boolean next()
                {
                    arrayIndex++;
                    return arrayIndex < entries.length;
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
