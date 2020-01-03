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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Math.toIntExact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;

public class NativeLabelScanReaderIT
{
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();
    @Rule
    public final LifeRule life = new LifeRule( true );

    @Test
    public void shouldStartFromGivenIdDense() throws IOException
    {
        shouldStartFromGivenId( 10 );
    }

    @Test
    public void shouldStartFromGivenIdSparse() throws IOException
    {
        shouldStartFromGivenId( 100 );
    }

    @Test
    public void shouldStartFromGivenIdSuperSparse() throws IOException
    {
        shouldStartFromGivenId( 1000 );
    }

    private void shouldStartFromGivenId( int sparsity ) throws IOException
    {
        // given
        NativeLabelScanStore store = life.add(
                new NativeLabelScanStore( storage.pageCache(), DatabaseLayout.of( storage.directory().directory() ), storage.fileSystem(), EMPTY, false,
                        new Monitors(), immediate() ) );
        int labelId = 1;
        int highNodeId = 100_000;
        BitSet expected = new BitSet( highNodeId );
        try ( LabelScanWriter writer = store.newWriter() )
        {
            int updates = highNodeId / sparsity;
            for ( int i = 0; i < updates; i++ )
            {
                int nodeId = random.nextInt( highNodeId );
                writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{labelId} ) );
                expected.set( nodeId );
            }
        }

        // when
        long fromId = random.nextInt( highNodeId );
        int nextExpectedId = expected.nextSetBit( toIntExact( fromId + 1 ) );
        try ( LabelScanReader reader = store.newReader();
              PrimitiveLongResourceIterator ids = reader.nodesWithAnyOfLabels( fromId, new int[] {labelId} ) )
        {
            // then
            while ( nextExpectedId != -1 )
            {
                assertTrue( ids.hasNext() );
                long nextId = ids.next();
                assertEquals( nextExpectedId, toIntExact( nextId ) );
                nextExpectedId = expected.nextSetBit( nextExpectedId + 1 );
            }
            assertFalse( ids.hasNext() );
        }
    }
}
