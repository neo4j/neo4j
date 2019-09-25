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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.BitSet;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Math.toIntExact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.index.label.FullStoreChangeStream.EMPTY;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( {RandomExtension.class, LifeExtension.class} )
class NativeLabelScanReaderIT
{
    @Inject
    private RandomRule random;
    @Inject
    private LifeSupport life;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldStartFromGivenIdDense() throws IOException
    {
        shouldStartFromGivenId( 10 );
    }

    @Test
    void shouldStartFromGivenIdSparse() throws IOException
    {
        shouldStartFromGivenId( 100 );
    }

    @Test
    void shouldStartFromGivenIdSuperSparse() throws IOException
    {
        shouldStartFromGivenId( 1000 );
    }

    private void shouldStartFromGivenId( int sparsity ) throws IOException
    {
        // given
        NativeLabelScanStore store = life.add(
                new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, EMPTY, false, new Monitors(), immediate() ) );
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
        LabelScanReader reader = store.newReader();
        try ( PrimitiveLongResourceIterator ids = reader.nodesWithAnyOfLabels( fromId, new int[] {labelId} ) )
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
