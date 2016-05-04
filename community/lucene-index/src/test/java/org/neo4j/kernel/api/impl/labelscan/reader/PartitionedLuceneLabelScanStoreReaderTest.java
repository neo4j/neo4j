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
package org.neo4j.kernel.api.impl.labelscan.reader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.LabelScanStorageStrategy;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PartitionedLuceneLabelScanStoreReaderTest
{

    @Mock
    private LabelScanStorageStrategy scanStorageStrategy;
    @Mock
    private PartitionSearcher partitionSearcher1;
    @Mock
    private PartitionSearcher partitionSearcher2;
    @Mock
    private PartitionSearcher partitionSearcher3;
    @Mock
    private LabelScanReader indexReader1;
    @Mock
    private LabelScanReader indexReader2;
    @Mock
    private LabelScanReader indexReader3;

    @Test
    public void partitionedReaderCloseAllSearchers() throws IOException
    {
        PartitionedLuceneLabelScanStoreReader partitionedIndexReader = createPartitionedReader();

        partitionedIndexReader.close();

        verify( partitionSearcher1 ).close();
        verify( partitionSearcher2 ).close();
        verify( partitionSearcher3 ).close();
    }

    @Test
    public void labelsForNodeOverPartitions()
    {
        PartitionedLuceneLabelScanStoreReader storeReader = createPartitionedReaderWithReaders();

        when( indexReader1.labelsForNode( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.labelsForNode( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.labelsForNode( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet result = PrimitiveLongCollections.asSet( storeReader.labelsForNode( 1 ) );
        verifyResult( result );
    }

    @Test
    public void nodesWithLabelOverPartitions()
    {
        PartitionedLuceneLabelScanStoreReader storeReader = createPartitionedReaderWithReaders();

        when( indexReader1.nodesWithLabel( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.nodesWithLabel( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.nodesWithLabel( 1 ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet result = PrimitiveLongCollections.asSet( storeReader.nodesWithLabel( 1 ) );
        verifyResult( result );
    }

    private void verifyResult( PrimitiveLongSet results )
    {
        assertEquals( 3, results.size() );
        assertTrue( results.contains( 1 ) );
        assertTrue( results.contains( 2 ) );
        assertTrue( results.contains( 3 ) );
    }

    private PartitionedLuceneLabelScanStoreReader createPartitionedReaderWithReaders()
    {
        return new PartitionedLuceneLabelScanStoreReader( getLabelScanReaders() );
    }

    private List<LabelScanReader> getLabelScanReaders()
    {
        return Arrays.asList( indexReader1, indexReader2, indexReader3 );
    }

    private PartitionedLuceneLabelScanStoreReader createPartitionedReader()
    {
        return new PartitionedLuceneLabelScanStoreReader( getPartitionSearchers(), scanStorageStrategy );
    }

    private List<PartitionSearcher> getPartitionSearchers()
    {
        return Arrays.asList( partitionSearcher1, partitionSearcher2, partitionSearcher3 );
    }

}
