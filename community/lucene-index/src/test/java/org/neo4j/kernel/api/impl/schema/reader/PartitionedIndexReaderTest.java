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
package org.neo4j.kernel.api.impl.schema.reader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PartitionedIndexReaderTest
{

    private IndexConfiguration indexConfiguration = IndexConfiguration.NON_UNIQUE;
    @Mock
    private IndexSamplingConfig samplingConfig;
    @Mock
    private TaskCoordinator taskCoordinator;
    @Mock
    private PartitionSearcher partitionSearcher1;
    @Mock
    private PartitionSearcher partitionSearcher2;
    @Mock
    private PartitionSearcher partitionSearcher3;
    @Mock
    private SimpleIndexReader indexReader1;
    @Mock
    private SimpleIndexReader indexReader2;
    @Mock
    private SimpleIndexReader indexReader3;

    @Test
    public void partitionedReaderCloseAllSearchers() throws IOException
    {
        PartitionedIndexReader partitionedIndexReader = createPartitionedReader();

        partitionedIndexReader.close();

        verify( partitionSearcher1 ).close();
        verify( partitionSearcher2 ).close();
        verify( partitionSearcher3 ).close();
    }

    @Test
    public void seekOverAllPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        when( indexReader1.seek( "Test" ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.seek( "Test" ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.seek( "Test" ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.seek( "Test" ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByNumberOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        when( indexReader1.rangeSeekByNumberInclusive( 1, 2 ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.rangeSeekByNumberInclusive( 1, 2 ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.rangeSeekByNumberInclusive( 1, 2 ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet results =
                PrimitiveLongCollections.asSet( indexReader.rangeSeekByNumberInclusive( 1, 2 ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByStringOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        when( indexReader1.rangeSeekByString( "a", false, "b", true ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.rangeSeekByString( "a", false, "b", true ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.rangeSeekByString( "a", false, "b", true ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet results =
                PrimitiveLongCollections.asSet( indexReader.rangeSeekByString( "a", false, "b", true ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByPrefixOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.rangeSeekByPrefix( "prefix" ) ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.rangeSeekByPrefix( "prefix" ) ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.rangeSeekByPrefix( "prefix" ) ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.rangeSeekByPrefix( "prefix") );
        verifyResult( results );
    }

    @Test
    public void scanOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.scan() ).thenReturn( PrimitiveLongCollections.iterator( 1 ) );
        when( indexReader2.scan() ).thenReturn( PrimitiveLongCollections.iterator( 2 ) );
        when( indexReader3.scan() ).thenReturn( PrimitiveLongCollections.iterator( 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.scan() );
        verifyResult( results );
    }

    @Test
    public void countNodesOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.countIndexedNodes(1, "a") ).thenReturn( 1L );
        when( indexReader2.countIndexedNodes(1, "a") ).thenReturn( 2L );
        when( indexReader3.countIndexedNodes(1, "a") ).thenReturn( 3L );

        assertEquals( 6, indexReader.countIndexedNodes( 1, "a" ) );
    }

    @Test
    public void samplingOverPartitions() throws IndexNotFoundKernelException
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.createSampler() ).thenReturn( new SimpleSampler( 1 ) );
        when( indexReader2.createSampler() ).thenReturn( new SimpleSampler( 2 ) );
        when( indexReader3.createSampler() ).thenReturn( new SimpleSampler( 3 ) );

        IndexSampler sampler = indexReader.createSampler();
        assertEquals( new IndexSample( 6, 6, 6 ), sampler.sampleIndex() );
    }

    private void verifyResult( PrimitiveLongSet results )
    {
        assertEquals(3, results.size());
        assertTrue( results.contains( 1 ) );
        assertTrue( results.contains( 2 ) );
        assertTrue( results.contains( 3 ) );
    }

    private PartitionedIndexReader createPartitionedReaderFromReaders()
    {
        return new PartitionedIndexReader( getPartitionReaders() );
    }

    private List<SimpleIndexReader> getPartitionReaders()
    {
        return Arrays.asList( indexReader1, indexReader2, indexReader3 );
    }

    private PartitionedIndexReader createPartitionedReader()
    {
        return new PartitionedIndexReader( getPartitionSearchers(), indexConfiguration, samplingConfig,
                taskCoordinator );
    }

    private List<PartitionSearcher> getPartitionSearchers()
    {
        return Arrays.asList( partitionSearcher1, partitionSearcher2, partitionSearcher3 );
    }

    private class SimpleSampler implements IndexSampler
    {
        private long sampleValue;

        SimpleSampler( long sampleValue )
        {
            this.sampleValue = sampleValue;
        }

        @Override
        public IndexSample sampleIndex() throws IndexNotFoundKernelException
        {
            return new IndexSample( sampleValue, sampleValue, sampleValue );
        }
    }
}
