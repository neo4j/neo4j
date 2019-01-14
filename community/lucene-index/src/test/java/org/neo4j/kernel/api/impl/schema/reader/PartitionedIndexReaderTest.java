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
package org.neo4j.kernel.api.impl.schema.reader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PartitionedIndexReaderTest
{

    private SchemaIndexDescriptor schemaIndexDescriptor = SchemaIndexDescriptorFactory.forLabel( 0, 1 );
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
    public void seekOverAllPartitions() throws Exception
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        IndexQuery.ExactPredicate query = IndexQuery.exact( 1, "Test" );
        when( indexReader1.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 1 ) );
        when( indexReader2.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2 ) );
        when( indexReader3.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.query( query ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByNumberOverPartitions() throws Exception
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        IndexQuery.RangePredicate<?> query = IndexQuery.range( 1, 1, true, 2, true );
        when( indexReader1.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 1 ) );
        when( indexReader2.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2 ) );
        when( indexReader3.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3 ) );

        PrimitiveLongSet results =
                PrimitiveLongCollections.asSet( indexReader.query( query ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByStringOverPartitions() throws Exception
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();

        IndexQuery.RangePredicate<?> query = IndexQuery.range( 1, "a", false, "b", true );
        when( indexReader1.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 1 ) );
        when( indexReader2.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2 ) );
        when( indexReader3.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3 ) );

        PrimitiveLongSet results =
                PrimitiveLongCollections.asSet( indexReader.query( query ) );
        verifyResult( results );
    }

    @Test
    public void rangeSeekByPrefixOverPartitions() throws Exception
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        IndexQuery.StringPrefixPredicate query = IndexQuery.stringPrefix( 1, "prefix" );
        when( indexReader1.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 1 ) );
        when( indexReader2.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2 ) );
        when( indexReader3.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.query( query ) );
        verifyResult( results );
    }

    @Test
    public void scanOverPartitions() throws Exception
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        IndexQuery.ExistsPredicate query = IndexQuery.exists( 1 );
        when( indexReader1.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 1 ) );
        when( indexReader2.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2 ) );
        when( indexReader3.query( query ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3 ) );

        PrimitiveLongSet results = PrimitiveLongCollections.asSet( indexReader.query( query ) );
        verifyResult( results );
    }

    @Test
    public void countNodesOverPartitions()
    {
        PartitionedIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.countIndexedNodes( 1, Values.of( "a" ) ) ).thenReturn( 1L );
        when( indexReader2.countIndexedNodes( 1, Values.of( "a" ) ) ).thenReturn( 2L );
        when( indexReader3.countIndexedNodes( 1, Values.of( "a" ) ) ).thenReturn( 3L );

        assertEquals( 6, indexReader.countIndexedNodes( 1, Values.of( "a" ) ) );
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
        return new PartitionedIndexReader( schemaIndexDescriptor, getPartitionReaders() );
    }

    private List<SimpleIndexReader> getPartitionReaders()
    {
        return Arrays.asList( indexReader1, indexReader2, indexReader3 );
    }

    private PartitionedIndexReader createPartitionedReader()
    {
        return new PartitionedIndexReader( getPartitionSearchers(), schemaIndexDescriptor, samplingConfig, taskCoordinator );
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
        public IndexSample sampleIndex()
        {
            return new IndexSample( sampleValue, sampleValue, sampleValue );
        }
    }
}
