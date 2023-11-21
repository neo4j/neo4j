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
package org.neo4j.kernel.api.impl.schema.reader;

import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.values.storable.Values.stringValue;

class PartitionedValueIndexReaderTest
{
    private static final int PROP_KEY = 1;
    private static final int LABEL_ID = 0;

    private final IndexDescriptor schemaIndexDescriptor = IndexPrototype.forSchema( forLabel( LABEL_ID, PROP_KEY ) ).withName( "index" ).materialise( 0 );
    private final IndexSamplingConfig samplingConfig = mock( IndexSamplingConfig.class );
    private final TaskCoordinator taskCoordinator = mock( TaskCoordinator.class );
    private final PartitionSearcher partitionSearcher1 = mock( PartitionSearcher.class );
    private final PartitionSearcher partitionSearcher2 = mock( PartitionSearcher.class );
    private final PartitionSearcher partitionSearcher3 = mock( PartitionSearcher.class );
    private final SimpleValueIndexReader indexReader1 = mock( SimpleValueIndexReader.class );
    private final SimpleValueIndexReader indexReader2 = mock( SimpleValueIndexReader.class );
    private final SimpleValueIndexReader indexReader3 = mock( SimpleValueIndexReader.class );

    @Test
    void partitionedReaderCloseAllSearchers() throws IOException
    {
        PartitionedValueIndexReader partitionedIndexReader = createPartitionedReader();

        partitionedIndexReader.close();

        verify( partitionSearcher1 ).close();
        verify( partitionSearcher2 ).close();
        verify( partitionSearcher3 ).close();
    }

    @Test
    void seekOverAllPartitions() throws Exception
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.ExactPredicate query = PropertyIndexQuery.exact( 1, "Test" );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 1 ) ).when( indexReader1 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 2 ) ).when( indexReader2 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 3 ) ).when( indexReader3 ).query( any(), any(), any(), any(), any() );

        LongSet results = queryResultAsSet( indexReader, query );
        verifyResult( results );
    }

    @Test
    void rangeSeekByNumberOverPartitions() throws Exception
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.RangePredicate<?> query = PropertyIndexQuery.range( 1, 1, true, 2, true );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 1 ) ).when( indexReader1 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 2 ) ).when( indexReader2 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 3 ) ).when( indexReader3 ).query( any(), any(), any(), any(), any() );

        LongSet results = queryResultAsSet( indexReader, query );
        verifyResult( results );
    }

    @Test
    void rangeSeekByStringOverPartitions() throws Exception
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.RangePredicate<?> query = PropertyIndexQuery.range( 1, "a", false, "b", true );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 1 ) ).when( indexReader1 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 2 ) ).when( indexReader2 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 3 ) ).when( indexReader3 ).query( any(), any(), any(), any(), any() );

        LongSet results = queryResultAsSet( indexReader, query );
        verifyResult( results );
    }

    @Test
    void rangeSeekByPrefixOverPartitions() throws Exception
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        PropertyIndexQuery.StringPrefixPredicate query = PropertyIndexQuery.stringPrefix( 1,  stringValue( "prefix" ) );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 1 ) ).when( indexReader1 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 2 ) ).when( indexReader2 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 3 ) ).when( indexReader3 ).query( any(), any(), any(), any(), any() );

        LongSet results = queryResultAsSet( indexReader, query );
        verifyResult( results );
    }

    @Test
    void scanOverPartitions() throws Exception
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        PropertyIndexQuery.ExistsPredicate query = PropertyIndexQuery.exists( 1 );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 1 ) ).when( indexReader1 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 2 ) ).when( indexReader2 ).query( any(), any(), any(), any(), any() );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( schemaIndexDescriptor, 3 ) ).when( indexReader3 ).query( any(), any(), any(), any(), any() );

        LongSet results = queryResultAsSet( indexReader, query );
        verifyResult( results );
    }

    @Test
    void countNodesOverPartitions()
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.countIndexedEntities( 1, NULL, new int[] {PROP_KEY}, Values.of( "a" ) ) ).thenReturn( 1L );
        when( indexReader2.countIndexedEntities( 1, NULL, new int[] {PROP_KEY}, Values.of( "a" ) ) ).thenReturn( 2L );
        when( indexReader3.countIndexedEntities( 1, NULL, new int[] {PROP_KEY}, Values.of( "a" ) ) ).thenReturn( 3L );

        assertEquals( 6, indexReader.countIndexedEntities( 1, NULL, new int[] {PROP_KEY}, Values.of( "a" ) ) );
    }

    @Test
    void samplingOverPartitions() throws IndexNotFoundKernelException
    {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        when( indexReader1.createSampler() ).thenReturn( new SimpleSampler( 1 ) );
        when( indexReader2.createSampler() ).thenReturn( new SimpleSampler( 2 ) );
        when( indexReader3.createSampler() ).thenReturn( new SimpleSampler( 3 ) );

        IndexSampler sampler = indexReader.createSampler();
        assertEquals( new IndexSample( 6, 6, 6 ), sampler.sampleIndex( NULL, new AtomicBoolean() ) );
    }

    private static LongSet queryResultAsSet( PartitionedValueIndexReader indexReader, PropertyIndexQuery query ) throws IndexNotApplicableKernelException
    {
        try ( NodeValueIterator iterator = new NodeValueIterator() )
        {
            indexReader.query( iterator, NULL_CONTEXT, AccessMode.Static.READ, unconstrained(), query );
            return PrimitiveLongCollections.asSet( iterator );
        }
    }

    private static void verifyResult( LongSet results )
    {
        assertEquals(3, results.size());
        assertTrue( results.contains( 1 ) );
        assertTrue( results.contains( 2 ) );
        assertTrue( results.contains( 3 ) );
    }

    private PartitionedValueIndexReader createPartitionedReaderFromReaders()
    {
        return new PartitionedValueIndexReader( schemaIndexDescriptor, getPartitionReaders() );
    }

    private List<SimpleValueIndexReader> getPartitionReaders()
    {
        return Arrays.asList( indexReader1, indexReader2, indexReader3 );
    }

    private PartitionedValueIndexReader createPartitionedReader()
    {
        return new PartitionedValueIndexReader( getPartitionSearchers(), schemaIndexDescriptor, samplingConfig, taskCoordinator );
    }

    private List<SearcherReference> getPartitionSearchers()
    {
        return Arrays.asList( partitionSearcher1, partitionSearcher2, partitionSearcher3 );
    }

    private static class SimpleSampler implements IndexSampler
    {
        private final long sampleValue;

        SimpleSampler( long sampleValue )
        {
            this.sampleValue = sampleValue;
        }

        @Override
        public IndexSample sampleIndex( CursorContext cursorContext, AtomicBoolean stopped )
        {
            return new IndexSample( sampleValue, sampleValue, sampleValue );
        }
    }
}
