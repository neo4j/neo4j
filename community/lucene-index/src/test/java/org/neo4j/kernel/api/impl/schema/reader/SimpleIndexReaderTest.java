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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQuery.range;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.values.storable.Values.stringValue;

class SimpleIndexReaderTest
{
    private static final SchemaDescriptor SCHEMA = SchemaDescriptor.forLabel( 0, 0 );
    private final PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
    private final IndexSearcher indexSearcher = mock( IndexSearcher.class );
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 0, TimeUnit.MILLISECONDS );

    @BeforeEach
    void setUp()
    {
        when( partitionSearcher.getIndexSearcher() ).thenReturn( indexSearcher );
    }

    @Test
    void releaseSearcherOnClose() throws IOException
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.close();

        verify( partitionSearcher ).close();
    }

    @Test
    void seekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        doQuery( simpleIndexReader, IndexQuery.exact( 1, "test" ) );

        verify( indexSearcher ).search( any( BooleanQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    void scanQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        doQuery( simpleIndexReader, IndexQuery.exists( 1 ) );

        verify( indexSearcher ).search( any( MatchAllDocsQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    void stringRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        doQuery( simpleIndexReader, range( 1, "a", false, "b", true ) );

        verify( indexSearcher ).search( any( TermRangeQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    void prefixRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        doQuery( simpleIndexReader, IndexQuery.stringPrefix( 1, stringValue( "bb" ) ));

        verify( indexSearcher ).search( any( MultiTermQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    void numberRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        assertThrows( UnsupportedOperationException.class, () -> doQuery( simpleIndexReader, range( 1, 7, true, 8, true ) ) );
    }

    @Test
    void countIndexedNodesReachSearcher() throws IOException
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.countIndexedNodes( 2, new int[] {3}, Values.of( "testValue" ) );

        verify( indexSearcher ).search( any( BooleanQuery.class ), any( TotalHitCountCollector.class ) );
    }

    @Test
    void uniqueIndexSamplerForUniqueIndex()
    {
        SimpleIndexReader uniqueSimpleReader = getUniqueSimpleReader();
        assertThat( uniqueSimpleReader.createSampler() ).isInstanceOf( UniqueLuceneIndexSampler.class );
    }

    @Test
    void nonUniqueIndexSamplerForNonUniqueIndex()
    {
        SimpleIndexReader uniqueSimpleReader = getNonUniqueSimpleReader();
        assertThat( uniqueSimpleReader.createSampler() ).isInstanceOf( NonUniqueLuceneIndexSampler.class );
    }

    private void doQuery( IndexReader reader, IndexQuery query ) throws IndexNotApplicableKernelException
    {
        reader.query( NULL_CONTEXT, new NodeValueIterator(), IndexOrder.NONE, false, query );
    }

    private SimpleIndexReader getNonUniqueSimpleReader()
    {
        IndexDescriptor index = IndexPrototype.forSchema( SCHEMA ).withName( "a" ).materialise( 0 );
        return new SimpleIndexReader( partitionSearcher, index, samplingConfig, taskCoordinator );
    }

    private SimpleIndexReader getUniqueSimpleReader()
    {
        IndexDescriptor index = IndexPrototype.uniqueForSchema( SCHEMA ).withName( "b" ).materialise( 1 );
        return new SimpleIndexReader( partitionSearcher, index, samplingConfig, taskCoordinator );
    }
}
