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
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQuery.range;

public class SimpleIndexReaderTest
{
    private final PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
    private final IndexSearcher indexSearcher = mock( IndexSearcher.class );
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 0, TimeUnit.MILLISECONDS );

    @Before
    public void setUp()
    {
        when( partitionSearcher.getIndexSearcher() ).thenReturn( indexSearcher );
    }

    @Test
    public void releaseSearcherOnClose() throws IOException
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.close();

        verify( partitionSearcher ).close();
    }

    @Test
    public void seekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.query( IndexQuery.exact( 1, "test" ) );

        verify( indexSearcher ).search( any( BooleanQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    public void scanQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.query( IndexQuery.exists( 1 ) );

        verify( indexSearcher ).search( any( MatchAllDocsQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    public void stringRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.query( range( 1, "a", false, "b", true ) );

        verify( indexSearcher ).search( any( TermRangeQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    public void prefixRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.query( IndexQuery.stringPrefix( 1, "bb" ) );

        verify( indexSearcher ).search( any( MultiTermQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    public void numberRangeSeekQueryReachSearcher() throws Exception
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.query( range( 1, 7, true, 8, true ) );

        verify( indexSearcher ).search( any( NumericRangeQuery.class ), any( DocValuesCollector.class ) );
    }

    @Test
    public void countIndexedNodesReachSearcher() throws IOException
    {
        IndexReader simpleIndexReader = getUniqueSimpleReader();

        simpleIndexReader.countIndexedNodes( 2, Values.of( "testValue" ) );

        verify( indexSearcher ).search( any( BooleanQuery.class ), any( TotalHitCountCollector.class ) );
    }

    @Test
    public void uniqueIndexSamplerForUniqueIndex()
    {
        SimpleIndexReader uniqueSimpleReader = getUniqueSimpleReader();
        Assert.assertThat( uniqueSimpleReader.createSampler(), instanceOf( UniqueLuceneIndexSampler.class ) );
    }

    @Test
    public void nonUuniqueIndexSamplerForNonUniqueIndex()
    {
        SimpleIndexReader uniqueSimpleReader = getNonUniqueSimpleReader();
        Assert.assertThat( uniqueSimpleReader.createSampler(), instanceOf( NonUniqueLuceneIndexSampler.class) );
    }

    private SimpleIndexReader getNonUniqueSimpleReader()
    {
        return new SimpleIndexReader( partitionSearcher, SchemaIndexDescriptorFactory.forLabel( 0, 0 ), samplingConfig,
                taskCoordinator );
    }

    private SimpleIndexReader getUniqueSimpleReader()
    {
        return new SimpleIndexReader( partitionSearcher, SchemaIndexDescriptorFactory.uniqueForLabel( 0, 0 ),
                samplingConfig, taskCoordinator );
    }
}
