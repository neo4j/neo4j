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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.IndexReaderStub;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartition;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NonUniqueDatabaseIndexSamplerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final IndexSearcher indexSearcher = mock( IndexSearcher.class, Mockito.RETURNS_DEEP_STUBS );
    private final TaskCoordinator taskControl = new TaskCoordinator( 0, TimeUnit.MILLISECONDS );
    private final IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );

    @Test
    public void nonUniqueSamplingCancel() throws IndexNotFoundKernelException, IOException
    {
        Terms terms = getTerms( "test", 1 );
        Map<String,Terms> fieldTermsMap = MapUtil.genericMap( "0string", terms, "id", terms, "0string", terms );
        IndexReaderStub indexReader = new IndexReaderStub( new SamplingFields( fieldTermsMap ) );
        when( indexSearcher.getIndexReader() ).thenReturn( indexReader );

        expectedException.expect( IndexNotFoundKernelException.class );
        expectedException.expectMessage( "Index dropped while sampling." );

        NonUniqueLuceneIndexSampler luceneIndexSampler = createSampler();
        taskControl.cancel();
        luceneIndexSampler.sampleIndex();
    }

    @Test
    public void nonUniqueIndexSampling() throws Exception
    {
        Terms aTerms = getTerms( "a", 1 );
        Terms idTerms = getTerms( "id", 2 );
        Terms bTerms = getTerms( "b", 3 );
        Map<String,Terms> fieldTermsMap = MapUtil.genericMap( "0string", aTerms, "id", idTerms, "0array", bTerms );
        IndexReaderStub indexReader = new IndexReaderStub( new SamplingFields( fieldTermsMap ) );
        indexReader.setElements( new String[4] );
        when( indexSearcher.getIndexReader() ).thenReturn( indexReader );

        assertEquals( new IndexSample( 4, 2, 4 ), createSampler().sampleIndex() );
    }

    @Test
    public void samplingOfLargeNumericValues() throws Exception
    {
        try ( RAMDirectory dir = new RAMDirectory();
              WritableIndexPartition indexPartition = new WritableIndexPartition( new File( "testPartition" ), dir,
                      IndexWriterConfigs.standard() ) )
        {
            insertDocument( indexPartition, 1, Long.MAX_VALUE );
            insertDocument( indexPartition, 2, Integer.MAX_VALUE );

            indexPartition.maybeRefreshBlocking();

            try ( PartitionSearcher searcher = indexPartition.acquireSearcher() )
            {
                NonUniqueLuceneIndexSampler sampler = new NonUniqueLuceneIndexSampler( searcher.getIndexSearcher(),
                        taskControl.newInstance(), new IndexSamplingConfig( Config.defaults() ) );

                assertEquals( new IndexSample( 2, 2, 2 ), sampler.sampleIndex() );
            }
        }
    }

    private NonUniqueLuceneIndexSampler createSampler()
    {
        return new NonUniqueLuceneIndexSampler( indexSearcher, taskControl.newInstance(), indexSamplingConfig );
    }

    private Terms getTerms( String value, int frequency ) throws IOException
    {
        TermsEnum termsEnum = mock( TermsEnum.class );
        Terms terms = mock( Terms.class );
        when( terms.iterator() ).thenReturn( termsEnum );
        when( termsEnum.next() ).thenReturn( new BytesRef( value.getBytes() ) ).thenReturn( null );
        when( termsEnum.docFreq() ).thenReturn( frequency );
        return terms;
    }

    private static void insertDocument( WritableIndexPartition partition, long nodeId, Object propertyValue )
            throws IOException
    {
        Document doc = LuceneDocumentStructure.documentRepresentingProperties( nodeId, Values.of( propertyValue ) );
        partition.getIndexWriter().addDocument( doc );
    }

    private class SamplingFields extends Fields
    {

        private Map<String,Terms> fieldTermsMap;

        SamplingFields( Map<String,Terms> fieldTermsMap )
        {
            this.fieldTermsMap = fieldTermsMap;
        }

        @Override
        public Iterator<String> iterator()
        {
            return fieldTermsMap.keySet().iterator();
        }

        @Override
        public Terms terms( String field )
        {
            return fieldTermsMap.get( field );
        }

        @Override
        public int size()
        {
            return fieldTermsMap.size();
        }
    }

}
