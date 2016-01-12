package org.neo4j.kernel.api.impl.index.sampler;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.IndexReaderStub;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NonUniqueLuceneIndexSamplerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final IndexSearcher indexSearcher = mock( IndexSearcher.class, Mockito.RETURNS_DEEP_STUBS );
    private final TaskCoordinator taskControl = new TaskCoordinator( 0, TimeUnit.MILLISECONDS );
    private final IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );

    @Test
    public void nonUniqueSamplingCancel() throws IndexNotFoundKernelException, IOException
    {
        Terms terms = getTerms( "test", 1 );
        Map<String,Terms> fieldTermsMap = MapUtil.genericMap( "a", terms, "id", terms, "b", terms );
        IndexReaderStub indexReader = new IndexReaderStub( new SamplingFields( fieldTermsMap ) );
        when( indexSearcher.getIndexReader() ).thenReturn( indexReader );

        expectedException.expect( IndexNotFoundKernelException.class );
        expectedException.expectMessage( "Index dropped while sampling." );

        NonUniqueLuceneIndexSampler luceneIndexSampler = createSampler();
        taskControl.cancel();
        luceneIndexSampler.sampleIndex( Registers.newDoubleLongRegister() );
    }

    @Test
    public void nonUniqueIndexSampling() throws Exception
    {
        Terms aTerms = getTerms( "a", 1 );
        Terms idTerms = getTerms( "id", 2 );
        Terms bTerms = getTerms( "b", 3 );
        Map<String,Terms> fieldTermsMap = MapUtil.genericMap( "a", aTerms, "id", idTerms, "b", bTerms );
        IndexReaderStub indexReader = new IndexReaderStub( new SamplingFields( fieldTermsMap ) );
        when( indexSearcher.getIndexReader() ).thenReturn( indexReader );

        NonUniqueLuceneIndexSampler luceneIndexSampler = createSampler();
        Register.DoubleLongRegister register = Registers.newDoubleLongRegister();
        long sample = luceneIndexSampler.sampleIndex( register );

        // unique values
        assertEquals( 2, register.readFirst() );
        // total values
        assertEquals( 4, register.readSecond() );
        assertEquals( 4, sample );
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

    private class SamplingFields extends Fields
    {

        private Map<String,Terms> fieldTermsMap;

        public SamplingFields( Map<String,Terms> fieldTermsMap )
        {
            this.fieldTermsMap = fieldTermsMap;
        }

        @Override
        public Iterator<String> iterator()
        {
            return fieldTermsMap.keySet().iterator();
        }

        @Override
        public Terms terms( String field ) throws IOException
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