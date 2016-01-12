package org.neo4j.kernel.api.impl.index.sampler;

import org.apache.lucene.search.IndexSearcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UniqueLuceneIndexSamplerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final IndexSearcher indexSearcher = mock( IndexSearcher.class, Mockito.RETURNS_DEEP_STUBS );
    private final TaskCoordinator taskControl = new TaskCoordinator(0, TimeUnit.MILLISECONDS);

    @Test
    public void uniqueSamplingUseDocumentsNumber() throws IndexNotFoundKernelException
    {
        when( indexSearcher.getIndexReader().numDocs() ).thenReturn( 17 );

        UniqueLuceneIndexSampler luceneIndexSampler = new UniqueLuceneIndexSampler( indexSearcher, taskControl.newInstance() );
        long sample = luceneIndexSampler.sampleIndex( Registers.newDoubleLongRegister() );
        assertEquals(17, sample);
    }

    @Test
    public void uniqueSamplingCancel() throws IndexNotFoundKernelException
    {
        when( indexSearcher.getIndexReader().numDocs() ).thenAnswer( invocation -> {
            taskControl.cancel();
            return 17;
        } );

        expectedException.expect( IndexNotFoundKernelException.class );
        expectedException.expectMessage( "Index dropped while sampling." );

        UniqueLuceneIndexSampler luceneIndexSampler = new UniqueLuceneIndexSampler( indexSearcher, taskControl.newInstance() );
        luceneIndexSampler.sampleIndex( Registers.newDoubleLongRegister() );
    }

}