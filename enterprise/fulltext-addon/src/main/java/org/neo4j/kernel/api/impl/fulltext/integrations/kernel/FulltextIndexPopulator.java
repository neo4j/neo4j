package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.fulltext.lucene.FulltextProviderImpl;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

public class FulltextIndexPopulator implements IndexPopulator
{
    private final long indexId;
    private final FulltextIndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private FulltextProviderImpl oldProvider;

    public FulltextIndexPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, FulltextProviderImpl oldProvider )
    {
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.oldProvider = oldProvider;
    }

    @Override
    public void create() throws IOException
    {
        //TODO remove old provider
        oldProvider.createIndex( descriptor );
    }

    @Override
    public void drop() throws IOException
    {
        throw new UnsupportedOperationException( "not implemented" );

    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        updates.forEach( System.out::println );
        //Ingore for now
//        throw new UnsupportedOperationException( "not implemented" );
//        for ( IndexEntryUpdate<?> update : updates )
//        {
//            update.updateMode()
//        }

    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        //TODO whatever
        System.out.println("close population");

    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        throw new UnsupportedOperationException( "not implemented" );

    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public IndexSample sampleResult()
    {
        return new IndexSample();
    }
}
