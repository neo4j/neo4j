package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

public class FulltextIndexPopulator implements IndexPopulator
{
    public FulltextIndexPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
    }

    @Override
    public void create() throws IOException
    {

    }

    @Override
    public void drop() throws IOException
    {

    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {

    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return null;
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {

    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {

    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {

    }

    @Override
    public IndexSample sampleResult()
    {
        return null;
    }
}
