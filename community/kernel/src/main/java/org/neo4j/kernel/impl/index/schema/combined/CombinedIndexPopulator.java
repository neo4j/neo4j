/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.combined;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.neo4j.kernel.impl.index.schema.combined.CombinedSchemaIndexProvider.combineSamples;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedSchemaIndexProvider.select;

class CombinedIndexPopulator implements IndexPopulator
{
    private final IndexPopulator boostPopulator;
    private final IndexPopulator fallbackPopulator;

    CombinedIndexPopulator( IndexPopulator boostPopulator, IndexPopulator fallbackPopulator )
    {
        this.boostPopulator = boostPopulator;
        this.fallbackPopulator = fallbackPopulator;
    }

    @Override
    public void create() throws IOException
    {
        boostPopulator.create();
        fallbackPopulator.create();
    }

    @Override
    public void drop() throws IOException
    {
        try
        {
            boostPopulator.drop();
        }
        finally
        {
            fallbackPopulator.drop();
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            add( update );
        }
    }

    @Override
    public void add( IndexEntryUpdate<?> update ) throws IndexEntryConflictException, IOException
    {
        select( update.values(), boostPopulator, fallbackPopulator ).add( update );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        boostPopulator.verifyDeferredConstraints( propertyAccessor );
        fallbackPopulator.verifyDeferredConstraints( propertyAccessor );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return new CombinedIndexUpdater(
                boostPopulator.newPopulatingUpdater( accessor ),
                fallbackPopulator.newPopulatingUpdater( accessor ) );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        try
        {
            boostPopulator.close( populationCompletedSuccessfully );
        }
        finally
        {
            fallbackPopulator.close( populationCompletedSuccessfully );
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        try
        {
            boostPopulator.markAsFailed( failure );
        }
        finally
        {
            fallbackPopulator.markAsFailed( failure );
        }
    }

    @Override
    public void includeSample( IndexEntryUpdate update )
    {
        boostPopulator.includeSample( update );
        fallbackPopulator.includeSample( update );
    }

    @Override
    public void configureSampling( boolean onlineSampling )
    {
        boostPopulator.configureSampling( onlineSampling );
        fallbackPopulator.configureSampling( onlineSampling );
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples( boostPopulator.sampleResult(), fallbackPopulator.sampleResult() );
    }
}
