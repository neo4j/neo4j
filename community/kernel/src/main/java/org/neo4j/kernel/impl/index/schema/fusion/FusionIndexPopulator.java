/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.combineSamples;

class FusionIndexPopulator implements IndexPopulator
{
    private final IndexPopulator nativePopulator;
    private final IndexPopulator spatialPopulator;
    private final IndexPopulator lucenePopulator;
    private final Selector selector;
    private final long indexId;
    private final DropAction dropAction;

    FusionIndexPopulator( IndexPopulator nativePopulator, IndexPopulator spatialPopulator,
            IndexPopulator lucenePopulator, Selector selector, long indexId, DropAction dropAction )
    {
        this.nativePopulator = nativePopulator;
        this.spatialPopulator = spatialPopulator;
        this.lucenePopulator = lucenePopulator;
        this.selector = selector;
        this.indexId = indexId;
        this.dropAction = dropAction;
    }

    @Override
    public void create() throws IOException
    {
        nativePopulator.create();
        spatialPopulator.create();
        lucenePopulator.create();
    }

    @Override
    public void drop() throws IOException
    {
        forAll( IndexPopulator::drop, nativePopulator, spatialPopulator, lucenePopulator );
        dropAction.drop( indexId );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<?>> luceneBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> spatialBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> nativeBatch = new ArrayList<>();
        for ( IndexEntryUpdate<?> update : updates )
        {
            selector.select( nativeBatch, spatialBatch, luceneBatch, update.values() ).add( update );
        }
        lucenePopulator.add( luceneBatch );
        spatialPopulator.add( spatialBatch );
        nativePopulator.add( nativeBatch );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        nativePopulator.verifyDeferredConstraints( propertyAccessor );
        spatialPopulator.verifyDeferredConstraints( propertyAccessor );
        lucenePopulator.verifyDeferredConstraints( propertyAccessor );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return new FusionIndexUpdater(
                nativePopulator.newPopulatingUpdater( accessor ),
                spatialPopulator.newPopulatingUpdater( accessor ),
                lucenePopulator.newPopulatingUpdater( accessor ), selector );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        forAll( populator -> populator.close( populationCompletedSuccessfully ), nativePopulator, spatialPopulator, lucenePopulator );
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        forAll( populator -> populator.markAsFailed( failure ), nativePopulator, spatialPopulator, lucenePopulator );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        selector.select( nativePopulator, spatialPopulator, lucenePopulator, update.values() ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples( nativePopulator.sampleResult(), spatialPopulator.sampleResult() , lucenePopulator.sampleResult() );
    }
}
