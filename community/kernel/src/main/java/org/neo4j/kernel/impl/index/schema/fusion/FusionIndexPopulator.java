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
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.combineSamples;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class FusionIndexPopulator implements IndexPopulator
{
    private final IndexPopulator stringPopulator;
    private final IndexPopulator numberPopulator;
    private final IndexPopulator spatialPopulator;
    private final IndexPopulator temporalPopulator;
    private final IndexPopulator lucenePopulator;
    private final IndexPopulator[] populators;
    private final Selector selector;
    private final long indexId;
    private final DropAction dropAction;

    FusionIndexPopulator( IndexPopulator stringPopulator, IndexPopulator numberPopulator, IndexPopulator spatialPopulator, IndexPopulator temporalPopulator,
            IndexPopulator lucenePopulator, Selector selector, long indexId, DropAction dropAction )
    {
        this.stringPopulator = stringPopulator;
        this.numberPopulator = numberPopulator;
        this.spatialPopulator = spatialPopulator;
        this.temporalPopulator = temporalPopulator;
        this.lucenePopulator = lucenePopulator;
        this.populators = array( stringPopulator, numberPopulator, spatialPopulator, temporalPopulator, lucenePopulator );
        this.selector = selector;
        this.indexId = indexId;
        this.dropAction = dropAction;
    }

    @Override
    public void create() throws IOException
    {
        FusionIndexUtils.forAll( IndexPopulator::create, populators );
    }

    @Override
    public void drop() throws IOException
    {
        forAll( IndexPopulator::drop, populators );
        dropAction.drop( indexId );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<?>> stringBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> numberBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> spatialBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> temporalBatch = new ArrayList<>();
        Collection<IndexEntryUpdate<?>> luceneBatch = new ArrayList<>();
        for ( IndexEntryUpdate<?> update : updates )
        {
            selector.select( stringBatch, numberBatch, spatialBatch, temporalBatch, luceneBatch, update.values() ).add( update );
        }
        stringPopulator.add( stringBatch );
        numberPopulator.add( numberBatch );
        spatialPopulator.add( spatialBatch );
        temporalPopulator.add( temporalBatch );
        lucenePopulator.add( luceneBatch );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        for ( IndexPopulator populator : populators )
        {
            populator.verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return new FusionIndexUpdater(
                stringPopulator.newPopulatingUpdater( accessor ),
                numberPopulator.newPopulatingUpdater( accessor ),
                spatialPopulator.newPopulatingUpdater( accessor ),
                temporalPopulator.newPopulatingUpdater( accessor ),
                lucenePopulator.newPopulatingUpdater( accessor ), selector );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        forAll( populator -> populator.close( populationCompletedSuccessfully ), populators );
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        forAll( populator -> populator.markAsFailed( failure ), populators );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        selector.select( stringPopulator, numberPopulator, spatialPopulator, temporalPopulator, lucenePopulator, update.values() ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples(
                stringPopulator.sampleResult(),
                numberPopulator.sampleResult(),
                spatialPopulator.sampleResult(),
                temporalPopulator.sampleResult(),
                lucenePopulator.sampleResult() );
    }
}
