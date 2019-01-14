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
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;

class FusionIndexPopulator extends FusionIndexBase<IndexPopulator> implements IndexPopulator
{
    private final long indexId;
    private final DropAction dropAction;
    private final boolean archiveFailedIndex;

    FusionIndexPopulator( SlotSelector slotSelector, InstanceSelector<IndexPopulator> instanceSelector, long indexId, DropAction dropAction,
            boolean archiveFailedIndex )
    {
        super( slotSelector, instanceSelector );
        this.indexId = indexId;
        this.dropAction = dropAction;
        this.archiveFailedIndex = archiveFailedIndex;
    }

    @Override
    public void create() throws IOException
    {
        dropAction.drop( indexId, archiveFailedIndex );
        instanceSelector.forAll( IndexPopulator::create );
    }

    @Override
    public void drop() throws IOException
    {
        instanceSelector.forAll( IndexPopulator::drop );
        dropAction.drop( indexId );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        LazyInstanceSelector<Collection<IndexEntryUpdate<?>>> batchSelector =
                new LazyInstanceSelector<>( new Collection[INSTANCE_COUNT], slot -> new ArrayList<>() );
        for ( IndexEntryUpdate<?> update : updates )
        {
            batchSelector.select( slotSelector.selectSlot( update.values(), GROUP_OF ) ).add( update );
        }

        // Manual loop due do multiple exception types
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            Collection<IndexEntryUpdate<?>> batch = batchSelector.getIfInstantiated( slot );
            if ( batch != null )
            {
                this.instanceSelector.select( slot ).add( batch );
            }
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        // Manual loop due do multiple exception types
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            instanceSelector.select( slot ).verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        LazyInstanceSelector<IndexUpdater> updaterSelector =
                new LazyInstanceSelector<>( new IndexUpdater[INSTANCE_COUNT], slot -> instanceSelector.select( slot ).newPopulatingUpdater( accessor ) );
        return new FusionIndexUpdater( slotSelector, updaterSelector );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        instanceSelector.close( populator -> populator.close( populationCompletedSuccessfully ) );
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        instanceSelector.forAll( populator -> populator.markAsFailed( failure ) );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        instanceSelector.select( slotSelector.selectSlot( update.values(), GROUP_OF ) ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples( instanceSelector.instancesAs( new IndexSample[INSTANCE_COUNT], IndexPopulator::sampleResult ) );
    }
}
