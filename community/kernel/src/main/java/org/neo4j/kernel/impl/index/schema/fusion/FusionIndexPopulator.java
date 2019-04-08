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

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.IndexFiles;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;

class FusionIndexPopulator extends FusionIndexBase<IndexPopulator> implements IndexPopulator
{
    private final boolean archiveFailedIndex;
    private final IndexFiles indexFiles;

    FusionIndexPopulator( SlotSelector slotSelector, InstanceSelector<IndexPopulator> instanceSelector, long indexId, FileSystemAbstraction fs,
            IndexDirectoryStructure directoryStructure, boolean archiveFailedIndex )
    {
        super( slotSelector, instanceSelector );
        this.archiveFailedIndex = archiveFailedIndex;
        this.indexFiles = new IndexFiles.Directory( fs, directoryStructure, indexId );
    }

    @Override
    public void create()
    {
        if ( archiveFailedIndex )
        {
            indexFiles.archiveIndex();
        }
        indexFiles.clear();
        instanceSelector.forAll( IndexPopulator::create );
    }

    @Override
    public void drop()
    {
        instanceSelector.forAll( IndexPopulator::drop );
        indexFiles.clear();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        LazyInstanceSelector<Collection<IndexEntryUpdate<?>>> batchSelector = new LazyInstanceSelector<>( slot -> new ArrayList<>() );
        for ( IndexEntryUpdate<?> update : updates )
        {
            batchSelector.select( slotSelector.selectSlot( update.values(), CATEGORY_OF ) ).add( update );
        }

        // Manual loop due do multiple exception types
        for ( IndexSlot slot : IndexSlot.values() )
        {
            Collection<IndexEntryUpdate<?>> batch = batchSelector.getIfInstantiated( slot );
            if ( batch != null )
            {
                this.instanceSelector.select( slot ).add( batch );
            }
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        // Manual loop due do multiple exception types
        for ( IndexSlot slot : IndexSlot.values() )
        {
            instanceSelector.select( slot ).verifyDeferredConstraints( nodePropertyAccessor );
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        LazyInstanceSelector<IndexUpdater> updaterSelector =
                new LazyInstanceSelector<>( slot -> instanceSelector.select( slot ).newPopulatingUpdater( accessor ) );
        return new FusionIndexUpdater( slotSelector, updaterSelector );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        instanceSelector.close( populator -> populator.close( populationCompletedSuccessfully ) );
    }

    @Override
    public void markAsFailed( String failure )
    {
        instanceSelector.forAll( populator -> populator.markAsFailed( failure ) );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        instanceSelector.select( slotSelector.selectSlot( update.values(), CATEGORY_OF ) ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples( instanceSelector.transform( IndexPopulator::sampleResult ) );
    }

    @Override
    public void scanCompleted( PhaseTracker phaseTracker ) throws IndexEntryConflictException
    {
        instanceSelector.throwingForAll( ip -> ip.scanCompleted( phaseTracker ) );
    }
}
