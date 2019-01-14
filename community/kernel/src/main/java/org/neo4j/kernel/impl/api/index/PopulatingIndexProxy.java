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
package org.neo4j.kernel.impl.api.index;

import java.io.File;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;

public class PopulatingIndexProxy implements IndexProxy
{
    private final IndexMeta indexMeta;
    private final IndexPopulationJob job;
    private final MultipleIndexPopulator.IndexPopulation indexPopulation;

    PopulatingIndexProxy( IndexMeta indexMeta, IndexPopulationJob job, MultipleIndexPopulator.IndexPopulation indexPopulation )
    {
        this.indexMeta = indexMeta;
        this.job = job;
        this.indexPopulation = indexPopulation;
    }

    @Override
    public void start()
    {
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        switch ( mode )
        {
            case ONLINE:
            case RECOVERY:
                return new PopulatingIndexUpdater()
                {
                    @Override
                    public void process( IndexEntryUpdate<?> update )
                    {
                        job.update( update );
                    }
                };
            default:
                return new PopulatingIndexUpdater()
                {
                    @Override
                    public void process( IndexEntryUpdate<?> update )
                    {
                        throw new IllegalArgumentException( "Unsupported update mode: " + mode );
                    }
                };
        }
    }

    @Override
    public void drop()
    {
        job.cancelPopulation( indexPopulation );
    }

    @Override
    public SchemaIndexDescriptor getDescriptor()
    {
        return indexMeta.indexDescriptor();
    }

    @Override
    public SchemaDescriptor schema()
    {
        return indexMeta.indexDescriptor().schema();
    }

    @Override
    public IndexProvider.Descriptor getProviderDescriptor()
    {
        return indexMeta.providerDescriptor();
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.POPULATING;
    }

    @Override
    public IndexCapability getIndexCapability()
    {
        return indexMeta.indexCapability();
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        // Ignored... this isn't called from the outside while we're populating the index.
    }

    @Override
    public void refresh()
    {
        // Ignored... this isn't called from the outside while we're populating the index.
    }

    @Override
    public void close()
    {
        job.cancelPopulation( indexPopulation );
    }

    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        throw new IndexNotFoundKernelException( "Index is still populating: " + job );
    }

    @Override
    public boolean awaitStoreScanCompleted() throws InterruptedException
    {
        job.awaitCompletion();
        return true;
    }

    @Override
    public void activate()
    {
        throw new IllegalStateException( "Cannot activate index while it is still populating: " + job );
    }

    @Override
    public void validate()
    {
        throw new IllegalStateException( "Cannot validate index while it is still populating: " + job );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        // It's OK to put whatever values in while populating because it will take the natural path of failing the population.
    }

    @Override
    public long getIndexId()
    {
        return indexMeta.getIndexId();
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyResourceIterator();
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( this + " is POPULATING" );
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        return job.getPopulationProgress();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[job:" + job + "]";
    }

    private abstract class PopulatingIndexUpdater implements IndexUpdater
    {
        @Override
        public void close()
        {
        }
    }
}
