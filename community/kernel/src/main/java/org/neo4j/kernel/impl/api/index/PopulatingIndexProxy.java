/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;


public class PopulatingIndexProxy implements IndexProxy
{
    private final JobScheduler scheduler;
    private final IndexDescriptor descriptor;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final IndexPopulationJob job;

    public PopulatingIndexProxy( JobScheduler scheduler,
                                 final IndexDescriptor descriptor,
                                 final SchemaIndexProvider.Descriptor providerDescriptor,
                                 final FailedIndexProxyFactory failureDelegateFactory,
                                 final IndexPopulator writer,
                                 FlippableIndexProxy flipper,
                                 IndexStoreView storeView, final String indexUserDescription,
                                 UpdateableSchemaState updateableSchemaState, Logging logging )
    {
        this.scheduler  = scheduler;
        this.descriptor = descriptor;
        this.providerDescriptor = providerDescriptor;
        this.job  = new IndexPopulationJob( descriptor, providerDescriptor,
                indexUserDescription, failureDelegateFactory, writer, flipper, storeView,
                updateableSchemaState, logging );
    }

    @Override
    public void start()
    {
        scheduler.schedule( job );
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        return new IndexUpdater()
        {
            @Override
            public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
            {
                switch( mode )
                {
                    case ONLINE:
                        job.update( update );
                        break;

                    case RECOVERY:
                        throw new UnsupportedOperationException( "Recovered updates shouldn't reach this place" );

                    default:
                        throw new ThisShouldNotHappenError( "Stefan", "Unsupported IndexUpdateMode" );
                }
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
            }
        };
    }

    @Override
    public Future<Void> drop()
    {
        return job.cancel();
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.POPULATING;
    }
    
    @Override
    public void force()
    {
        // Ignored... I think
    }

    @Override
    public Future<Void> close()
    {
        return job.cancel();
    }
    
    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        throw new IndexNotFoundKernelException( descriptor + " is still populating" );
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException
    {
        job.awaitCompletion();
        return true;
    }

    @Override
    public void activate() throws IndexActivationFailedKernelException
    {
        throw new IllegalStateException( "Cannot activate index while it is still populating." );
    }

    @Override
    public void validate()
    {
        throw new IllegalStateException( "Cannot validate index while it is still populating." );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyIterator();
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( this + " is POPULATING" );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[job:" + job + "]";
    }
}
