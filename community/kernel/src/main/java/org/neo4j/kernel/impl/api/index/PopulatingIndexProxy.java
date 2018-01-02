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
package org.neo4j.kernel.impl.api.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.indexPopulation;


public class PopulatingIndexProxy implements IndexProxy
{
    private final JobScheduler scheduler;
    private final IndexDescriptor descriptor;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final IndexPopulationJob job;
    private final IndexConfiguration configuration;

    public PopulatingIndexProxy( JobScheduler scheduler,
                                 IndexDescriptor descriptor,
                                 IndexConfiguration configuration,
                                 FailedIndexProxyFactory failureDelegateFactory,
                                 IndexPopulator writer,
                                 FlippableIndexProxy flipper,
                                 IndexStoreView storeView,
                                 UpdateableSchemaState updateableSchemaState,
                                 LogProvider logProvider,
                                 String indexUserDescription,
                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                 IndexingService.Monitor monitor )
    {
        this.scheduler = scheduler;
        this.descriptor = descriptor;
        this.configuration = configuration;
        this.providerDescriptor = providerDescriptor;
        this.job = new IndexPopulationJob( descriptor, configuration, providerDescriptor,
                indexUserDescription, failureDelegateFactory, writer, flipper, storeView,
                updateableSchemaState, logProvider, monitor );
    }

    @Override
    public void start()
    {
        scheduler.schedule( indexPopulation, job );
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        switch ( mode )
        {
            case ONLINE:
            case BATCHED:
                return new PopulatingIndexUpdater()
                {
                    @Override
                    public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
                    {
                        job.update( update );
                    }
                };
            default:
                return new PopulatingIndexUpdater()
                {
                    @Override
                    public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
                    {
                        throw new ThisShouldNotHappenError( "Stefan", "Unsupported IndexUpdateMode" );
                    }
                };
        }
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
        // Ignored... this isn't controlled from the outside while we're populating the index.
    }

    @Override
    public void flush() throws IOException
    {
        // Ignored... this isn't controlled from the outside while we're populating the index.
    }

    @Override
    public Future<Void> close()
    {
        return job.cancel();
    }

    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        throw new IndexNotFoundKernelException( "Index is still populating: " + job );
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
        throw new IllegalStateException( "Cannot activate index while it is still populating: " + job );
    }

    @Override
    public void validate()
    {
        throw new IllegalStateException( "Cannot validate index while it is still populating: " + job );
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
    public IndexConfiguration config()
    {
        return configuration;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[job:" + job + "]";
    }

    private abstract class PopulatingIndexUpdater implements IndexUpdater
    {
        @Override
        public Reservation validate( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexCapacityExceededException
        {
            return Reservation.EMPTY;
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds )
        {
            throw new UnsupportedOperationException( "Should not remove() from populating index." );
        }
    }
}
