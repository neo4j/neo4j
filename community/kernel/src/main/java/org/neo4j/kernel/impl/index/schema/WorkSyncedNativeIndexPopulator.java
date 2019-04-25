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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;
import org.neo4j.values.storable.Value;

/**
 * Takes a {@link NativeIndexPopulator}, which is intended for single-threaded population and wraps it in a populator
 * which applies {@link WorkSync} to multi-threaded inserts, making them look like single-threaded inserts.
 *
 * @param <KEY> type of {@link NativeIndexKey}
 * @param <VALUE> type of {@link NativeIndexValue}
 */
class WorkSyncedNativeIndexPopulator<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        implements IndexPopulator, ConsistencyCheckableIndexPopulator
{
    private final NativeIndexPopulator<KEY,VALUE> actual;
    private final WorkSync<IndexUpdateApply,IndexUpdateWork> workSync = new WorkSync<>( new IndexUpdateApply() );

    WorkSyncedNativeIndexPopulator( NativeIndexPopulator<KEY,VALUE> actual )
    {
        this.actual = actual;
    }

    /**
     * Method visible due to the complex nature of the "part" populators in the legacy temporal/spatial implementations.
     * This can go as soon as they disappear.
     */
    NativeIndexPopulator<KEY,VALUE> getActual()
    {
        return actual;
    }

    @Override
    public void create()
    {
        actual.create();
    }

    @Override
    public void drop()
    {
        actual.drop();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        try
        {
            workSync.apply( new IndexUpdateWork( updates ) );
        }
        catch ( ExecutionException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IOException )
            {
                throw new UncheckedIOException( (IOException) cause );
            }
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw new RuntimeException( cause );
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        actual.verifyDeferredConstraints( nodePropertyAccessor );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return actual.newPopulatingUpdater( accessor );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        actual.close( populationCompletedSuccessfully );
    }

    @Override
    public void markAsFailed( String failure )
    {
        actual.markAsFailed( failure );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        actual.includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        return actual.sampleResult();
    }

    @Override
    public void consistencyCheck()
    {
        actual.consistencyCheck();
    }

    @Override
    public void scanCompleted( PhaseTracker phaseTracker ) throws IndexEntryConflictException
    {
        actual.scanCompleted( phaseTracker );
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return actual.indexConfig();
    }

    private class IndexUpdateApply
    {
        void process( Collection<? extends IndexEntryUpdate<?>> indexEntryUpdates ) throws Exception
        {
            actual.add( indexEntryUpdates );
        }
    }

    private class IndexUpdateWork implements Work<IndexUpdateApply,IndexUpdateWork>
    {
        private final Collection<? extends IndexEntryUpdate<?>> updates;

        IndexUpdateWork( Collection<? extends IndexEntryUpdate<?>> updates )
        {
            this.updates = updates;
        }

        @Override
        public IndexUpdateWork combine( IndexUpdateWork work )
        {
            ArrayList<IndexEntryUpdate<?>> combined = new ArrayList<>( updates );
            combined.addAll( work.updates );
            return new IndexUpdateWork( combined );
        }

        @Override
        public void apply( IndexUpdateApply indexUpdateApply ) throws Exception
        {
            indexUpdateApply.process( updates );
        }
    }
}
