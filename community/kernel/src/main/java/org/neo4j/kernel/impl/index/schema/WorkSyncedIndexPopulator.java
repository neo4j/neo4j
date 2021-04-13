/*
 * Copyright (c) "Neo4j"
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
import java.util.concurrent.ExecutionException;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

/**
 * Delegating populator that turns multi-threaded calls to {@link IndexPopulator#add(Collection, PageCursorTracer)} into single-threaded stack work
 * by passing them through {@link WorkSync}.
 *
 * Used to wrap {@link IndexPopulator}s that are not thread-safe in terms of {@link IndexPopulator#add(Collection, PageCursorTracer)} operation.
 */
public class WorkSyncedIndexPopulator extends IndexPopulator.Delegating
{
    private final WorkSync<IndexUpdateApply,IndexUpdateWork> workSync = new WorkSync<>( new IndexUpdateApply() );

    public WorkSyncedIndexPopulator( IndexPopulator delegate )
    {
        super( delegate );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates, PageCursorTracer cursorTracer ) throws IndexEntryConflictException
    {
        if ( updates.isEmpty() )
        {
            return;
        }

        try
        {
            workSync.apply( new IndexUpdateWork( updates, cursorTracer ) );
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

    private class IndexUpdateApply
    {
        void process( Collection<? extends IndexEntryUpdate<?>> indexEntryUpdates, PageCursorTracer cursorTracer ) throws Exception
        {
            WorkSyncedIndexPopulator.super.add( indexEntryUpdates, cursorTracer );
        }
    }

    private class IndexUpdateWork implements Work<IndexUpdateApply,IndexUpdateWork>
    {
        private final Collection<? extends IndexEntryUpdate<?>> updates;
        private final PageCursorTracer cursorTracer;

        IndexUpdateWork( Collection<? extends IndexEntryUpdate<?>> updates, PageCursorTracer cursorTracer )
        {
            this.updates = updates;
            this.cursorTracer = cursorTracer;
        }

        @Override
        public IndexUpdateWork combine( IndexUpdateWork work )
        {
            ArrayList<IndexEntryUpdate<?>> combined = new ArrayList<>( updates );
            combined.addAll( work.updates );
            return new IndexUpdateWork( combined, cursorTracer );
        }

        @Override
        public void apply( IndexUpdateApply indexUpdateApply ) throws Exception
        {
            indexUpdateApply.process( updates, cursorTracer );
        }
    }
}
