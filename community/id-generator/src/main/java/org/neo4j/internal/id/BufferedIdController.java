/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.id;

import java.util.function.Supplier;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Storage id controller that provide buffering possibilities to be able so safely free and reuse ids.
 * Allows perform clear and maintenance operations over currently buffered set of ids.
 * @see BufferingIdGeneratorFactory
 */
public class BufferedIdController extends LifecycleAdapter implements IdController
{
    private static final String BUFFERED_ID_CONTROLLER = "idController";
    private final BufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private final JobScheduler scheduler;
    private final PageCacheTracer pageCacheTracer;
    private final String databaseName;
    private JobHandle<?> jobHandle;

    public BufferedIdController( BufferingIdGeneratorFactory bufferingIdGeneratorFactory, JobScheduler scheduler, PageCacheTracer pageCacheTracer,
            String databaseName )
    {
        this.bufferingIdGeneratorFactory = bufferingIdGeneratorFactory;
        this.scheduler = scheduler;
        this.pageCacheTracer = pageCacheTracer;
        this.databaseName = databaseName;
    }

    @Override
    public void start()
    {
        var monitoringParams = JobMonitoringParams.systemJob( databaseName, "ID generator maintenance" );
        jobHandle = scheduler.scheduleRecurring( Group.STORAGE_MAINTENANCE, monitoringParams, () -> maintenance( false ), 1, SECONDS );
    }

    @Override
    public void stop()
    {
        if ( jobHandle != null )
        {
            jobHandle.cancel();
            jobHandle = null;
        }
    }

    @Override
    public void clear()
    {
        bufferingIdGeneratorFactory.clear();
    }

    @Override
    public void maintenance( boolean awaitOngoing )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( BUFFERED_ID_CONTROLLER ) )
        {
            bufferingIdGeneratorFactory.maintenance( awaitOngoing, cursorTracer );
        }
    }

    @Override
    public void initialize( Supplier<ConditionSnapshot> conditionSnapshotSupplier )
    {
        bufferingIdGeneratorFactory.initialize( conditionSnapshotSupplier );
    }
}
