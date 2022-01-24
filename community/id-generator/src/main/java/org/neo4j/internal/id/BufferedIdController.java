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
package org.neo4j.internal.id;

import java.util.function.Supplier;

import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
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
    private final CursorContextFactory contextFactory;
    private final String databaseName;
    private JobHandle<?> jobHandle;

    public BufferedIdController( BufferingIdGeneratorFactory bufferingIdGeneratorFactory, JobScheduler scheduler, CursorContextFactory contextFactory,
            String databaseName )
    {
        this.bufferingIdGeneratorFactory = bufferingIdGeneratorFactory;
        this.scheduler = scheduler;
        this.contextFactory = contextFactory;
        this.databaseName = databaseName;
    }

    @Override
    public void start()
    {
        var monitoringParams = JobMonitoringParams.systemJob( databaseName, "ID generator maintenance" );
        jobHandle = scheduler.scheduleRecurring( Group.STORAGE_MAINTENANCE, monitoringParams, this::maintenance, 1, SECONDS );
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
    public void maintenance()
    {
        try ( var cursorContext = contextFactory.create( BUFFERED_ID_CONTROLLER ) )
        {
            bufferingIdGeneratorFactory.maintenance( cursorContext );
        }
    }

    @Override
    public void initialize( Supplier<IdFreeCondition> conditionSupplier, MemoryTracker memoryTracker )
    {
        bufferingIdGeneratorFactory.initialize( conditionSupplier, memoryTracker );
    }
}
