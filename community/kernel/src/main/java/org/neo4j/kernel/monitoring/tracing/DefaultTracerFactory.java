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
package org.neo4j.kernel.monitoring.tracing;

import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.DefaultTransactionTracer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DefaultCheckPointerTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.time.SystemNanoClock;

/**
 * The default TracerFactory, when nothing else is otherwise configured.
 */
public class DefaultTracerFactory implements TracerFactory
{
    @Override
    public String getImplementationName()
    {
        return "default";
    }

    @Override
    public PageCacheTracer createPageCacheTracer( Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock,
            Log log )
    {
        return new DefaultPageCacheTracer();
    }

    @Override
    public TransactionTracer createTransactionTracer( Monitors monitors, JobScheduler jobScheduler )
    {
        DefaultTransactionTracer.Monitor monitor = monitors.newMonitor( DefaultTransactionTracer.Monitor.class );
        return new DefaultTransactionTracer( monitor, jobScheduler );
    }

    @Override
    public CheckPointTracer createCheckPointTracer( Monitors monitors, JobScheduler jobScheduler )
    {
        DefaultCheckPointerTracer.Monitor monitor = monitors.newMonitor( DefaultCheckPointerTracer.Monitor.class );
        return new DefaultCheckPointerTracer( monitor, jobScheduler );
    }
}
