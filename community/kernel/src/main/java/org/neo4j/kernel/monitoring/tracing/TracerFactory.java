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
package org.neo4j.kernel.monitoring.tracing;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.time.SystemNanoClock;

/**
 * A TracerFactory determines the implementation of the tracers, that a database should use. Each implementation has
 * a particular name, which is given by the getImplementationName method, and is used for identifying it in the
 * {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration#tracer} setting.
 */
public interface TracerFactory
{
    /**
     * @return The name this implementation is identified by in the
     * {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration#tracer} setting.
     */
    String getImplementationName();

    /**
     * Create a new PageCacheTracer instance.
     *
     * @param monitors the monitoring manager
     * @param jobScheduler a scheduler for async jobs
     * @param clock system nano clock
     * @param log log
     * @return The created instance.
     */
    PageCacheTracer createPageCacheTracer( Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock, Log log );

    /**
     * Create a new TransactionTracer instance.
     *
     * @param monitors the monitoring manager
     * @param jobScheduler a scheduler for async jobs
     * @return The created instance.
     */
    TransactionTracer createTransactionTracer( Monitors monitors, JobScheduler jobScheduler );

    /**
     * Create a new CheckPointTracer instance.
     *
     * @param monitors the monitoring manager
     * @param jobScheduler a scheduler for async jobs
     * @return The created instance.
     */
    CheckPointTracer createCheckPointTracer( Monitors monitors, JobScheduler jobScheduler );

    /**
     * Create a new LockTracer instance.
     *
     * @param monitors the monitoring manager
     * @param jobScheduler a scheduler for async jobs
     * @return The created instance.
     */
    default LockTracer createLockTracer( Monitors monitors, JobScheduler jobScheduler )
    {
        return LockTracer.NONE;
    }

    /**
     * Create a new PageCursorTracerSupplier instance.
     * @param monitors the monitoring manager
     * @param jobScheduler a scheduler for async jobs
     * @return The created instance.
     */
    default PageCursorTracerSupplier createPageCursorTracerSupplier( Monitors monitors, JobScheduler jobScheduler )
    {
        return DefaultPageCursorTracerSupplier.INSTANCE;
    }
}
