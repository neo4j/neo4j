/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ObjLongConsumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointFlushControl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.Rush;

public class ConfigurableCheckPointFlushControl implements CheckPointFlushControl, IOLimiter
{
    private static final AtomicIntegerFieldUpdater<ConfigurableCheckPointFlushControl> rushUpdater =
            AtomicIntegerFieldUpdater.newUpdater( ConfigurableCheckPointFlushControl.class, "rushCount" );

    private static final int NO_LIMIT = 0;
    private static final int QUANTUM_MILLIS = 100;
    private static final int TIME_BITS = 32;
    private static final long TIME_MASK = (1L << TIME_BITS) - 1;

    private final int iopq; // IOs per quantum
    private final ObjLongConsumer<Object> pauseNanos;
    private final Rush rush;

    @SuppressWarnings( "unused" ) // Updated via rushUpdater
    private volatile int rushCount;

    public ConfigurableCheckPointFlushControl( Config config )
    {
        this( config, LockSupport::parkNanos );
    }

    // Only visible for testing
    ConfigurableCheckPointFlushControl( Config config, ObjLongConsumer<Object> pauseNanos )
    {
        this.pauseNanos = pauseNanos;
        int quantumsPerSecond = (int) (TimeUnit.SECONDS.toMillis( 1 ) / QUANTUM_MILLIS);
        Integer iops = config.get( GraphDatabaseSettings.check_point_iops_limit );
        this.iopq = iops == null || iops < 1? NO_LIMIT : (iops / quantumsPerSecond);
        rush = () -> rushUpdater.getAndDecrement( this );
    }

    @Override
    public IOLimiter getIOLimiter()
    {

        return iopq == NO_LIMIT? IOLimiter.unlimited() : this;
    }

    @Override
    public Rush beginTemporaryRush()
    {
        rushUpdater.getAndIncrement( this );
        return rush;
    }

    // The stamp is in two 32-bit parts:
    // The high bits are the number of IOs performed since the last pause.
    // The low bits is the 32-bit timestamp in milliseconds (~25 day range) since the last pause.
    // We keep adding summing up the IOs until either a quantum elapses, or we've exhausted the IOs we're allowed in
    // this quantum. If we've exhausted our IOs, we pause for the rest of the quantum.
    // We don't make use of the Flushable at this point, because IOs from fsyncs have a high priority, so they
    // might jump the IO queue and cause delays for transaction log IOs. Further, fsync on some file systems also
    // flush the entire IO queue, which can cause delays on IO rate limited cloud machines.
    // We need the Flushable to be implemented in terms of sync_file_range before we can make use of it.

    @Override
    public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable ) throws IOException
    {
        if ( rushCount > 0 )
        {
            return INITIAL_STAMP;
        }

        long now = currentTimeMillis() & TIME_MASK;
        long then = previousStamp & TIME_MASK;

        if ( now - then > QUANTUM_MILLIS )
        {
            return now + (((long) recentlyCompletedIOs) << TIME_BITS);
        }

        long ioSum = (previousStamp >> TIME_BITS) + recentlyCompletedIOs;
        if ( ioSum >= iopq )
        {
            long millisLeftInQuantum = QUANTUM_MILLIS - (now - then);
            pauseNanos.accept( this, TimeUnit.MILLISECONDS.toNanos( millisLeftInQuantum ) );
            return currentTimeMillis() & TIME_MASK;
        }

        return then + (ioSum << TIME_BITS);
    }

    private long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }
}
