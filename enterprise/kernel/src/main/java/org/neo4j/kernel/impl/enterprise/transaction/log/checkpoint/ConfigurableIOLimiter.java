/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

public class ConfigurableIOLimiter implements IOLimiter
{
    private static final AtomicIntegerFieldUpdater<ConfigurableIOLimiter> disableCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater( ConfigurableIOLimiter.class, "disabledCount" );

    private static final int NO_LIMIT = 0;
    private static final int QUANTUM_MILLIS = 100;
    private static final int TIME_BITS = 32;
    private static final long TIME_MASK = (1L << TIME_BITS) - 1;

    private final int iopq; // IOs per quantum
    private final ObjLongConsumer<Object> pauseNanos;

    @SuppressWarnings( "unused" ) // Updated via disableCountUpdater
    private volatile int disabledCount;

    public ConfigurableIOLimiter( Config config )
    {
        this( config, LockSupport::parkNanos );
    }

    // Only visible for testing
    ConfigurableIOLimiter( Config config, ObjLongConsumer<Object> pauseNanos )
    {
        this.pauseNanos = pauseNanos;
        int quantumsPerSecond = (int) (TimeUnit.SECONDS.toMillis( 1 ) / QUANTUM_MILLIS);
        Integer iops = config.get( GraphDatabaseSettings.check_point_iops_limit );
        if ( iops == null || iops < 1 )
        {
            iopq = NO_LIMIT;
            disabledCount = 1; // This permanently disables IO limiting
        }
        else
        {
            this.iopq = iops / quantumsPerSecond;
        }

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
    // NOTE: The check-pointer IOPS setting is documented as being a "best effort" hint. We are making use of that
    // wording here, and not compensating for over-counted IOs. For instance, if we receive 2 quantums worth of IOs
    // in one quantum, we are not going to sleep for two quantums. The reason is that such compensation algorithms
    // can easily over-compensate, and end up sleeping a lot more than what makes sense for other rate limiting factors
    // in the system, thus wasting IO bandwidth. No, "best effort" here means that likely end up doing a little bit
    // more IO than what we've been configured to allow, but that's okay. If it's a problem, people can just reduce
    // their IOPS limit setting a bit more.

    @Override
    public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable ) throws IOException
    {
        if ( disabledCount > 0 )
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

    @Override
    public void disableLimit()
    {
        disableCountUpdater.getAndIncrement( this );
    }

    @Override
    public void enableLimit()
    {
        disableCountUpdater.getAndDecrement( this );
    }

    @Override
    public boolean isLimited()
    {
        return disabledCount == 0;
    }

    private long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }
}
