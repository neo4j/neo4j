/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ObjLongConsumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.configuration.Config;

public class ConfigurableIOLimiter implements IOLimiter
{
    private static final AtomicLongFieldUpdater<ConfigurableIOLimiter> stateUpdater =
            AtomicLongFieldUpdater.newUpdater( ConfigurableIOLimiter.class, "state" );

    private static final int NO_LIMIT = 0;
    private static final int QUANTUM_MILLIS = 100;
    private static final int TIME_BITS = 32;
    private static final long TIME_MASK = (1L << TIME_BITS) - 1;
    private static final int QUANTUMS_PER_SECOND = (int) (TimeUnit.SECONDS.toMillis( 1 ) / QUANTUM_MILLIS);

    private final ObjLongConsumer<Object> pauseNanos;

    /**
     * Upper 32 bits is the "disabled counter", lower 32 bits is the "IOs per quantum" field.
     * The "disabled counter" is modified online in 2-increments, leaving the lowest bit for signalling when
     * the limiter disabled by configuration.
     */
    @SuppressWarnings( "unused" ) // Updated via stateUpdater
    private volatile long state;

    public ConfigurableIOLimiter( Config config )
    {
        this( config, LockSupport::parkNanos );
    }

    // Only visible for testing.
    ConfigurableIOLimiter( Config config, ObjLongConsumer<Object> pauseNanos )
    {
        this.pauseNanos = pauseNanos;
        Integer iops = config.get( GraphDatabaseSettings.check_point_iops_limit );
        updateConfiguration( iops );
        config.registerDynamicUpdateListener( GraphDatabaseSettings.check_point_iops_limit,
                ( prev, update ) -> updateConfiguration( update ) );
    }

    private void updateConfiguration( Integer iops )
    {
        long oldState;
        long newState;
        if ( iops == null || iops < 1 )
        {
            do
            {
                oldState = stateUpdater.get( this );
                int disabledCounter = getDisabledCounter( oldState );
                disabledCounter |= 1; // Raise the "permanently disabled" bit.
                newState = composeState( disabledCounter, NO_LIMIT );
            }
            while ( !stateUpdater.compareAndSet( this, oldState, newState ) );
        }
        else
        {
            do
            {
                oldState = stateUpdater.get( this );
                int disabledCounter = getDisabledCounter( oldState );
                disabledCounter &= 0xFFFFFFFE; // Mask off "permanently disabled" bit.
                int iopq = iops / QUANTUMS_PER_SECOND;
                newState = composeState( disabledCounter, iopq );
            }
            while ( !stateUpdater.compareAndSet( this, oldState, newState ) );
        }
    }

    private long composeState( int disabledCounter, int iopq )
    {
        return ((long) disabledCounter) << 32 | iopq;
    }

    private int getIOPQ( long state )
    {
        return (int) (state & 0x00000000_FFFFFFFFL);
    }

    private int getDisabledCounter( long state )
    {
        return (int) (state >>> 32);
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
    public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable )
    {
        long state = stateUpdater.get( this );
        if ( getDisabledCounter( state ) > 0 )
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
        if ( ioSum >= getIOPQ( state ) )
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
        long currentState;
        long newState;
        do
        {
            currentState = stateUpdater.get( this );
            // Increment by two to leave "permanently disabled bit" alone.
            int disabledCounter = getDisabledCounter( currentState ) + 2;
            newState = composeState( disabledCounter, getIOPQ( currentState ) );
        }
        while ( !stateUpdater.compareAndSet( this, currentState, newState ) );
    }

    @Override
    public void enableLimit()
    {
        long currentState;
        long newState;
        do
        {
            currentState = stateUpdater.get( this );
            // Decrement by two to leave "permanently disabled bit" alone.
            int disabledCounter = getDisabledCounter( currentState ) - 2;
            newState = composeState( disabledCounter, getIOPQ( currentState ) );
        }
        while ( !stateUpdater.compareAndSet( this, currentState, newState ) );
    }

    private long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }
}
