/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.System.currentTimeMillis;

/**
 * Able to pull updates from a master and apply onto this slave database.
 * <p>
 * Updates are pulled and applied using a single and dedicated thread, created in here. No other threads are allowed to
 * pull and apply transactions on a slave. Calling one of the {@link Master#pullUpdates(RequestContext) pullUpdates}
 * {@link TransactionObligationFulfiller#fulfill(long)} will {@link SlaveUpdatePuller#poke() poke} that single thread,
 * so
 * that it gets going, if not already doing so, with its usual task of pulling updates and the caller which poked
 * the update thread will constantly poll to see if the transactions it is obliged to await have been applied.
 * <p>
 * Here comes a diagram of how the classes making up this functionality hangs together:
 * <p>
 * <pre>
 *
 *             -------- 1 -------------------->({@link MasterImpl master})
 *           /                                   |
 *          |                                   /
 *          | v--------------------- 2 --------
 *       ({@link MasterClient slave})
 *       | ^ \
 *       | |   -------- 3 -----
 *       | \                   \
 *       |  \                   v
 *       |   ---- 8 -----------({@link TransactionCommittingResponseUnpacker response unpacker})
 *       |                        |     ^
 *       9                        |     |
 *    (continue)                  4     7
 *                                |     |
 *                                v     |
 *                             ({@link UpdatePullingTransactionObligationFulfiller obligation fulfiller})
 *                                |     ^
 *                                |     |
 *                                5     6
 *                                |     |
 *                                v     |
 *                            ({@link SlaveUpdatePuller update puller})
 *
 * </pre>
 * <p>
 * In the above picture:
 * <p>
 * <ol>
 * <li>Slave issues request to master, for example for locking an entity</li>
 * <li>Master responds with a {@link TransactionObligationResponse transaction obligation} telling the slave
 * which transaction it must have applied before continuing</li>
 * <li>The response from the master gets unpacked by the response unpacker...</li>
 * <li>...which will be sent to the {@link UpdatePullingTransactionObligationFulfiller obligation fulfiller}...</li>
 * <li>...which will ask the {@link SlaveUpdatePuller update puller}, a separate thread, to have that transaction
 * committed and applied. The calling thread will gently wait for that to happen.</li>
 * <li>The {@link SlaveUpdatePuller update puller} will pull updates until it reaches that desired transaction id,
 * and might actually continue passed that point if master has more transactions. The obligation fulfiller,
 * constantly polling for {@link TransactionIdStore#getLastClosedTransactionId() last applied transaction}
 * will notice when the obligation has been fulfilled.</li>
 * <li>Response unpacker finishes its call to {@link TransactionObligationFulfiller#fulfill(long) fulfill the
 * obligation}</li>
 * <li>Slave has fully received the response and is now able to...</li>
 * <li>...continue</li>
 * </ol>
 * <p>
 * All communication, except actually pulling updates, work this way between slave and master. The only difference
 * in the pullUpdates case is that instead of receiving and fulfilling a transaction obligation,
 * {@link TransactionStream transaction data} is received and applied to store directly, in batches.
 *
 * @see org.neo4j.kernel.ha.UpdatePuller
 */
public class SlaveUpdatePuller extends LifecycleAdapter implements Runnable, UpdatePuller
{
    public interface Monitor
    {
        void pulledUpdates( long lastAppliedTxId );
    }

    public static final int LOG_CAP = Integer.getInteger(
            "org.neo4j.kernel.ha.SlaveUpdatePuller.LOG_CAP", 10 );
    public static final long PARK_NANOS = TimeUnit.MILLISECONDS.toNanos( Integer.getInteger(
            "org.neo4j.kernel.ha.SlaveUpdatePuller.PARK_MILLIS", 100 ) );
    public static final int AVAILABILITY_AWAIT_MILLIS = Integer.getInteger(
            "org.neo4j.kernel.ha.SlaveUpdatePuller.AVAILABILITY_AWAIT_MILLIS", 5000 );
    public static final String UPDATE_PULLER_THREAD_PREFIX = "UpdatePuller@";

    static final Condition NEXT_TICKET = new Condition()
    {
        @Override
        public boolean evaluate( int currentTicket, int targetTicket )
        {
            return currentTicket >= targetTicket;
        }
    };

    private volatile boolean halted;
    private final AtomicInteger targetTicket = new AtomicInteger();
    private final AtomicInteger currentTicket = new AtomicInteger();
    private final RequestContextFactory requestContextFactory;
    private final Master master;
    private final Log logger;
    private final CappedLogger invalidEpochCappedLogger;
    private final CappedLogger comExceptionCappedLogger;
    private final LastUpdateTime lastUpdateTime;
    private final InstanceId instanceId;
    private final AvailabilityGuard availabilityGuard;
    private InvalidEpochExceptionHandler invalidEpochHandler;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;
    private volatile Thread updatePullingThread;
    private volatile BinaryLatch shutdownLatch; // Store under synchronised(this), load in update puller thread

    SlaveUpdatePuller(
            RequestContextFactory requestContextFactory,
            Master master,
            LastUpdateTime lastUpdateTime,
            LogProvider logging,
            InstanceId instanceId,
            AvailabilityGuard availabilityGuard,
            InvalidEpochExceptionHandler invalidEpochHandler,
            JobScheduler jobScheduler,
            Monitor monitor)
    {
        this.requestContextFactory = requestContextFactory;
        this.master = master;
        this.lastUpdateTime = lastUpdateTime;
        this.instanceId = instanceId;
        this.availabilityGuard = availabilityGuard;
        this.invalidEpochHandler = invalidEpochHandler;
        this.jobScheduler = jobScheduler;
        this.monitor = monitor;
        this.logger = logging.getLog( getClass() );
        this.invalidEpochCappedLogger = new CappedLogger( logger ).setCountLimit( LOG_CAP );
        this.comExceptionCappedLogger = new CappedLogger( logger ).setCountLimit( LOG_CAP );
    }

    @Override
    public void run()
    {
        updatePullingThread = Thread.currentThread();
        String oldName = updatePullingThread.getName();
        updatePullingThread.setName( UPDATE_PULLER_THREAD_PREFIX + instanceId );
        try
        {
            periodicallyPullUpdates();
        }
        finally
        {
            updatePullingThread.setName( oldName );
            updatePullingThread = null;
            shutdownLatch.release();
        }
    }

    private void periodicallyPullUpdates()
    {
        while ( !halted )
        {
            int round = targetTicket.get();
            if ( currentTicket.get() < round )
            {
                doPullUpdates();
                currentTicket.set( round );
                continue;
            }

            LockSupport.parkNanos( PARK_NANOS );
        }
    }

    @Override
    public synchronized void init() throws Throwable
    {
        if ( shutdownLatch != null )
        {
            return; // This SlaveUpdatePuller has already been initialised
        }

        shutdownLatch = new BinaryLatch();
        jobScheduler.schedule( JobScheduler.Groups.pullUpdates, this );
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        if ( shutdownLatch == null )
        {
            return; // This SlaveUpdatePuller has already been shut down
        }

        Thread thread = updatePullingThread;
        halted = true;
        LockSupport.unpark( thread );
        shutdownLatch.await();
        shutdownLatch = null;
    }

    @Override
    public void pullUpdates() throws InterruptedException
    {
        if ( !isActive() || !availabilityGuard.isAvailable( AVAILABILITY_AWAIT_MILLIS ) )
        {
            return;
        }

        tryPullUpdates();
    }

    @Override
    public boolean tryPullUpdates() throws InterruptedException
    {
        return await( NEXT_TICKET,
                false /*we're OK with the update puller becoming inactive while we await the condition*/ );
    }

    @Override
    public void pullUpdates( Condition condition, boolean strictlyAssertActive ) throws InterruptedException
    {
        await( condition, strictlyAssertActive );
    }

    /**
     * Gets the update puller going, if it's not already going, and waits for the supplied condition to be
     * fulfilled as part of the update pulling happening.
     *
     * @param condition {@link UpdatePuller.Condition} to wait for.
     * @param strictlyAssertActive if {@code true} then observing an inactive update puller, whether
     * {@link #shutdown() halted}, will throw an {@link IllegalStateException},
     * otherwise if {@code false} just stop waiting and return {@code false}.
     * @return whether or not the condition was met. If {@code strictlyAssertActive} either
     * {@code true} will be returned or exception thrown, if puller became inactive.
     * If {@code !strictlyAssertActive} and puller became inactive then {@code false} is returned.
     * @throws InterruptedException if we were interrupted while awaiting the condition.
     * @throws IllegalStateException if {@code strictlyAssertActive} and the update puller
     * became inactive while awaiting the condition.
     */
    private boolean await( Condition condition, boolean strictlyAssertActive ) throws InterruptedException
    {
        if ( !checkActive( strictlyAssertActive ) )
        {
            return false;
        }

        int ticket = poke();
        while ( !condition.evaluate( currentTicket.get(), ticket ) )
        {
            if ( !checkActive( strictlyAssertActive ) )
            {
                return false;
            }

            Thread.sleep( 1 );
        }
        return true;
    }

    /**
     * @return {@code true} if active, {@code false} if inactive and {@code !strictlyAssertActive}
     * @throws IllegalStateException if inactive and {@code strictlyAssertActive}.
     */
    private boolean checkActive( boolean strictlyAssertActive )
    {
        if ( !isActive() )
        {
            // The update puller is observed as being inactive

            // The caller strictly requires the update puller to be active so should throw exception
            if ( strictlyAssertActive )
            {
                throw new IllegalStateException( this + " is not active" );
            }

            // The caller is OK with ignoring an inactive update puller, so just return
            return false;
        }
        return true;
    }

    private int poke()
    {
        int result = this.targetTicket.incrementAndGet();
        LockSupport.unpark( updatePullingThread );
        return result;
    }

    public boolean isActive()
    {
        return !halted;
    }

    @Override
    public String toString()
    {
        return "UpdatePuller[halted:" + halted + ", current:" + currentTicket + ", target:" + targetTicket + "]";
    }

    private void doPullUpdates()
    {
        try
        {
            RequestContext context = requestContextFactory.newRequestContext();
            try ( Response<Void> ignored = master.pullUpdates( context ) )
            {
                // Updates would be applied as part of response processing
                monitor.pulledUpdates( context.lastAppliedTransaction() );
            }
            invalidEpochCappedLogger.reset();
            comExceptionCappedLogger.reset();
        }
        catch ( InvalidEpochException e )
        {
            invalidEpochHandler.handle();
            invalidEpochCappedLogger.warn( "Pull updates by " + this + " failed at the epoch check", e );
        }
        catch ( ComException e )
        {
            invalidEpochCappedLogger.warn( "Pull updates by " + this + " failed due to network error.", e );
        }
        catch ( Throwable e )
        {
            logger.error( "Pull updates by " + this + " failed", e );
        }
        lastUpdateTime.setLastUpdateTime( currentTimeMillis() );
    }

}
