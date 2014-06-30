/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import static java.lang.String.format;

/**
 * This is the real master code that executes on a master. The actual
 * communication over network happens in {@link org.neo4j.kernel.ha.com.slave.MasterClient} and
 * {@link MasterServer}.
 */
public class MasterImpl extends LifecycleAdapter implements Master
{
    public interface Monitor
    {
        void initializeTx( RequestContext context );
    }

    public static final int TX_TIMEOUT_ADDITION = 5 * 1000;

    // This is a bridge SPI that MasterImpl requires to function. Eventually this should be split
    // up into many smaller APIs implemented by other services so that this is not needed.
    // This SPI allows MasterImpl to have no direct dependencies, and instead puts those dependencies into the
    // SPI implementation, thus making it easier to test this class by mocking the SPI.
    public interface SPI
    {
        boolean isAccessible();

        IdAllocation allocateIds( IdType idType );

        StoreId storeId();

        long applyPreparedTransaction( TransactionRepresentation preparedTransaction ) throws IOException, org.neo4j.kernel.api.exceptions.TransactionFailureException;

        Integer createRelationshipType( String name );

        Pair<Integer, Long> getMasterIdForCommittedTx( long txId ) throws IOException;

        RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer );

        Response<Void> copyTransactions( String dsName, long startTxId, long endTxId );

        <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter );

        void pushTransaction( int eventIdentifier, long tx, int machineId );

        int getOrCreateLabel( String name );

        int getOrCreateProperty( String name );

        Locks.Client acquireClient();
    }

    public static final int UNFINISHED_TRANSACTION_CLEANUP_DELAY = 1;

    private final SPI spi;
    private final StringLogger msgLog;
    private final Config config;
    private final Monitor monitor;
    private final long epoch;

    private Map<RequestContext, TimestampedLockClient> slaveLockState = new ConcurrentHashMap<>();
    private ScheduledExecutorService staleSlaveReaper;
    private long unfinishedTransactionThresholdMillis;

    public MasterImpl( SPI spi, Monitor monitor, Logging logging, Config config )
    {
        this.spi = spi;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.config = config;
        this.monitor = monitor;
        this.epoch = generateEpoch();
    }

    private long generateEpoch()
    {
        return ( ( ( (long) config.get( ClusterSettings.server_id ).toIntegerIndex() ) ) << 48 ) | System.currentTimeMillis();
    }

    @Override
    public void start() throws Throwable
    {
        this.unfinishedTransactionThresholdMillis = config.get( HaSettings.lock_read_timeout ) + TX_TIMEOUT_ADDITION;
        this.staleSlaveReaper =
                Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "Unfinished transaction reaper" ) );
        this.staleSlaveReaper.scheduleWithFixedDelay( new UnfinishedTransactionReaper(),
                UNFINISHED_TRANSACTION_CLEANUP_DELAY, UNFINISHED_TRANSACTION_CLEANUP_DELAY, TimeUnit.SECONDS );
    }

    @Override
    public void stop()
    {
        staleSlaveReaper.shutdown();
        slaveLockState = null;
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        monitor.initializeTx( context );

        if ( !spi.isAccessible() )
        {
            throw new TransactionFailureException( "Database is currently not available" );
        }
        assertCorrectEpoch( context );

        TimestampedLockClient locks = new TimestampedLockClient( spi.acquireClient() );
        slaveLockState.put( context, locks );
        return packResponse( context, null );
    }

    /**
     * Basically for all public methods call this assertion to verify that the caller meant to call this
     * master. The epoch is the one handed out from {@link #handshake(long, StoreId)}.
     * Exceptions to the above are:
     * o {@link #handshake(long, StoreId)}
     * o {@link #copyStore(RequestContext, StoreWriter)}
     * o {@link #copyTransactions(RequestContext, String, long, long)}
     * o {@link #pullUpdates(RequestContext)}
     *
     * all other methods must have this.
     * @param context the request context containing the epoch the request thinks it's for.
     */
    private void assertCorrectEpoch( RequestContext context )
    {
        if ( this.epoch != context.getEpoch() )
        {
            throw new InvalidEpochException( epoch, context.getEpoch() );
        }
    }

    private <T> Response<T> packResponse( RequestContext context, T response )
    {
        return packResponse( context, response, Predicates.<Long>TRUE() );
    }

    private <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        return spi.packResponse( context, response, filter );
    }

    private Locks.Client getTx( RequestContext txId )
    {
        TimestampedLockClient result = slaveLockState.get( txId );
        if ( result == null )
        {
            throw new TransactionNotPresentOnMasterException( txId );
        }

        // set time stamp to zero so that we don't even try to finish it off
        // if getting old. This is because if the tx is active and old then
        // it means it's waiting for a lock and we cannot do anything about it.
        result.resetTime();
        return result.getLockClient();
    }

    private void finishTransaction0( RequestContext txId )
    {
        try
        {
            getTx( txId ).close();
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
        finally
        {
            slaveLockState.remove( txId );
        }
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, IdType idType )
    {
        assertCorrectEpoch( context );
        IdAllocation result = spi.allocateIds( idType );
        return new Response<>( result, spi.storeId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context,
                                                           TransactionRepresentation preparedTransaction )
    {
        assertCorrectEpoch( context );
        try
        {
            long txId = spi.applyPreparedTransaction( preparedTransaction );
            return packResponse( context, txId );
        }
        catch ( IOException | org.neo4j.kernel.api.exceptions.TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        assertCorrectEpoch( context );
        // TODO 2.2-future verify the same thing
//        try
//        {
//            resumeTransaction( context );
//        }
//        catch ( TransactionNotPresentOnMasterException e )
//        {   // Let these ones through straight away
//            throw e;
//        }
//        catch ( RuntimeException e )
//        {
//            MasterTransaction masterTransaction = transactions.get( context );
//            It is possible that the transaction is not there anymore, or never was. No need for an NPE to be thrown.
//            if ( masterTransaction != null )
//            {
//                masterTransaction.markAsFinishAsap();
//            }
//            throw e;
//        }

        finishTransaction0( context );

        return packResponse( context, null );
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return packResponse( context, spi.createRelationshipType( name ) );
    }

    @Override
    public Response<Integer> createPropertyKey( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return packResponse( context, spi.getOrCreateProperty( name ) );
    }

    @Override
    public Response<Integer> createLabel( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return packResponse( context, spi.getOrCreateLabel( name ) );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return packResponse( context, null );
    }

    @Override
    public Response<HandshakeResult> handshake( long txId, StoreId storeId )
    {
        try
        {
            Pair<Integer, Long> masterId = spi.getMasterIdForCommittedTx( txId );
            return new Response<>(
                    new HandshakeResult( masterId.first(), masterId.other(), epoch ), spi.storeId(),
                    TransactionStream.EMPTY, ResourceReleaser.NO_OP );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't get master ID for " + txId, e );
        }
    }

    @Override
    public Response<Void> copyStore( RequestContext requestContext, StoreWriter writer )
    {
        RequestContext context;
        try ( StoreWriter storeWriter = writer )
        {
            context = spi.flushStoresAndStreamStoreFiles( storeWriter );
        }   // close the store writer

        return packResponse( context, null );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
                                            String dsName, long startTxId, long endTxId )
    {
        return spi.copyTransactions( dsName, startTxId, endTxId );
    }

    @Override
    public Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType type, long...
            resourceIds )
    {
        assertCorrectEpoch( context );
        try
        {
            System.out.println("Acquiring exlusive log for " + context );
            Locks.Client locks = slaveLockState.get( context ).getLockClient();
            locks.acquireExclusive( type, resourceIds );
            return packResponse( context, new LockResult( LockStatus.OK_LOCKED ) );
        }
        catch ( DeadlockDetectedException e )
        {
            return packResponse( context, new LockResult( e.getMessage() ) );
        }
        catch ( IllegalResourceException e )
        {
            return packResponse( context, new LockResult( LockStatus.NOT_LOCKED ) );
        }
    }

    @Override
    public Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType type, long...
            resourceIds )
    {
        assertCorrectEpoch( context );
        try
        {
            Locks.Client locks = slaveLockState.get( context ).getLockClient();
            locks.acquireShared( type, resourceIds );
            return packResponse( context, new LockResult( LockStatus.OK_LOCKED ) );
        }
        catch ( DeadlockDetectedException e )
        {
            return packResponse( context, new LockResult( e.getMessage() ) );
        }
        catch ( IllegalResourceException e )
        {
            return packResponse( context, new LockResult( LockStatus.NOT_LOCKED ) );
        }
    }

    @Override
    public Response<Void> pushTransaction( RequestContext context, long tx )
    {
        // 2.2-future no longer required
        return null;
    }

    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<RequestContext>> getOngoingTransactions()
    {
        Map<Integer, Collection<RequestContext>> result = new HashMap<>();
        Set<RequestContext> contexts = slaveLockState.keySet();
        for ( RequestContext context : contexts.toArray( new RequestContext[contexts.size()] ) )
        {
            Collection<RequestContext> txs = result.get( context.machineId() );
            if ( txs == null )
            {
                txs = new ArrayList<>();
                result.put( context.machineId(), txs );
            }
            txs.add( context );
        }
        return result;
    }

    private static class TimestampedLockClient
    {
        private final Locks.Client lockClient;
        private final AtomicLong timeLastSuspended = new AtomicLong();
        private volatile boolean finishAsap;

        TimestampedLockClient( Locks.Client locksClient )
        {
            this.lockClient = locksClient;
        }

        void updateTime()
        {
            this.timeLastSuspended.set( System.currentTimeMillis() );
        }

        void resetTime()
        {
            this.timeLastSuspended.set( 0 );
        }

        void markAsFinishAsap()
        {
            this.finishAsap = true;
        }

        public Locks.Client getLockClient()
        {
            return lockClient;
        }

        @Override
        public String toString()
        {
            return lockClient + "[lastSuspended=" + timeLastSuspended + ", finishAsap=" + finishAsap + "]";
        }

        boolean finishAsap()
        {
            return this.finishAsap;
        }
    }

    private class UnfinishedTransactionReaper implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                Map<RequestContext, TimestampedLockClient> safeTransactions;
                synchronized ( slaveLockState )
                {
                    safeTransactions = new HashMap<>( slaveLockState );
                }

                for ( Map.Entry<RequestContext, TimestampedLockClient> entry : safeTransactions.entrySet() )
                {
                    long time = entry.getValue().timeLastSuspended.get();
                    if ( (time != 0 && System.currentTimeMillis() - time >= unfinishedTransactionThresholdMillis)
                            || entry.getValue().finishAsap() )
                    {
                        long displayableTime = (time == 0 ? 0 : (System.currentTimeMillis() - time));
                        String oldTxDescription = format( "old tx %s: %s at age %s ms",
                                entry.getKey(), entry.getValue().getLockClient(), displayableTime );
                        try
                        {
                            finishTransaction0( entry.getKey() );
                            msgLog.info( "Rolled back " + oldTxDescription );
                        }
                        catch ( Throwable t )
                        {
                            // Not really expected
                            msgLog.warn( "Unable to roll back " + oldTxDescription, t );
                        }
                    }
                }
            }
            catch ( Throwable t )
            {
                // The show must go on
                msgLog.warn( "Exception running " + getClass().getName() + ", although will continue...", t );
            }
        }
    }
}
