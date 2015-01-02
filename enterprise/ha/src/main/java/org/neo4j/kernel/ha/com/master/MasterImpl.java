/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static java.lang.String.format;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
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

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.ha.lock.LockableNode;
import org.neo4j.kernel.ha.lock.LockableRelationship;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.IndexLock;
import org.neo4j.kernel.impl.core.SchemaLock;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.IndexEntryLock;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionAlreadyActiveException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

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

        void acquireLock( LockGrabber grabber, Object... entities );

        Transaction beginTx() throws SystemException, NotSupportedException;

        void finishTransaction( boolean success );

        void suspendTransaction() throws SystemException;

        void resumeTransaction( Transaction transaction );

        GraphProperties graphProperties();

        IdAllocation allocateIds( IdType idType );

        StoreId storeId();

        long applyPreparedTransaction( String resource, ReadableByteChannel extract ) throws IOException;

        Integer createRelationshipType( String name );

        Pair<Integer, Long> getMasterIdForCommittedTx( long txId ) throws IOException;

        RequestContext rotateLogsAndStreamStoreFiles( StoreWriter writer );

        Response<Void> copyTransactions( String dsName, long startTxId, long endTxId );

        <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter );

        void pushTransaction( String resourceName, int eventIdentifier, long tx, int machineId );

        int getOrCreateLabel( String name );

        int getOrCreateProperty( String name );
    }

    public static final int UNFINISHED_TRANSACTION_CLEANUP_DELAY = 1;

    private final SPI spi;
    private final StringLogger msgLog;
    private final Config config;
    private final Monitor monitor;
    private final long epoch;

    private Map<RequestContext, MasterTransaction> transactions = new ConcurrentHashMap<>();
    private ScheduledExecutorService unfinishedTransactionsExecutor;
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
        this.unfinishedTransactionsExecutor =
                Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "Unfinished transaction reaper" ) );
        this.unfinishedTransactionsExecutor.scheduleWithFixedDelay( new UnfinishedTransactionReaper(),
                UNFINISHED_TRANSACTION_CLEANUP_DELAY, UNFINISHED_TRANSACTION_CLEANUP_DELAY, TimeUnit.SECONDS );
    }

    @Override
    public void stop()
    {
        unfinishedTransactionsExecutor.shutdown();
        transactions = null;
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

        boolean beganTx = false;
        try
        {
            Transaction tx = spi.beginTx();
            transactions.put( context, new MasterTransaction( tx ) );
            beganTx = true;

            return packResponse( context, null );
        }
        catch ( NotSupportedException | SystemException e )
        {
            throw Exceptions.launderedException( e );
        }
        finally
        {
            if(beganTx)
            {
                suspendTransaction( context );
            }
        }
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

    private Response<LockResult> acquireLock( RequestContext context,
                                              LockGrabber lockGrabber, Object... entities )
    {
        assertCorrectEpoch( context );
        resumeTransaction( context );
        try
        {
            spi.acquireLock( lockGrabber, entities );
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
        finally
        {
            suspendTransaction( context );
        }
    }

    private <T> Response<T> packResponse( RequestContext context, T response )
    {
        return packResponse( context, response, ServerUtil.ALL );
    }

    private <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        return spi.packResponse( context, response, filter );
    }

    private Transaction getTx( RequestContext txId )
    {
        MasterTransaction result = transactions.get( txId );
        if ( result == null )
        {
            throw new TransactionNotPresentOnMasterException( txId );
        }
        
        // set time stamp to zero so that we don't even try to finish it off
        // if getting old. This is because if the tx is active and old then
        // it means it's waiting for a lock and we cannot do anything about it.
        result.resetTime();
        return result.transaction;
    }

    private void resumeTransaction( RequestContext txId )
    {
        spi.resumeTransaction( getTx( txId ) );
    }

    private void suspendTransaction( RequestContext context )
    {
        try
        {
            MasterTransaction tx = transactions.get( context );
            if ( tx.finishAsap() )
            {   // If we've tried to finish this tx off earlier then do it now when we have the chance.
                finishTransaction( context, false );
                return;
            }

            // update time stamp to current time so that we know that this tx just completed
            // a request and can now again start to be monitored, so that it can be
            // rolled back if it's getting old.
            tx.updateTime();
        }
        finally
        {
            try
            {
                spi.suspendTransaction();
            }
            catch ( SystemException e )
            {
                throw Exceptions.launderedException( e );
            }
        }
    }

    private void finishTransaction0( RequestContext txId, boolean success )
    {
        try
        {
            spi.finishTransaction( success );
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
        finally
        {
            transactions.remove( txId );
        }
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return acquireLock( context, READ_LOCK_GRABBER, (Object[])nodesById( nodes ) );
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, (Object[])nodesById( nodes ) );
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
                                                             long... relationships )
    {
        return acquireLock( context, READ_LOCK_GRABBER, (Object[])relationshipsById( relationships ) );
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
                                                              long... relationships )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, (Object[])relationshipsById( relationships ) );
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return acquireLock( context, READ_LOCK_GRABBER, spi.graphProperties() );
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, spi.graphProperties() );
    }

    private Node[] nodesById( long[] ids )
    {
        Node[] result = new Node[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            result[i] = new LockableNode( ids[i] );
        }
        return result;
    }

    private Relationship[] relationshipsById( long[] ids )
    {
        Relationship[] result = new Relationship[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            result[i] = new LockableRelationship( ids[i] );
        }
        return result;
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, IdType idType )
    {
        assertCorrectEpoch( context );
        IdAllocation result = spi.allocateIds( idType );
        return ServerUtil.packResponseWithoutTransactionStream( spi.storeId(), result );
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context, String resource,
                                                           TxExtractor txGetter )
    {
        assertCorrectEpoch( context );
        resumeTransaction( context );
        try
        {
            final long txId = spi.applyPreparedTransaction( resource, txGetter.extract() );
            Predicate<Long> upUntilThisTx = new Predicate<Long>()
            {
                @Override
                public boolean accept( Long item )
                {
                    return item < txId;
                }
            };
            return packResponse( context, txId, upUntilThisTx );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            suspendTransaction( context );
        }
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        assertCorrectEpoch( context );
        try
        {
            resumeTransaction( context );
        }
        catch ( TransactionNotPresentOnMasterException e )
        {   // Let these ones through straight away
            throw e;
        }
        catch ( RuntimeException e )
        {
            MasterTransaction masterTransaction = transactions.get( context );
            // It is possible that the transaction is not there anymore, or never was. No need for an NPE to be thrown.
            if ( masterTransaction != null )
            {
                masterTransaction.markAsFinishAsap();
            }
            throw e;
        }

        finishTransaction0( context, success );

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
            return ServerUtil.packResponseWithoutTransactionStream( spi.storeId(),
                    new HandshakeResult( masterId.first(), masterId.other(), epoch ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't get master ID for " + txId, e );
        }
    }

    @Override
    public Response<Void> copyStore( RequestContext context, StoreWriter writer )
    {
        context = spi.rotateLogsAndStreamStoreFiles( writer );
        writer.done();
        return packResponse( context, null );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
                                            String dsName, long startTxId, long endTxId )
    {
        return spi.copyTransactions( dsName, startTxId, endTxId );
    }

    public static interface LockGrabber
    {
        void grab( LockManager lockManager, TransactionState state, Object entity );
    }

    private static LockGrabber READ_LOCK_GRABBER = new LockGrabber()
    {
        @Override
        public void grab( LockManager lockManager, TransactionState state, Object entity )
        {
            state.acquireReadLock( entity );
        }
    };

    private static LockGrabber WRITE_LOCK_GRABBER = new LockGrabber()
    {
        @Override
        public void grab( LockManager lockManager, TransactionState state, Object entity )
        {
            state.acquireWriteLock( entity );
        }
    };

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return acquireLock( context, READ_LOCK_GRABBER, new IndexLock( index, key ) );
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index,
                                                       String key )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, new IndexLock( index, key ) );
    }

    @Override
    public Response<LockResult> acquireSchemaReadLock( RequestContext context )
    {
        return acquireLock( context, READ_LOCK_GRABBER, new SchemaLock() );
    }

    @Override
    public Response<LockResult> acquireSchemaWriteLock( RequestContext context )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, new SchemaLock() );
    }

    @Override
    public Response<LockResult> acquireIndexEntryWriteLock( RequestContext context, long labelId, long propertyKeyId,
                                                            String propertyValue )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, new IndexEntryLock(
                safeCastLongToInt( labelId ), safeCastLongToInt( propertyKeyId ), propertyValue ) );
    }

    @Override
    public Response<Void> pushTransaction( RequestContext context, String resourceName, long tx )
    {
        assertCorrectEpoch( context );
        spi.pushTransaction( resourceName, context.getEventIdentifier(), tx, context.machineId() );
        return new Response<>( null, spi.storeId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<RequestContext>> getOngoingTransactions()
    {
        Map<Integer, Collection<RequestContext>> result = new HashMap<>();
        Set<RequestContext> contexts = transactions.keySet();
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

    private static class MasterTransaction
    {
        private final Transaction transaction;
        private final AtomicLong timeLastSuspended = new AtomicLong();
        private volatile boolean finishAsap;

        MasterTransaction( Transaction transaction )
        {
            this.transaction = transaction;
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

        @Override
        public String toString()
        {
            return transaction + "[lastSuspended=" + timeLastSuspended + ", finishAsap=" + finishAsap + "]";
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
                Map<RequestContext, MasterTransaction> safeTransactions;
                synchronized ( transactions )
                {
                    safeTransactions = new HashMap<>( transactions );
                }

                for ( Map.Entry<RequestContext, MasterTransaction> entry : safeTransactions.entrySet() )
                {
                    long time = entry.getValue().timeLastSuspended.get();
                    if ( (time != 0 && System.currentTimeMillis() - time >= unfinishedTransactionThresholdMillis)
                            || entry.getValue().finishAsap() )
                    {
                        long displayableTime = (time == 0 ? 0 : (System.currentTimeMillis() - time));
                        String oldTxDescription = format( "old tx %s: %s at age %s ms",
                                entry.getKey(), entry.getValue().transaction, displayableTime );
                        try
                        {
                            resumeTransaction( entry.getKey() );
                            finishTransaction0( entry.getKey(), false );
                            msgLog.info( "Rolled back " + oldTxDescription );
                        }
                        catch ( TransactionAlreadyActiveException e )
                        {
                            // Expected for transactions awaiting locks, just leave them be
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
