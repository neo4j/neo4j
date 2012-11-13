/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is the real master code that executes on a master. The actual
 * communication over network happens in {@link MasterClient} and
 * {@link MasterServer}.
 */
public class MasterImpl extends LifecycleAdapter implements Master
{
    private static final int ID_GRAB_SIZE = 1000;
    public static final int UNFINISHED_TRANSACTION_CLEANUP_DELAY = 1;

    private final GraphDatabaseAPI graphDb;
    private final StringLogger msgLog;
    private final Config config;

    private final Map<RequestContext, MasterTransaction> transactions = new ConcurrentHashMap<RequestContext,
            MasterTransaction>();
    private ScheduledExecutorService unfinishedTransactionsExecutor;
    private long unfinishedTransactionThresholdMillis;
    private GraphProperties graphProperties;
    private final LockManager lockManager;
    private final TransactionManager txManager;

    public MasterImpl( GraphDatabaseAPI db, StringLogger logger, Config config )
    {
        this.graphDb = db;
        this.msgLog = logger;
        this.config = config;
        graphProperties = graphDb.getDependencyResolver().resolveDependency( NodeManager.class ).getGraphProperties();
        lockManager = graphDb.getDependencyResolver().resolveDependency( LockManager.class );
        txManager = graphDb.getDependencyResolver().resolveDependency( TransactionManager.class );
    }

    @Override
    public void start() throws Throwable
    {
        this.unfinishedTransactionThresholdMillis = config.isSet( HaSettings.lock_read_timeout ) ?
                config.get( HaSettings.lock_read_timeout ) : config.get( HaSettings.read_timeout );
        this.unfinishedTransactionsExecutor =
                Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "Unfinished transaction reaper" ) );
        this.unfinishedTransactionsExecutor.scheduleWithFixedDelay( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Map<RequestContext, MasterTransaction> safeTransactions = null;
                    synchronized ( transactions )
                    {
                        safeTransactions = new HashMap<RequestContext, MasterTransaction>( transactions );
                    }

                    for ( Map.Entry<RequestContext, MasterTransaction> entry : safeTransactions.entrySet() )
                    {
                        long time = entry.getValue().timeLastSuspended.get();
                        if ( (time != 0 && System.currentTimeMillis() - time >= unfinishedTransactionThresholdMillis) || entry.getValue().finishAsap() )
                        {
                            long displayableTime = (time == 0 ? 0 : (System.currentTimeMillis() - time));
                            msgLog.logMessage( "Found old tx " + entry.getKey() + ", " +
                                    "" + entry.getValue().transaction + ", " + displayableTime );
                            try
                            {
                                Transaction otherTx = suspendOtherAndResumeThis( entry.getKey(), false );
                                finishThisAndResumeOther( otherTx, entry.getKey(), false );
                                msgLog.logMessage( "Rolled back old tx " + entry.getKey() + ", " +
                                        "" + entry.getValue().transaction + ", " + displayableTime );
                            }
                            catch ( IllegalStateException e )
                            {
                                // Expected for waiting transactions
                            }
                            catch ( Throwable t )
                            {
                                // Not really expected
                                msgLog.logMessage( "Unable to roll back old tx " + entry.getKey() + ", " +
                                        "" + entry.getValue().transaction + ", " + displayableTime );
                            }
                        }
                    }
                }
                catch ( Throwable t )
                {
                    // The show must go on
                }
            }
        }, UNFINISHED_TRANSACTION_CLEANUP_DELAY, UNFINISHED_TRANSACTION_CLEANUP_DELAY, TimeUnit.SECONDS );
    }

    @Override
    public void stop()
    {
        unfinishedTransactionsExecutor.shutdown();
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context, true );
        try
        {
            return packResponse( context, null );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx, context );
        }
    }

    private Response<LockResult> acquireLock( RequestContext context,
                                              LockGrabber lockGrabber, Object... entities )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context, false );
        try
        {
            LockManager lockManager = graphDb.getLockManager();
            TransactionState state = ((AbstractTransactionManager)graphDb.getTxManager()).getTransactionState();
            for ( Object entity : entities )
            {
                lockGrabber.grab( lockManager, state, entity );
            }
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
            suspendThisAndResumeOther( otherTx, context );
        }
    }

    private <T> Response<T> packResponse( RequestContext context, T response )
    {
        return packResponse( context, response, ServerUtil.ALL );
    }

    private <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        return ServerUtil.packResponse( graphDb, context, response, filter );
    }

    private Transaction getTx( RequestContext txId )
    {
        MasterTransaction result = transactions.get( txId );
        if ( result != null )
        {
            // set time stamp to zero so that we don't even try to finish it off
            // if getting old. This is because if the tx is active and old then
            // it means it's waiting for a lock and we cannot do anything about it.
            result.resetTime();
            return result.transaction;
        }
        return null;
    }

    private Transaction beginTx( RequestContext txId )
    {
        try
        {
            txManager.begin();
            Transaction tx = txManager.getTransaction();
            transactions.put( txId, new MasterTransaction( tx ) );
            return tx;
        }
        catch ( NotSupportedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    Transaction suspendOtherAndResumeThis( RequestContext txId, boolean allowBegin )
    {
        try
        {
            Transaction otherTx = txManager.getTransaction();
            Transaction transaction = getTx( txId );
            if ( otherTx != null && otherTx == transaction )
            {
                return null;
            }
            else
            {
                if ( otherTx != null )
                {
                    txManager.suspend();
                }
                if ( transaction == null )
                {
                    if ( allowBegin )
                    {
                        beginTx( txId );
                    }
                    else
                    {
                        throw new IllegalStateException( "Transaction " + txId + " has either timed out on the" +
                                " master or was not started on this master. There may have been a master switch" +
                                " between the time this transaction started and up to now. This transaction" +
                                " cannot continue since the state from the previous master isn't transferred." );
                    }
                }
                else
                {
                    try
                    {
                        txManager.resume( transaction );
                    }
                    catch ( IllegalStateException e )
                    {
                        throw new UnableToResumeTransactionException( e );
                    }
                }
                return otherTx;
            }
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    void suspendThisAndResumeOther( Transaction otherTx, RequestContext txId )
    {
        try
        {
            MasterTransaction tx = transactions.get( txId );
            if ( tx.finishAsap() )
            {   // If we've tried to finish this tx off earlier then do it now when we have the chance.
                finishThisAndResumeOther( otherTx, txId, false );
                return;
            }

            // update time stamp to current time so that we know that this tx just completed
            // a request and can now again start to be monitored, so that it can be
            // rolled back if it's getting old.
            tx.updateTime();

            txManager.suspend();
            if ( otherTx != null )
            {
                txManager.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    void finishThisAndResumeOther( Transaction otherTx, RequestContext txId, boolean success )
    {
        try
        {
            if ( success )
            {
                txManager.commit();
            }
            else
            {
                txManager.rollback();
            }
            transactions.remove( txId );
            if ( otherTx != null )
            {
                txManager.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return acquireLock( context, READ_LOCK_GRABBER, nodesById( nodes ) );
    }

    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, nodesById( nodes ) );
    }

    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
                                                             long... relationships )
    {
        return acquireLock( context, READ_LOCK_GRABBER, relationshipsById( relationships ) );
    }

    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
                                                              long... relationships )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, relationshipsById( relationships ) );
    }

    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return acquireLock( context, READ_LOCK_GRABBER, graphProperties() );
    }

    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, graphProperties() );
    }

    private PropertyContainer graphProperties()
    {
        return graphProperties;
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

    public Response<IdAllocation> allocateIds( IdType idType )
    {
        IdGenerator generator = graphDb.getIdGeneratorFactory().get( idType );
        IdAllocation result = new IdAllocation( generator.nextIdBatch( ID_GRAB_SIZE ), generator.getHighId(),
                generator.getDefragCount() );
        return ServerUtil.packResponseWithoutTransactionStream( graphDb.getStoreId(), result );
    }

    public Response<Long> commitSingleResourceTransaction( RequestContext context, String resource,
                                                           TxExtractor txGetter )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context, false );
        try
        {
            XaDataSource dataSource = graphDb.getXaDataSourceManager()
                    .getXaDataSource( resource );
            final long txId = dataSource.applyPreparedTransaction( txGetter.extract() );
            Predicate<Long> upUntilThisTx = new Predicate<Long>()
            {
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
            suspendThisAndResumeOther( otherTx, context );
        }
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        Transaction otherTx;
        try
        {
            otherTx = suspendOtherAndResumeThis( context, false );
        }
        catch ( UnableToResumeTransactionException e )
        {
            transactions.get( context ).markAsFinishAsap();
            throw e;
        }

        finishThisAndResumeOther( otherTx, context, success );

        return packResponse( context, null );
    }

    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        graphDb.getRelationshipTypeHolder().addValidRelationshipType( name, true );
        return packResponse( context, graphDb.getRelationshipTypeHolder().getIdFor( name ) );
    }

    public Response<Void> pullUpdates( RequestContext context )
    {
        return packResponse( context, null );
    }

    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId storeId )
    {
        XaDataSource nioneoDataSource = graphDb.getXaDataSourceManager()
                .getNeoStoreDataSource();
        try
        {
            Pair<Integer, Long> masterId = nioneoDataSource.getMasterForCommittedTx( txId );
            return ServerUtil.packResponseWithoutTransactionStream( graphDb.getStoreId(), masterId );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't get master ID for " + txId, e );
        }
    }

    public Response<Void> copyStore( RequestContext context, StoreWriter writer )
    {
        context = ServerUtil.rotateLogsAndStreamStoreFiles( graphDb, true, writer );
        writer.done();
        return packResponse( context, null );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
                                            String dsName, long startTxId, long endTxId )
    {
        return ServerUtil.getTransactions( graphDb, dsName, startTxId, endTxId );
    }

    private static interface LockGrabber
    {
        void grab( LockManager lockManager, TransactionState state, Object entity );
    }

    private static LockGrabber READ_LOCK_GRABBER = new LockGrabber()
    {
        public void grab( LockManager lockManager, TransactionState state, Object entity )
        {
            lockManager.getReadLock( entity );
            state.addLockToTransaction( lockManager, entity, LockType.READ );
        }
    };

    private static LockGrabber WRITE_LOCK_GRABBER = new LockGrabber()
    {
        public void grab( LockManager lockManager, TransactionState state, Object entity )
        {
            lockManager.getWriteLock( entity );
            state.addLockToTransaction( lockManager, entity, LockType.WRITE );
        }
    };

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return acquireLock( context, READ_LOCK_GRABBER, new NodeManager.IndexLock( index, key ) );
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index,
                                                       String key )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, new NodeManager.IndexLock( index, key ) );
    }

    @Override
    public Response<Void> pushTransaction( RequestContext context, String resourceName, long tx )
    {
        graphDb.getTxIdGenerator().committed( graphDb.getXaDataSourceManager().getXaDataSource( resourceName ),
                context.getEventIdentifier(), tx, context.machineId() );
        return new Response<Void>( null, graphDb.getStoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<RequestContext>> getOngoingTransactions()
    {
        Map<Integer, Collection<RequestContext>> result = new HashMap<Integer, Collection<RequestContext>>();
        for ( RequestContext context : transactions.keySet().toArray( new RequestContext[0] ) )
        {
            Collection<RequestContext> txs = result.get( context.machineId() );
            if ( txs == null )
            {
                txs = new ArrayList<RequestContext>();
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
            return transaction+"[lastSuspended="+timeLastSuspended+", finishAsap="+finishAsap+"]";
        }

        boolean finishAsap()
        {
            return this.finishAsap;
        }
    }
}
