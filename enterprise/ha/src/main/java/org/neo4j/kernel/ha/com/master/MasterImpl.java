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
package org.neo4j.kernel.ha.com.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.util.collection.ConcurrentAccessException;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

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

    // This is a bridge SPI that MasterImpl requires to function. Eventually this should be split
    // up into many smaller APIs implemented by other services so that this is not needed.
    // This SPI allows MasterImpl to have no direct dependencies, and instead puts those dependencies into the
    // SPI implementation, thus making it easier to test this class by mocking the SPI.
    public interface SPI
    {
        boolean isAccessible();

        IdAllocation allocateIds( IdType idType );

        StoreId storeId();

        long applyPreparedTransaction( TransactionRepresentation preparedTransaction )
                throws IOException, org.neo4j.kernel.api.exceptions.TransactionFailureException;

        Integer createRelationshipType( String name );

        long getTransactionChecksum( long txId ) throws IOException;

        RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer );

        <T> Response<T> packEmptyResponse( T response );

        <T> Response<T> packTransactionStreamResponse( RequestContext context, T response );

        <T> Response<T> packTransactionObligationResponse( RequestContext context, T response );

        int getOrCreateLabel( String name );

        int getOrCreateProperty( String name );

    }

    private final SPI spi;
    private final Config config;
    private final Monitor monitor;
    private final long epoch;

    private ConversationManager conversationManager;

    public MasterImpl( SPI spi, ConversationManager conversationManager, Monitor monitor, Config config )
    {
        this.spi = spi;
        this.config = config;
        this.monitor = monitor;
        this.conversationManager = conversationManager;
        this.epoch = generateEpoch();
    }

    private long generateEpoch()
    {
        return ( ( ( (long) config.get( ClusterSettings.server_id ).toIntegerIndex() ) ) << 48 ) | System.currentTimeMillis();
    }

    @Override
    public void start() throws Throwable
    {
        conversationManager.start();
    }

    @Override
    public void stop()
    {
        conversationManager.stop();
    }

    /**
     * Basically for all public methods call this assertion to verify that the caller meant to call this
     * master. The epoch is the one handed out from {@link #handshake(long, StoreId)}.
     * Exceptions to the above are:
     * o {@link #handshake(long, StoreId)}
     * o {@link #copyStore(RequestContext, StoreWriter)}
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

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, IdType idType )
    {
        assertCorrectEpoch( context );
        IdAllocation result = spi.allocateIds( idType );
        return spi.packEmptyResponse( result );
    }

    @Override
    public Response<Long> commit( RequestContext context, TransactionRepresentation preparedTransaction )
            throws IOException, org.neo4j.kernel.api.exceptions.TransactionFailureException
    {
        assertCorrectEpoch( context );

        // There are two constraints relating to locking during commit:
        // 1) If the client is has grabbed locks, we need to ensure those locks remain live during commit
        // 2) We must hold a schema read lock while committing, to not race with schema transactions on the master
        //
        // To satisfy this, we must determine if the client is holding locks, and if so, use that lock client, and if
        // not, we use a one-off lock client. The way the client signals this is via the 'eventIdentifier' in the
        // request. -1 means no locks are held, any other number means there should be a matching lock session.

        if(context.getEventIdentifier() == -1)
        {
            // Client is not holding locks, use a temporary lock client
            try(Conversation conversation = conversationManager.acquire())
            {
                return commit0( context, preparedTransaction, conversation.getLocks() );
            }
        }
        else
        {
            // Client is holding locks, use the clients lock session
            try
            {
                Conversation conversation = conversationManager.acquire( context );
                Locks.Client locks = conversation.getLocks();
                try
                {
                    return commit0( context, preparedTransaction, locks );
                }
                finally
                {
                    conversationManager.release(context);
                }
            }
            catch(NoSuchEntryException | ConcurrentAccessException e)
            {
                throw new TransactionNotPresentOnMasterException(context);
            }
        }
    }

    private Response<Long> commit0( RequestContext context, TransactionRepresentation preparedTransaction, Locks
            .Client locks ) throws IOException, org.neo4j.kernel.api.exceptions.TransactionFailureException
    {
        if ( locks.trySharedLock( ResourceTypes.SCHEMA, ResourceTypes.schemaResource() ) )
        {
            long txId = spi.applyPreparedTransaction( preparedTransaction );
            return spi.packTransactionObligationResponse( context, txId );
        }
        else
        {
            throw new TransactionFailureException( Status.Schema.ModifiedConcurrently, "Failed to commit, because another transaction is making " +
                    "schema changes. Slave commits are disallowed while schema changes are being committed. " +
                    "Retrying the transaction should yield a successful result." );
        }
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return spi.packTransactionObligationResponse( context, spi.createRelationshipType( name ) );
    }

    @Override
    public Response<Integer> createPropertyKey( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return spi.packTransactionObligationResponse( context, spi.getOrCreateProperty( name ) );
    }

    @Override
    public Response<Integer> createLabel( RequestContext context, String name )
    {
        assertCorrectEpoch( context );
        return spi.packTransactionObligationResponse( context, spi.getOrCreateLabel( name ) );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return spi.packTransactionStreamResponse( context, null );
    }

    @Override
    public Response<HandshakeResult> handshake( long txId, StoreId storeId )
    {
        try
        {
            long checksum = spi.getTransactionChecksum( txId );
            return spi.packEmptyResponse( new HandshakeResult( checksum, epoch ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't get master ID for transaction id " + txId, e );
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

        return spi.packTransactionStreamResponse( context, null );
    }

    @Override
    public Response<Void> newLockSession( RequestContext context ) throws TransactionFailureException
    {
        monitor.initializeTx( context );

        if ( !spi.isAccessible() )
        {
            throw new TransactionFailureException(Status.General.DatabaseUnavailable, "Database is currently not available" );
        }
        assertCorrectEpoch( context );

        try
        {
            conversationManager.begin( context );
        }
        catch ( ConcurrentAccessException e )
        {
            throw new TransactionFailureException( Status.Transaction.ConcurrentRequest, e,
                    "The lock session requested to start is already in use. " +
                    "Please retry your request in a few seconds." );
        }
        return spi.packTransactionObligationResponse( context, null );
    }

    @Override
    public Response<Void> endLockSession( RequestContext context, boolean success )
    {
        assertCorrectEpoch( context );
        conversationManager.end( context );
        if ( !success )
        {
            conversationManager.stop( context );
        }
        return spi.packTransactionObligationResponse( context, null );
    }

    @Override
    public Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType type,
                                                      long... resourceIds )
    {
        assertCorrectEpoch( context );
        Locks.Client session;
        try
        {
            session = conversationManager.acquire( context ).getLocks();
        }
        catch ( NoSuchEntryException | ConcurrentAccessException e)
        {
            return spi.packTransactionObligationResponse( context, new LockResult( "Unable to acquire exclusive lock: " + e.getMessage() ) );
        }
        try
        {
            for ( long resourceId : resourceIds )
            {
                session.acquireExclusive( type, resourceId );
            }
            return spi.packTransactionObligationResponse( context, new LockResult( LockStatus.OK_LOCKED ) );
        }
        catch ( DeadlockDetectedException e )
        {
            return spi.packTransactionObligationResponse( context, new LockResult( "Can't acquire exclusive lock, because it would have caused a deadlock: " + e.getMessage() ) );
        }
        catch ( IllegalResourceException e )
        {
            return spi.packTransactionObligationResponse( context, new LockResult( LockStatus.NOT_LOCKED ) );
        }
        finally
        {
            conversationManager.release( context );
        }
    }

    @Override
    public Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType type,
                                                   long... resourceIds )
    {
        assertCorrectEpoch( context );
        Locks.Client session;
        try
        {
            session = conversationManager.acquire( context ).getLocks();
        }
        catch ( NoSuchEntryException | ConcurrentAccessException e)
        {
            return spi.packTransactionObligationResponse( context, new LockResult( "Unable to acquire shared lock: " + e.getMessage() ) );
        }
        try
        {
            for ( long resourceId : resourceIds )
            {
                session.acquireShared( type, resourceId );
            }

            return spi.packTransactionObligationResponse( context, new LockResult( LockStatus.OK_LOCKED ) );
        }
        catch ( DeadlockDetectedException e )
        {
            return spi.packTransactionObligationResponse( context, new LockResult( e.getMessage() ) );
        }
        catch ( IllegalResourceException e )
        {
            return spi.packTransactionObligationResponse( context, new LockResult( LockStatus.NOT_LOCKED ) );
        }
        finally
        {
            conversationManager.release( context );
        }
    }

    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<RequestContext>> getOngoingTransactions()
    {
        Map<Integer, Collection<RequestContext>> result = new HashMap<>();
        Set<RequestContext> contexts = conversationManager.getActiveContexts();
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
}
