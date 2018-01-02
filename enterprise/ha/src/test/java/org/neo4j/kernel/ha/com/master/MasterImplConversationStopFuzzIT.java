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

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.ConversationSPI;
import org.neo4j.kernel.ha.cluster.DefaultConversationSPI;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.collection.ConcurrentAccessException;
import org.neo4j.kernel.impl.util.collection.TimedRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.lock_read_timeout;

/**
 *  Current test will try to emulate client master conversation lifecycle
 *  starting from handshake till end of the session,
 *  including simulation of idle conversations cleanup and inactive session removal.
 *
 *  Workers will try to follow common patterns of client-server communication using defined state machine.
 *  Except common cases state machine will try to simulate abnormal behaviour and will
 *  fall into sleep from time to time emulating inactivates.
 *
 */
public class MasterImplConversationStopFuzzIT
{
    private static final int numberOfWorkers = 10;
    private static final int numberOfOperations = 1_000;
    private static final int numberOfResources = 100;

    public static final StoreId StoreId = new StoreId();

    private final LifeSupport life = new LifeSupport();
    private final ExecutorService executor = Executors.newFixedThreadPool( numberOfWorkers + 1 );
    private final JobScheduler scheduler = life.add( new Neo4jJobScheduler() );
    private final Config config = new Config( stringMap( server_id.name(), "0", lock_read_timeout.name(), "1" ) );
    private final Locks locks = new ForsetiLockManager( ResourceTypes.NODE, ResourceTypes.SCHEMA );

    private static MasterExecutionStatistic executionStatistic = new MasterExecutionStatistic();

    @Test( timeout = 50000 )
    public void shouldHandleRandomizedLoad() throws Throwable
    {
        // Given
        DefaultConversationSPI conversationSPI = new DefaultConversationSPI( locks, scheduler );
        final ExposedConversationManager
                conversationManager = new ExposedConversationManager( conversationSPI, config, 100, 0 );

        ConversationTestMasterSPI conversationTestMasterSPI = new ConversationTestMasterSPI();
        MasterImpl master = new MasterImpl( conversationTestMasterSPI, conversationManager,
                new Monitors().newMonitor( MasterImpl.Monitor.class ), config );
        life.add( conversationManager);
        life.start();

        ConversationKiller conversationKiller = new ConversationKiller( conversationManager );
        executor.submit( conversationKiller );
        List<Callable<Void>> slaveWorkers = workers( master, numberOfWorkers );
        List<Future<Void>> workers = executor.invokeAll( slaveWorkers );

        // Wait for all workers to complete
        for ( Future<Void> future : workers )
        {
            future.get();
        }
        conversationKiller.stop();

        assertTrue( executionStatistic.isSuccessfulExecution() );
    }

    @After
    public void cleanup() throws InterruptedException
    {
        life.shutdown();
        executor.shutdownNow();
    }

    private List<Callable<Void>> workers( MasterImpl master, int numWorkers )
    {
        LinkedList<Callable<Void>> workers = new LinkedList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            workers.add( new SlaveEmulatorWorker( master, i ) );
        }
        return workers;
    }

    static class SlaveEmulatorWorker implements Callable<Void>
    {
        private final Random random;
        private final MasterImpl master;
        private final int machineId;

        private State state = State.UNINITIALIZED;
        private final long lastTx = 0;
        private long epoch;
        private RequestContext requestContext;

        enum State
        {
            UNINITIALIZED
                    {
                        @Override
                        State next( SlaveEmulatorWorker worker )
                        {
                            HandshakeResult handshake = worker.master.handshake( worker.lastTx, StoreId ).response();
                            worker.epoch = handshake.epoch();
                            return IDLE;
                        }
                    },
            IDLE
                    {
                        @Override
                        State next( SlaveEmulatorWorker worker ) throws Exception
                        {
                            if ( lowProbabilityEvent( worker ) )
                            {
                                return UNINITIALIZED;
                            }
                            else if ( lowProbabilityEvent( worker ) )
                            {
                                return commit( worker, new RequestContext( worker.epoch, worker.machineId, -1, worker.lastTx, 0 ) );
                            }
                            else
                            {
                                try
                                {
                                    worker.master.newLockSession( worker.newRequestContext() );
                                    return IN_SESSION;
                                }
                                catch ( TransactionFailureException e )
                                {
                                    if ( e.getCause() instanceof ConcurrentAccessException )
                                    {
                                        executionStatistic.reportAlreadyInUseError();
                                        return IDLE;
                                    }
                                    else
                                    {
                                        throw e;
                                    }
                                }
                            }
                        }
                    },
            IN_SESSION
                    {
                        @Override
                        State next( SlaveEmulatorWorker worker ) throws Exception
                        {
                            if ( lowProbabilityEvent( worker ) )
                            {
                                return UNINITIALIZED;
                            }
                            else
                            {
                                int i = worker.random.nextInt( 10 );
                                if ( i >= 5 )
                                {
                                    return commit( worker, worker.requestContext );
                                }
                                else if ( i >= 4 )
                                {
                                    worker.master.acquireExclusiveLock( worker.requestContext, ResourceTypes.NODE,
                                            randomResource( worker ) );
                                    return IN_SESSION;
                                }
                                else if ( i >= 1 )
                                {
                                    worker.master.acquireSharedLock( worker.requestContext, ResourceTypes.NODE,
                                            randomResource( worker ) );
                                    return IN_SESSION;
                                }
                                else
                                {
                                    endLockSession( worker );
                                    return IDLE;
                                }
                            }
                        }
                    },
            CLOSING_SESSION
                    {
                        @Override
                        State next( SlaveEmulatorWorker worker ) throws Exception
                        {
                            if ( lowProbabilityEvent( worker ) )
                            {
                                return UNINITIALIZED;
                            }
                            else
                            {
                                endLockSession( worker );
                                return IDLE;
                            }
                        }
                    };

            abstract State next( SlaveEmulatorWorker worker ) throws Exception;

            protected State commit( SlaveEmulatorWorker worker, RequestContext requestContext )
                    throws IOException, TransactionFailureException
            {
                try
                {
                    worker.master.commit( requestContext, mock( TransactionRepresentation.class ) );
                    executionStatistic.reportCommittedOperation();
                    return CLOSING_SESSION;
                }
                catch ( TransactionNotPresentOnMasterException e )
                {
                    executionStatistic.reportTransactionNotPresentError();
                    return IDLE;
                }
            }
        }

        private static boolean lowProbabilityEvent( SlaveEmulatorWorker worker )
        {
            return worker.random.nextInt( 100 ) <= 1;
        }

        private static long randomResource( SlaveEmulatorWorker worker )
        {
            return worker.random.nextInt( numberOfResources );
        }

        public SlaveEmulatorWorker( MasterImpl master, int clientNumber)
        {
            this.machineId = clientNumber;
            this.random = new Random( machineId );
            this.master = master;
        }

        @Override
        public Void call() throws Exception
        {
            for ( int i = 0; i < numberOfOperations; i++ )
            {
                state = state.next( this );
            }
            return null;
        }

        private RequestContext newRequestContext()
        {
            return requestContext = new RequestContext( epoch, machineId, newLockSessionId(), lastTx, random.nextInt() );
        }

        private int newLockSessionId()
        {
            return random.nextInt();
        }

        private static void endLockSession( SlaveEmulatorWorker worker )
        {
            boolean successfulSession = worker.random.nextBoolean();
            worker.master.endLockSession( worker.requestContext, successfulSession );
        }
    }

    static class ConversationTestMasterSPI implements MasterImpl.SPI
    {
        @Override
        public boolean isAccessible()
        {
            return true;
        }

        @Override
        public StoreId storeId()
        {
            return StoreId;
        }

        @Override
        public long applyPreparedTransaction( TransactionRepresentation preparedTransaction )
                throws IOException, TransactionFailureException
        {
            // sleeping here and hope to be noticed by conversation killer.
            sleep();
            return 0;
        }

        private void sleep()
        {
            try
            {
                Thread.sleep( 20 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public long getTransactionChecksum( long txId ) throws IOException
        {
            return 0;
        }

        @Override
        public <T> Response<T> packEmptyResponse( T response )
        {
            return new TransactionObligationResponse<>( response, StoreId, TransactionIdStore.BASE_TX_ID,
                    ResourceReleaser.NO_OP );
        }

        @Override
        public <T> Response<T> packTransactionObligationResponse( RequestContext context, T response )
        {
            return packEmptyResponse( response );
        }

        @Override
        public IdAllocation allocateIds( IdType idType )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer createRelationshipType( String name )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Response<T> packTransactionStreamResponse( RequestContext context, T response )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOrCreateLabel( String name )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOrCreateProperty( String name )
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * This emulates the MasterServer behavior of killing conversations after they have not had traffic sent on them for
     * a certain time
     */
    private static class ConversationKiller implements Runnable
    {
        private volatile boolean running = true;
        private final ConversationManager conversationManager;

        public ConversationKiller( ConversationManager conversationManager )
        {
            this.conversationManager = conversationManager;
        }

        @Override
        public void run()
        {
            try
            {
                while ( running )
                {
                    Iterator<RequestContext> conversationIterator = conversationManager.getActiveContexts().iterator();
                    if ( conversationIterator.hasNext() )
                    {
                        RequestContext next = conversationIterator.next();
                        conversationManager.end( next );
                    }
                    try
                    {
                        Thread.sleep( 10 );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( "Conversation killer failed.", e );
            }
        }

        public void stop()
        {
            running = false;
        }
    }

    private class ExposedConversationManager extends ConversationManager {

        private TimedRepository<RequestContext,Conversation> conversationStore;

        public ExposedConversationManager( ConversationSPI spi, Config config, int activityCheckInterval,
                int lockTimeoutAddition )
        {
            super( spi, config, activityCheckInterval, lockTimeoutAddition );
        }

        @Override
        protected TimedRepository<RequestContext,Conversation> createConversationStore()
        {
            conversationStore = new TimedRepository<>( getConversationFactory(), getConversationReaper(),
                    1, Clock.SYSTEM_CLOCK );
            return conversationStore;
        }
    }

    private static class MasterExecutionStatistic
    {
        private final AtomicLong alreadyInUseErrors = new AtomicLong();
        private final AtomicLong transactionNotPresentErrors = new AtomicLong();
        private final AtomicLong committedOperations = new AtomicLong();

        public void reportAlreadyInUseError()
        {
            alreadyInUseErrors.incrementAndGet();
        }

        public void reportTransactionNotPresentError()
        {
            transactionNotPresentErrors.incrementAndGet();
        }

        public void reportCommittedOperation()
        {
            committedOperations.incrementAndGet();
        }

        public AtomicLong getAlreadyInUseErrors()
        {
            return alreadyInUseErrors;
        }

        public AtomicLong getTransactionNotPresentErrors()
        {
            return transactionNotPresentErrors;
        }

        public AtomicLong getCommittedOperations()
        {
            return committedOperations;
        }

        public boolean isSuccessfulExecution()
        {
            return committedOperations.get() > ((alreadyInUseErrors.get() + transactionNotPresentErrors.get()) * 10);
        }
    }
}
