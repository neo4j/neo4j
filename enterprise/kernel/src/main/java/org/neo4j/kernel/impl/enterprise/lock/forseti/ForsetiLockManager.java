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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.Pool;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;

/**
 * <h1>Forseti, the Nordic god of justice</h1>
 * <p/>
 * Forseti is a lock manager using the dreadlocks deadlock detection algorithm, which means
 * deadlock detection does not require complex RAG traversal and can be found in O(1).
 * <p/>
 * In the best case, Forseti acquires a lock in one CAS instruction, and scales linearly with the number of cores.
 * However, since it uses a shared-memory approach, it will most likely degrade in use cases where there is high
 * contention and a very large number of sockets running the database.
 * <p/>
 * As such, it is optimized for servers with up to, say, 16 cores across 2 sockets. Past that other strategies such
 * as centralized lock services using message passing may yield better results.
 * <p/>
 * <h2>Locking algorithm</h2>
 * <p/>
 * Forseti is used by acquiring clients, which act as agents on behalf of whoever wants to grab locks. The clients
 * have access to a central map of locks.
 * <p/>
 * To grab a lock, a client must insert itself into the holder list of the lock it wants. The lock may either be a
 * shared lock or an exclusive lock. In the case of a shared lock, the client simply appends itself to the holder list.
 * In the case of an exclusive lock, the client has it's own unique exclusive lock, which it must put into the lock map
 * using a CAS operation.
 * <p/>
 * Once the client is in the holder list, it has the lock.
 * <p/>
 * <h2>Deadlock detection</h2>
 * <p/>
 * Each Client maintains a waiting-for list, which by default always contains the client itself. This list indicates
 * which other clients are blocking our progress. By default, then, if client A is waiting for no-one, its waiting-for
 * list will contain only itself:
 * <p/>
 * A.waitlist = [A]
 * <p/>
 * Once the client is blocked by someone else, it will copy this someones entire wait list into it's own. Assuming A
 * becomes blocked by B, and B has a wait list of:
 * <p/>
 * B.waitlist = [B]
 * <p/>
 * Then A will modify is's wait list as:
 * <p/>
 * A.waitlist = [A] U [B] => [A,B]
 * <p/>
 * It will do this in a loop, continuously figuring out the union of wait lists for all clients it waits for. The magic
 * then happens whenever one of those clients become blocked on client A. Assuming client B now has to wait for A,
 * it will also perform a union of A's wait list (which is [A,B] at this point):
 * <p/>
 * B.waitlist = [B] U [A,B]
 * <p/>
 * As it performs this union, B will find itself in A's waiting list, and when it does, it has detected a deadlock.
 * <p/>
 * This algorithm always identifies real deadlocks, but it may also mistakenly identify a deadlock where there is none;
 * a false positive. For this reason, we have a secondary deadlock verification algorithm that only runs if the
 * algorithm above found what appears to be a deadlock.
 * <p/>
 * The secondary deadlock verification algorithm works like this: Whenever a lock client blocks to wait on a lock, the
 * lock is stored in the clients `waitsFor` field, and the field is cleared when the client unblocks. Since every lock
 * track their owners, we now have all the information we need to traverse the waiter/lock-holder dependency graph to
 * verify that a cycle really does exist.
 * <p/>
 * We first collect the owners of the lock that we are blocking upon. From there, we need to find a lock that one of
 * these lock-owners are waiting on, and have us amongst its owners. So to recap, we collect the immediate owners of
 * the lock that we are immediately blocked upon, then we collect the set of locks that they are waiting upon, and then
 * we collect the combined set of owners of <em>those</em> locks, and if we are amongst those, then we consider the
 * deadlock is real. If we are not amongst those owners, then we take another step out into the graph, collect the next
 * frontier of locks that are waited upon, and their owners, and then we check again in this new owner set. We continue
 * traversing the graph like this until we either find ourselves amongst the owners - a deadlock - or we run out of
 * locks that are being waited upon - no deadlock.
 * <p/>
 */
public class ForsetiLockManager implements Locks
{
    /** This is Forsetis internal lock API, which it uses to do deadlock detection. */
    interface Lock
    {
        /**
         * For each client currently holding this lock, copy their wait list into the given bitset.
         * This is how information on who is waiting for whom is propagated.
         */
        void copyHolderWaitListsInto( SimpleBitSet waitList );

        /**
         * Check if anyone holding this lock is currently waiting for the specified client. This
         * check is performed continuously while a client waits for a lock - if the check ever
         * comes back positive, it means we've deadlocked, because we are waiting for someone
         * (the holder of the lock) who in turn is waiting for us (so they won't release the lock).
         *
         * @param client the client id that is waiting to grab this lock
         * @return the id of a client we've deadlocked with, or -1 if there is not currently a deadlock
         */
        int detectDeadlock( int client );

        /**
         * For introspection and error messages, this gives a (somewhat) human-readable description of who is waiting
         * for the lock.
         */
        String describeWaitList();

        /**
         * Collect the current owners of this lock into the given set. This is used for verifying that apparent
         * deadlocks really do involve circular wait dependencies.
         *
         * Note that the owner set may change while this method is running, and thus it is not guaranteed to reflect any
         * particular snapshot of the set of lock owners. Furthermore, the set may change arbitrarily after the method
         * returns, immediately rendering the result outdated.
         * @param owners The set into which to collect the current owners of this lock.
         */
        void collectOwners( Set<ForsetiClient> owners );
    }

    /**
     * Deadlocks always involve at least two participants - and they can be resolved by either or both participants
     * "aborting", meaning they release their locks and give up, perhaps to try again later. However, this is extremely
     * expensive, since the client may be acting on behalf of a big expensive (or small but important) transaction.
     * <p/>
     * Hence, we want to minimize aborts, and we want to ensure that whichever client does abort is the most sensible
     * one. This interface abstracts multiple different approaches for choosing who should abort.
     */
    interface DeadlockResolutionStrategy
    {
        /**
         * This gets called when a deadlock has been detected by a client - it realizes it's in a deadlock
         * situation with at least one other client. In many cases, both (or more!) clients involved in the
         * deadlock will discover this fact at the same time, and thus this method will get called from different
         * clients asking about the same deadlock.
         * <p/>
         * In fact, this method is guaranteed to keep getting called, eventually from every client involved in
         * a deadlock, assuming the method keeps returning false.
         * <p/>
         * The goal of whoever implements this method should be that for each unique deadlock, independent of how
         * many clients discover it, exactly one client should abort, and no more. Which client is chosen to abort
         * is up to the strategy to decide, but should generally be based on something sensible relating to the
         * value or importance of letting one client continue to the detriment of another.
         * <p/>
         * IMPORTANT: For every unique deadlock, this method MUST abort at least one client involved, eventually.
         * If it does not guarantee this, the deadlock will not be resolved, and the database will
         * actually deadlock, causing a fatal system outage.
         *
         * @param clientThatsAsking this is the client that has discovered a deadlock
         * @param clientWereDeadlockedWith this is the client we've realized we are deadlocking with, meaning
         * this method will eventually be invoked from this clients perspective
         * as well (eg. with inverted arguments), assuming we return false here.
         * @return true to make {@code clientThatsAsking} abort, false to take no action, but wait to be called again
         * from the perspective of the other client.
         */
        boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith );
    }

    /** Pointers to lock maps, one array per resource type. */
    private final ConcurrentMap<Long,ForsetiLockManager.Lock>[] lockMaps;

    /** Reverse lookup resource types by id, used for introspection */
    private final ResourceType[] resourceTypes;

    /** Pool forseti clients. */
    private final Pool<ForsetiClient> clientPool;

    private volatile boolean closed;

    @SuppressWarnings( "unchecked" )
    public ForsetiLockManager( Config config, Clock clock, ResourceType... resourceTypes )
    {
        int maxResourceId = findMaxResourceId( resourceTypes );
        this.lockMaps = new ConcurrentMap[maxResourceId];
        this.resourceTypes = new ResourceType[maxResourceId];

        /* Wait strategies per resource type */
        WaitStrategy<AcquireLockTimeoutException>[] waitStrategies = new WaitStrategy[maxResourceId];

        for ( ResourceType type : resourceTypes )
        {
            this.lockMaps[type.typeId()] = new ConcurrentHashMap<>( 16, 0.6f, 512 );
            waitStrategies[type.typeId()] = type.waitStrategy();
            this.resourceTypes[type.typeId()] = type;
        }
        // TODO Using a FlyweightPool here might still be more than what we actually need.
        // TODO We should investigate if a simple concurrent stack (aka. free-list) would
        // TODO be good enough. In fact, we could add the required fields for such a stack
        // TODO to the ForsetiClient objects themselves, making the stack garbage-free in
        // TODO the (presumably) common case of client re-use.
        clientPool = new ForsetiClientFlyweightPool( config, clock, lockMaps, waitStrategies );
    }

    /**
     * Create a new client to use to grab and release locks.
     */
    @Override
    public Client newClient()
    {
        // We check this volatile closed flag here, which may seem like a contention overhead, but as the time
        // of writing we apply pooling of transactions and in extension pooling of lock clients,
        // so this method is called very rarely.
        if ( closed )
        {
            throw new IllegalStateException( this + " already closed" );
        }

        ForsetiClient forsetiClient = clientPool.acquire();
        forsetiClient.reset();
        return forsetiClient;
    }

    @Override
    public void accept( Visitor out )
    {
        for ( int i = 0; i < lockMaps.length; i++ )
        {
            if ( lockMaps[i] != null )
            {
                ResourceType type = resourceTypes[i];
                for ( Map.Entry<Long,Lock> entry : lockMaps[i].entrySet() )
                {
                    Lock lock = entry.getValue();
                    out.visit( type, entry.getKey(), lock.describeWaitList(), 0, System.identityHashCode( lock ) );
                }
            }
        }
    }

    private int findMaxResourceId( ResourceType[] resourceTypes )
    {
        int max = 0;
        for ( ResourceType resourceType : resourceTypes )
        {
            max = Math.max( resourceType.typeId(), max );
        }
        return max + 1;
    }

    @Override
    public void close()
    {
        this.closed = true;
    }

    private static class ForsetiClientFlyweightPool extends LinkedQueuePool<ForsetiClient>
    {
        /** Client id counter **/
        private final AtomicInteger clientIds = new AtomicInteger( 0 );

        /** Re-use ids, forseti uses these in arrays, so we want to keep them low and not loose them. */
        private final Queue<Integer> unusedIds = new ConcurrentLinkedQueue<>();
        private final ConcurrentMap<Integer,ForsetiClient> clientsById = new ConcurrentHashMap<>();
        private final Config config;
        private final Clock clock;
        private final ConcurrentMap<Long,ForsetiLockManager.Lock>[] lockMaps;
        private final WaitStrategy<AcquireLockTimeoutException>[] waitStrategies;
        private final DeadlockResolutionStrategy deadlockResolutionStrategy = DeadlockStrategies.DEFAULT;

        ForsetiClientFlyweightPool( Config config, Clock clock, ConcurrentMap<Long,Lock>[] lockMaps,
                WaitStrategy<AcquireLockTimeoutException>[] waitStrategies )
        {
            super( 128, null );
            this.config = config;
            this.clock = clock;
            this.lockMaps = lockMaps;
            this.waitStrategies = waitStrategies;
        }

        @Override
        protected ForsetiClient create()
        {
            Integer id = unusedIds.poll();
            if ( id == null )
            {
                id = clientIds.getAndIncrement();
            }
            long lockAcquisitionTimeoutMillis = config.get( GraphDatabaseSettings.lock_acquisition_timeout ).toMillis();
            ForsetiClient client = new ForsetiClient( id, lockMaps, waitStrategies, this,
                    deadlockResolutionStrategy, clientsById::get, lockAcquisitionTimeoutMillis, clock );
            clientsById.put( id, client );
            return client;
        }

        @Override
        protected void dispose( ForsetiClient resource )
        {
            super.dispose( resource );
            clientsById.remove( resource.id() );
            if ( resource.id() < 1024 )
            {
                // Re-use all ids < 1024
                unusedIds.offer( resource.id() );
            }
        }
    }
}
