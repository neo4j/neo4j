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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.Pool;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.kernel.impl.util.concurrent.WaitStrategy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * <h1>Forseti, the Nordic god of justice</h1>
 *
 * Forseti is a lock manager using the dreadlocks deadlock detection algorithm, which means
 * deadlock detection does not require complex RAG traversal and can be found in O(1).
 *
 * In the best case, Forseti acquires a lock in one CAS instruction, and scales linearly with the number of cores.
 * However, since it uses a shared-memory approach, it will most likely degrade in use cases where there is high
 * contention and a very large number of sockets running the database.
 *
 * As such, it is optimized for servers with up to, say, 16 cores across 2 sockets. Past that other strategies such
 * as centralized lock services using message passing may yield better results.
 *
 * <h2>Locking algorithm</h2>
 *
 * Forseti is used by acquiring clients, which act as agents on behalf of whoever wants to grab locks. The clients
 * have access to a central map of locks.
 *
 * To grab a lock, a client must insert itself into the holder list of the lock it wants. The lock may either be a
 * shared lock or an exclusive lock. In the case of a shared lock, the client simply appends itself to the holder list.
 * In the case of an exclusive lock, the client has it's own unique exclusive lock, which it must put into the lock map
 * using a CAS operation.
 *
 * Once the client is in the holder list, it has the lock.
 *
 * <h2>Deadlock detection</h2>
 *
 * Each Client maintains a waiting-for list, which by default always contains the client itself. This list indicates
 * which other clients are blocking our progress. By default, then, if client A is waiting for no-one, its waiting-for
 * list will contain only itself:
 *
 * A.waitlist = [A]
 *
 * Once the client is blocked by someone else, it will copy this someones entire wait list into it's own. Assuming A
 * becomes blocked by B, and B has a wait list of:
 *
 * B.waitlist = [B]
 *
 * Then A will modify is's wait list as:
 *
 * A.waitlist = [A] U [B] => [A,B]
 *
 * It will do this in a loop, continiously figuring out the union of wait lists for all clients it waits for. The magic
 * then happens whenever one of those clients become blocked on client A. Assuming client B now has to wait for A,
 * it will also perform a union of A's wait list (which is [A,B] at this point):
 *
 * B.waitlist = [B] U [A,B]
 *
 * As it performs this union, B will find itself in A's waiting list, and when it does, it has detected a deadlock.
 *
 *
 * <h2>Future work</h2>
 *
 * We have at least one type of lock (SchemaLock) that can be held concurrently by several hundred transactions. It may
 * be worth investigating fat locks, or in any case optimize the current way SharedLock adds and removes clients from
 * its holder list.
 *
 * The maps used by forseti should be replaced by faster concurrent maps, perhaps a striped hopscotch map or something
 * similar.
 */
public class ForsetiLockManager extends LifecycleAdapter implements Locks
{
    /** This is Forsetis internal lock API, which it uses to do deadlock detection. */
    interface Lock
    {
        void copyHolderWaitListsInto( SimpleBitSet waitList );
        int holderWaitListSize();
        boolean anyHolderIsWaitingFor( int client );

        /** For introspection and error messages */
        String describeWaitList();

    }

    /** Pointers to lock maps, one array per resource type. */
    private final ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps;

    /** Reverse lookup resource types by id, used for introspection */
    private final ResourceType[] resourceTypes;

    /** Pool forseti clients. */
    private final Pool<ForsetiClient> clientPool;
    private volatile boolean closed;

    @SuppressWarnings( "unchecked" )
    public ForsetiLockManager( ResourceType... resourceTypes )
    {
        int maxResourceId = findMaxResourceId( resourceTypes );
        this.lockMaps = new ConcurrentMap[maxResourceId];
        this.resourceTypes = new ResourceType[maxResourceId];

        /* Wait strategies per resource type */
        WaitStrategy<AcquireLockTimeoutException>[] waitStrategies = new WaitStrategy[maxResourceId];

        for ( ResourceType type : resourceTypes )
        {
            this.lockMaps[type.typeId()] = new ConcurrentHashMap<>(16, 0.6f, 512);
            waitStrategies[type.typeId()] = type.waitStrategy();
            this.resourceTypes[type.typeId()] = type;
        }
        // TODO Using a FlyweightPool here might still be more than what we actually need.
        // TODO We should investigate if a simple concurrent stack (aka. free-list) would
        // TODO be good enough. In fact, we could add the required fields for such a stack
        // TODO to the ForsetiClient objects themselves, making the stack garbage-free in
        // TODO the (presumably) common case of client re-use.
        clientPool = new ForsetiClientFlyweightPool( lockMaps, waitStrategies );
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
            if(lockMaps[i] != null)
            {
                ResourceType type = resourceTypes[i];
                for ( Map.Entry<Long, Lock> entry : lockMaps[i].entrySet() )
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
    public void shutdown()
    {
        this.closed = true;
    }

    private static class ForsetiClientFlyweightPool extends LinkedQueuePool<ForsetiClient>
    {
        /** Client id counter **/
        private final AtomicInteger clientIds = new AtomicInteger( 0 );

        /** Re-use ids, forseti uses these in arrays, so we want to keep them low and not loose them. */
        // TODO we could use a synchronised SimpleBitSet instead, since we know that we only care about reusing a very limited set of integers.
        private final Queue<Integer> unusedIds = new ConcurrentLinkedQueue<>();
        private final ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps;
        private final WaitStrategy<AcquireLockTimeoutException>[] waitStrategies;

        public ForsetiClientFlyweightPool(
                ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps,
                WaitStrategy<AcquireLockTimeoutException>[] waitStrategies )
        {
            super( 128, null);
            this.lockMaps = lockMaps;
            this.waitStrategies = waitStrategies;
        }

        @Override
        protected ForsetiClient create()
        {
            Integer id = unusedIds.poll();
            if(id == null)
            {
                id = clientIds.getAndIncrement();
            }
            return new ForsetiClient( id, lockMaps, waitStrategies, this );
        }

        @Override
        protected void dispose( ForsetiClient resource )
        {
            super.dispose( resource );
            if ( resource.id() < 1024 )
            {
                // Re-use all ids < 1024
                unusedIds.offer( resource.id() );
            }
        }
    }
}
