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
package org.neo4j.kernel.ha.lock.forseti;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.FlyweightPool;
import org.neo4j.kernel.impl.util.StringLogger;
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
 */
public class ForsetiLockManager extends LifecycleAdapter implements Locks
{
    /** This is Forsetis internal lock API, which it uses to do deadlock detection. */
    interface Lock
    {
        void copyHolderWaitListsInto( SimpleBitSet waitList );
        int holderWaitListSize();
        boolean anyHolderIsWaitingFor( int client );

        /** For debugging */
        String describeWaitList();
    }

    /** Pointers to lock maps, one array per resource type. */
    private final ConcurrentMap[] lockMaps;

    /** Wait strategies per resource type */
    private final WaitStrategy[] waitStrategies;

    /** Pool forseti clients. */
    private final FlyweightPool<ForsetiClient> clientPool = new FlyweightPool<ForsetiClient>( 128 )
    {
        /** Client id counter **/
        private final AtomicInteger clientIds = new AtomicInteger( 0 );

        /** Re-use ids, forseti uses these in arrays, so we want to keep them low and not loose them. */
        private final Queue<Integer> unusedIds = new ConcurrentLinkedDeque<>();

        @Override
        protected ForsetiClient create()
        {
            Integer id = unusedIds.poll();
            if(id == null)
            {
                id = clientIds.getAndIncrement();
            }
            return new ForsetiClient(id, lockMaps, waitStrategies, clientPool);
        }

        @Override
        protected void dispose( ForsetiClient resource )
        {
            super.dispose( resource );
            if(resource.id() < 1024)
            {
                // Re-use all ids < 1024
                unusedIds.offer( resource.id() );
            }
        }
    };

    public ForsetiLockManager( ResourceType... resourceTypes )
    {
        lockMaps = new ConcurrentMap[findMaxResourceId( resourceTypes )];
        waitStrategies = new WaitStrategy[findMaxResourceId( resourceTypes )];

        for ( ResourceType resourceDefinition : resourceTypes )
        {
            lockMaps[resourceDefinition.typeId()] = new ConcurrentHashMap(16, 0.6f, 512);
            waitStrategies[resourceDefinition.typeId()] = resourceDefinition.waitStrategy();
        }
    }

    @Override
    public Client newClient()
    {
        return clientPool.acquire();
    }

    @Override
    public void dumpLocks( StringLogger out )
    {
        for ( int i = 0; i < lockMaps.length; i++ )
        {
            if(lockMaps[i] != null)
            {
                out.info( "ResouceType[" + i + "]" );
                out.info( "==============" );
                for ( Object raw : lockMaps[i].entrySet() )
                {
                    Map.Entry<Long, Lock> entry = (Map.Entry<Long, Lock>)raw;
                    out.info( "Resource["+entry.getKey()+"]: " + entry.getValue().describeWaitList() );
                }
            }
        }
    }

    @Override
    public String implementationId()
    {
        return "forseti";
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
}