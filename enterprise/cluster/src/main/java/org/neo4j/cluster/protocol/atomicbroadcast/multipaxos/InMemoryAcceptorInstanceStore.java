/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * In memory version of an acceptor instance store.
 */
public class InMemoryAcceptorInstanceStore
        implements AcceptorInstanceStore
{
    private final Map<InstanceId, AcceptorInstance> instances;
    private final BlockingQueue<InstanceId> currentInstances;

    private long lastDeliveredInstanceId;

    public InMemoryAcceptorInstanceStore()
    {
        this( new HashMap<>(), new ArrayBlockingQueue<>( 1000 ), -1 );
    }

    private InMemoryAcceptorInstanceStore( Map<InstanceId,AcceptorInstance> instances,
            BlockingQueue<InstanceId> currentInstances, long lastDeliveredInstanceId )
    {
        this.instances = instances;
        this.lastDeliveredInstanceId = lastDeliveredInstanceId;
        this.currentInstances = currentInstances;
    }

    @Override
    public AcceptorInstance getAcceptorInstance( InstanceId instanceId )
    {
        AcceptorInstance instance = instances.get( instanceId );
        if ( instance == null )
        {
            instance = new AcceptorInstance();
            instances.put( instanceId, instance );

            // Make sure we only keep a maximum number of instances, to not run out of memory
            if ( !currentInstances.offer( instanceId ) )
            {
                instances.remove( currentInstances.poll() );
                currentInstances.offer( instanceId );
            }
        }

        return instance;
    }

    @Override
    public void promise( AcceptorInstance instance, long ballot )
    {
        instance.promise( ballot );
    }

    @Override
    public void accept( AcceptorInstance instance, Object value )
    {
        instance.accept( value );
    }

    @Override
    public void lastDelivered( InstanceId instanceId )
    {
        lastDeliveredInstanceId = instanceId.getId();
    }

    @Override
    public void clear()
    {
        instances.clear();
    }

    public InMemoryAcceptorInstanceStore snapshot()
    {
        return new InMemoryAcceptorInstanceStore( new HashMap<>( instances ),
                new ArrayBlockingQueue<>( currentInstances.size() + currentInstances.remainingCapacity(), false,
                        currentInstances ), lastDeliveredInstanceId );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InMemoryAcceptorInstanceStore that = (InMemoryAcceptorInstanceStore) o;

        if ( lastDeliveredInstanceId != that.lastDeliveredInstanceId )
        {
            return false;
        }
        return instances.equals( that.instances );
    }

    @Override
    public int hashCode()
    {
        int result = instances.hashCode();
        result = 31 * result + (int) (lastDeliveredInstanceId ^ (lastDeliveredInstanceId >>> 32));
        return result;
    }
}
