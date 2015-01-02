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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Store of Paxos instances, from a proposer perspective
 */
public class PaxosInstanceStore
{
    private static final int MAX_STORED = 100;

    private int queued = 0;
    private Queue<InstanceId> delivered = new LinkedList<InstanceId>();
    private Map<InstanceId, PaxosInstance> instances = new HashMap<InstanceId, PaxosInstance>();

    public PaxosInstance getPaxosInstance( InstanceId instanceId )
    {
        if ( instanceId == null )
        {
            throw new NullPointerException( "InstanceId may not be null" );
        }

        PaxosInstance instance = instances.get( instanceId );
        if ( instance == null )
        {
            instance = new PaxosInstance( this, instanceId );
            instances.put( instanceId, instance );
        }
        return instance;
    }

    public void delivered( InstanceId instanceId )
    {
        queued++;
        delivered.offer( instanceId );

        if ( queued > MAX_STORED )
        {
            InstanceId removeInstanceId = delivered.poll();
            instances.remove( removeInstanceId );
            queued--;
        }
    }

    public void leave()
    {
        queued = 0;
        delivered.clear();
        instances.clear();
    }

    public PaxosInstanceStore snapshot()
    {
        PaxosInstanceStore snapshotStore = new PaxosInstanceStore();
        snapshotStore.queued = queued;
        snapshotStore.delivered = new LinkedList<>(delivered);
        for ( Map.Entry<InstanceId, PaxosInstance> instance : instances.entrySet() )
        {
            snapshotStore.instances.put( instance.getKey(), instance.getValue().snapshot(snapshotStore) );
        }
        return snapshotStore;
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

        PaxosInstanceStore that = (PaxosInstanceStore) o;

        if ( queued != that.queued )
        {
            return false;
        }
        if ( !delivered.equals( that.delivered ) )
        {
            return false;
        }
        if ( !instances.equals( that.instances ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = queued;
        result = 31 * result + delivered.hashCode();
        result = 31 * result + instances.hashCode();
        return result;
    }
}
