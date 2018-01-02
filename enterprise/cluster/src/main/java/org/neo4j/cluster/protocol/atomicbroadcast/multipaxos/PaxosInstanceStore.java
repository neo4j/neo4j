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
    /*
     * This represents the number of delivered paxos instances to keep in memory - it is essentially the length
     * of the delivered queue and therefore the size of the instances map.
     * The use of this is to server Learn requests. While learning is the final phase of the Paxos algo, it is also
     * executed when an instance needs to catch up with events that happened in the cluster but it missed because it
     * was temporarily disconnected.
     * MAX_STORED therefore represents the maximum number of paxos instances a lagging member may be lagging behind
     * and be able to recover from. This number must be large enough to account for a few minutes of downtime (like
     * an extra long GC pause) during which intense broadcasting happens.
     * Assuming 2 paxos instances per second gives us 120 instances per minute or 1200 instances for 10 minutes of
     * downtime. We about 5x that here since instances are relatively small in size and we can spare the memory.
     */
    // TODO (quite challenging and interesting) Prune this queue aggressively.
    /*
     * This queue, as it stands now, will always remain at full capacity. However, if we could figure out that
     * all cluster members have learned a particular paxos instance then we can remove it since no one will ever
     * request it. That way the MAX_STORED value should be reached only when an instance is know to be in the failed
     * state.
     */

    private static final int MAX_STORED = 5000;

    private int queued = 0;
    private Queue<InstanceId> delivered = new LinkedList<InstanceId>();
    private Map<InstanceId, PaxosInstance> instances = new HashMap<InstanceId, PaxosInstance>();
    private final int maxInstancesToStore;

    public PaxosInstanceStore()
    {
        this( MAX_STORED );
    }

    public PaxosInstanceStore( int maxInstancesToStore )
    {
        this.maxInstancesToStore = maxInstancesToStore;
    }

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

        if ( queued > maxInstancesToStore )
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
