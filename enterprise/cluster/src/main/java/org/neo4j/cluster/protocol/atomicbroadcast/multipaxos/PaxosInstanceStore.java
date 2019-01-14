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

    private int queued;
    private Queue<InstanceId> delivered = new LinkedList<>();
    private Map<InstanceId, PaxosInstance> instances = new HashMap<>();
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

        return instances.computeIfAbsent( instanceId, i -> new PaxosInstance( this, i ) );
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
        snapshotStore.delivered = new LinkedList<>( delivered );
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
        return instances.equals( that.instances );
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
