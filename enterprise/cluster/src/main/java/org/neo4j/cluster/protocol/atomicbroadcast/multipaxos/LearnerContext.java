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

package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

/**
 * Context for the Learner Paxos state machine.
 */
public class LearnerContext
{
    // Learner state
    private long lastDeliveredInstanceId = -1;
    private long lastLearnedInstanceId = -1;
    private long lastKnownLearnedInstanceInCluster = -1;

    public long getLastDeliveredInstanceId()
    {
        return lastDeliveredInstanceId;
    }

    public void setLastDeliveredInstanceId( long lastDeliveredInstanceId )
    {
        this.lastDeliveredInstanceId = lastDeliveredInstanceId;
    }

    public long getLastLearnedInstanceId()
    {
        return lastLearnedInstanceId;
    }

    public long getLastKnownLearnedInstanceInCluster()
    {
        return lastKnownLearnedInstanceInCluster;
    }

    public void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster )
    {
        this.lastKnownLearnedInstanceInCluster = lastKnownLearnedInstanceInCluster;
    }

    public void learnedInstanceId( long instanceId )
    {
        this.lastLearnedInstanceId = Math.max( lastLearnedInstanceId, instanceId );
        if ( lastLearnedInstanceId > lastKnownLearnedInstanceInCluster )
        {
            lastKnownLearnedInstanceInCluster = lastLearnedInstanceId;
        }
    }

    public boolean hasDeliveredAllKnownInstances()
    {
        return lastDeliveredInstanceId == lastKnownLearnedInstanceInCluster;
    }

    public void leave()
    {
        lastDeliveredInstanceId = -1;
        lastLearnedInstanceId = -1;
        lastKnownLearnedInstanceInCluster = -1;
    }
}
