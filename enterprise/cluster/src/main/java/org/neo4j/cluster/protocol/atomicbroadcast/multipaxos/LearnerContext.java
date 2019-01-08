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

import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;

/**
 * Context for the Learner Paxos state machine.
 */
public interface LearnerContext
    extends TimeoutsContext, LoggingContext, ConfigurationContext
{
    /*
     * How many instances the coordinator will allow to be open before taking drastic action and delivering them
     */
    int LEARN_GAP_THRESHOLD = 10;

    long getLastDeliveredInstanceId();

    void setLastDeliveredInstanceId( long lastDeliveredInstanceId );

    long getLastLearnedInstanceId();

    long getLastKnownLearnedInstanceInCluster();

    void learnedInstanceId( long instanceId );

    boolean hasDeliveredAllKnownInstances();

    void leave();

    PaxosInstance getPaxosInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId );

    AtomicBroadcastSerializer newSerializer();

    Iterable<org.neo4j.cluster.InstanceId> getAlive();

    void setNextInstanceId( long id );

    void notifyLearnMiss( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId );

    org.neo4j.cluster.InstanceId getLastKnownAliveUpToDateInstance();

    void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster, org.neo4j.cluster.InstanceId instanceId );
}
