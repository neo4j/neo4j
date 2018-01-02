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
package org.neo4j.cluster.protocol.heartbeat;

import java.util.List;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;

/**
 * Context used by the {@link HeartbeatState} state machine.
 */
public interface HeartbeatContext
    extends TimeoutsContext, ConfigurationContext, LoggingContext
{
    void started();

    /**
     * @return True iff the node was suspected
     */
    boolean alive( final InstanceId node );

    void suspect( final InstanceId node );

    void suspicions( InstanceId from, Set<InstanceId> suspicions );

    Set<InstanceId> getFailed();

    Iterable<InstanceId> getAlive();

    void addHeartbeatListener( HeartbeatListener listener );

    void removeHeartbeatListener( HeartbeatListener listener );

    void serverLeftCluster( InstanceId node );

    boolean isFailed( InstanceId node );

    List<InstanceId> getSuspicionsOf( InstanceId server );

    Set<InstanceId> getSuspicionsFor( InstanceId uri );

    Iterable<InstanceId> getOtherInstances();

    long getLastKnownLearnedInstanceInCluster();

    long getLastLearnedInstanceId();
}
