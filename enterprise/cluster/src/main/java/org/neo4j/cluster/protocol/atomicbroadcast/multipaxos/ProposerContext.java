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

import java.net.URI;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;

/**
 * Context used by {@link ProposerState} state machine.
 */
public interface ProposerContext
    extends TimeoutsContext, LoggingContext, ConfigurationContext
{
    InstanceId newInstanceId( );

    PaxosInstance getPaxosInstance( InstanceId instanceId );

    void pendingValue( Message message );

    void bookInstance( InstanceId instanceId, Message message );

    int nrOfBookedInstances();

    boolean canBookInstance();

    Message getBookedInstance( InstanceId id );

    Message<ProposerMessage> unbookInstance( InstanceId id );

    void patchBookedInstances( ClusterMessage.ConfigurationChangeState value );

    int getMinimumQuorumSize( List<URI> acceptors );

    boolean hasPendingValues();

    Message popPendingValue();

    void leave();

    List<URI> getAcceptors();
}
