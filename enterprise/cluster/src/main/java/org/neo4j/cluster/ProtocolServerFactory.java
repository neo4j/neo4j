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
package org.neo4j.cluster;

import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.timeout.TimeoutStrategy;

/**
 * Factory for instantiating ProtocolServers.
 *
 * @see ProtocolServer
 */
public interface ProtocolServerFactory
{
    ProtocolServer newProtocolServer( InstanceId me, int maxAcceptors, TimeoutStrategy timeouts, MessageSource input, MessageSender output,
                                      AcceptorInstanceStore acceptorInstanceStore,
                                      ElectionCredentialsProvider electionCredentialsProvider,
                                      Executor stateMachineExecutor,
                                      ObjectInputStreamFactory objectInputStreamFactory,
                                      ObjectOutputStreamFactory objectOutputStreamFactory );
}
