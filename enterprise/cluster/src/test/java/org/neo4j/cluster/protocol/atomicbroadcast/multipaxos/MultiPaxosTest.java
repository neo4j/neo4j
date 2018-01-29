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

import org.junit.Test;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.MultipleFailureLatencyStrategy;
import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.ScriptableNetworkFailureLatencyStrategy;
import org.neo4j.cluster.TestProtocolServer;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.monitoring.Monitors;

public class MultiPaxosTest
{
    @Test
    public void testFailure() throws Exception
    {
        ScriptableNetworkFailureLatencyStrategy networkLatency = new ScriptableNetworkFailureLatencyStrategy();
        NetworkMock network = new NetworkMock( NullLogService.getInstance(), new Monitors(), 50,
                new MultipleFailureLatencyStrategy( networkLatency ),
                new MessageTimeoutStrategy( new FixedTimeoutStrategy( 1000 ) ) );

        List<TestProtocolServer> nodes = new ArrayList<>();

        TestProtocolServer server = network.addServer( 1, URI.create( "cluster://server1" ) );
        server.newClient( Cluster.class ).create( "default" );
        network.tickUntilDone();
        nodes.add( server );

        for ( int i = 1; i < 3; i++ )
        {
            TestProtocolServer protocolServer = network.addServer( i + 1, new URI( "cluster://server" + (i + 1) ) );
            protocolServer.newClient( Cluster.class ).join( "default", new URI( "cluster://server1" ) );
            network.tick( 10 );
            nodes.add( protocolServer );
        }

        final AtomicBroadcast atomicBroadcast = nodes.get( 0 ).newClient( AtomicBroadcast.class );
        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();
        final AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer( objectStreamFactory,
                objectStreamFactory );
        atomicBroadcast.broadcast( serializer.broadcast( new DaPayload() ) );

        networkLatency.nodeIsDown( "cluster://server2" );
        networkLatency.nodeIsDown( "cluster://server3" );

        atomicBroadcast.broadcast( serializer.broadcast( new DaPayload() ) );
        network.tick( 100 );
        networkLatency.nodeIsUp( "cluster://server3" );
        network.tick( 1000 );

        for ( TestProtocolServer node : nodes )
        {
            node.newClient( Cluster.class ).leave();
            network.tick( 10 );
        }

    }

    private static final class DaPayload implements Serializable
    {
        private static final long serialVersionUID = -2896543854010391900L;
    }
}
