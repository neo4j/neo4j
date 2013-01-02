/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.equalTo;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.cluster.FixedNetworkLatencyStrategy;
import org.neo4j.cluster.MultipleFailureLatencyStrategy;
import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.cluster.protocol.cluster.ClusterRule;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;

/**
 * TODO
 */
public class MultiPaxosTest
{
    private NetworkMock network = new NetworkMock( 50,
            new MultipleFailureLatencyStrategy( new FixedNetworkLatencyStrategy( 0 ) ),
            new MessageTimeoutStrategy( new FixedTimeoutStrategy( 1000 ) ) );

    @Rule
    public ClusterRule cluster = new ClusterRule( network, 3 );

    @Test
    public void testDecision()
            throws ExecutionException, InterruptedException, URISyntaxException
    {
        Map<String, String> map1 = new AtomicBroadcastMap<String, String>( cluster.getNodes().get( 0 ).newClient(
                AtomicBroadcast.class ), cluster.getNodes().get( 0 ).newClient( Snapshot.class ) );
        Map<String, String> map2 = new AtomicBroadcastMap<String, String>( cluster.getNodes().get( 1 ).newClient(
                AtomicBroadcast.class ), cluster.getNodes().get( 1 ).newClient( Snapshot.class ) );

        map1.put( "foo", "bar" );

        network.tick( 30 );
        Object foo = map1.get( "foo" );
        Assert.assertThat( foo.toString(), equalTo( "bar" ) );

        map1.put( "bar", "foo" );
        network.tick( 30 );
        Object bar = map2.get( "bar" );
        Assert.assertThat( bar.toString(), equalTo( "foo" ) );

        map1.put( "foo", "bar2" );
        network.tick( 30 );
        foo = map2.get( "foo" );
        Assert.assertThat( foo.toString(), equalTo( "bar2" ) );

        map1.clear();
        network.tick( 30 );
        foo = map2.get( "foo" );
        Assert.assertThat( foo, CoreMatchers.nullValue() );
    }
}
