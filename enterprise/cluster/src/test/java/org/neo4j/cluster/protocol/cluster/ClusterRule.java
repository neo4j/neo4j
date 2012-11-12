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

package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.TestProtocolServer;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule that sets up a mocked cluster.
 */
public class ClusterRule
        extends ExternalResource
{
    private NetworkMock network;
    private int nrOfNodes;
    private List<TestProtocolServer> nodes = new ArrayList<TestProtocolServer>();

    public ClusterRule( NetworkMock network, int nrOfNodes )
    {
        this.network = network;
        this.nrOfNodes = nrOfNodes;
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return super.apply( base, description );
    }

    @Override
    protected void before()
            throws Throwable
    {
        TestProtocolServer server = network.addServer( new URI( "cluster://server1" ) );
        server.newClient( Cluster.class ).create( "default" );
        network.tickUntilDone();
        nodes.add( server );

        for ( int i = 1; i < nrOfNodes; i++ )
        {
            TestProtocolServer protocolServer = network.addServer( new URI( "cluster://server" + (i + 1) ) );
            protocolServer.newClient( Cluster.class ).join( new URI( "cluster://server1" ) );
            network.tick( 10 );
            nodes.add( protocolServer );
        }

        super.before();
    }

    @Override
    protected void after()
    {
        for ( TestProtocolServer node : nodes )
        {
            node.newClient( Cluster.class ).leave();
            network.tick( 10 );
        }

        super.after();
    }

    public List<TestProtocolServer> getNodes()
    {
        return nodes;
    }
}
