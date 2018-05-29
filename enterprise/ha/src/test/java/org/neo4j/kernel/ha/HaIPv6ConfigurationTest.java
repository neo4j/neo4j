/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

/**
 * Test various IPv6 configuration options on a single HA instance.
 */
public class HaIPv6ConfigurationTest
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void testClusterWithLocalhostAddresses() throws Throwable
    {
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::1", 5000 ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", 5000 ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::1", 6000 ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void testClusterWithLinkLocalAddress() throws Throwable
    {
        InetAddress inetAddress;

        Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
        while ( nics.hasMoreElements() )
        {
            NetworkInterface nic = nics.nextElement();
            Enumeration<InetAddress> inetAddresses = nic.getInetAddresses();
            while ( inetAddresses.hasMoreElements() )
            {
                inetAddress = inetAddresses.nextElement();
                if ( inetAddress instanceof Inet6Address && inetAddress.isLinkLocalAddress() )
                {
                    try
                    {
                        if ( inetAddress.isReachable( 1000 ) )
                        {
                            testWithAddress( inetAddress );
                        }
                    }
                    catch ( ConnectException e )
                    {
                        // fine, just ignore
                    }
                }
            }
        }
    }

    private void testWithAddress( InetAddress inetAddress ) throws Exception
    {
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( inetAddress.getHostAddress(), 5000 ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( inetAddress.getHostAddress(), 5000 ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", 6000 ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void testClusterWithWildcardAddresses() throws Throwable
    {
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::", 5000 ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", 5000 ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", 6000 ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    private String ipv6HostPortSetting( String address, int port )
    {
        return "[" + address + "]:" + port;
    }
}
