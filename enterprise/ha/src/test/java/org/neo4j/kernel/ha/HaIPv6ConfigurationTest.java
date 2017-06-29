/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assume.assumeTrue;

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
    public void testClusterWithLinkLocalAddress() throws Throwable
    {
        boolean foundAnIpv6LinkLocalAddress = false;
        InetAddress inetAddress = null;

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
                    foundAnIpv6LinkLocalAddress = true;
                    break;
                }
            }
        }

        assumeTrue( foundAnIpv6LinkLocalAddress );

        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( inetAddress.getHostAddress(), 5000 ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( inetAddress.getHostAddress(), 5000 ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", 6000 ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .newGraphDatabase();

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

        db.shutdown();
    }

    private String ipv6HostPortSetting( String address, int port )
    {
        return "[" + address + "]:" + port;
    }
}
