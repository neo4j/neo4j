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

import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.TestDirectory;

/**
 * Test various IPv6 configuration options on a single HA instance.
 */
public class HaIPv6ConfigurationIT
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void testClusterWithLocalhostAddresses() throws Throwable
    {
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::1", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
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
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( inetAddress.getHostAddress(), clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( inetAddress.getHostAddress(), clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
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
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.makeGraphDbDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::", clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
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
