/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.MasterServer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SwitchToMasterTest
{
    @Test
    public void switchToMasterShouldUseConfigSettingIfSuitable() throws Exception
    {
        // given
        Config config = Config.defaults(
                stringMap( ClusterSettings.server_id.name(), "1", HaSettings.ha_server.name(), "192.168.1.99:6001" ) );
        URI me = new URI( "ha://127.0.0.1" );

        MasterServer masterServer = mock( MasterServer.class );

        // when
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( "192.168.1.1", 6001 ) );

        URI result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://192.168.1.99:6001?serverId=1", result.toString() );
    }

    @Test
    public void switchToMasterShouldUseIPv6ConfigSettingIfSuitable() throws Exception
    {
        // given
        Config config = Config.defaults(
                stringMap( ClusterSettings.server_id.name(), "1", HaSettings.ha_server.name(), "[fe80::1]:6001" ) );
        URI me = new URI( "ha://[::1]" );

        MasterServer masterServer = mock( MasterServer.class );

        // when
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( "[fe80::1]", 6001 ) );

        URI result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://[fe80::1]:6001?serverId=1", result.toString() );
    }

    @Test
    public void switchToMasterShouldIgnoreWildcardInConfig() throws Exception
    {
        // SwitchToMaster is used to advertise to the rest of the cluster and advertising 0.0.0.0 makes no sense

        // given
        Config config = Config.defaults(
                stringMap( ClusterSettings.server_id.name(), "1", HaSettings.ha_server.name(), "0.0.0.0:6001" ) );
        URI me = new URI( "ha://127.0.0.1" );

        MasterServer masterServer = mock( MasterServer.class );

        // when
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( "192.168.1.1", 6001 ) );

        URI result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://192.168.1.1:6001?serverId=1", result.toString() );

        // when masterServer is 0.0.0.0
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( 6001 ) );

        result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://127.0.0.1:6001?serverId=1", result.toString() );
    }

    @Test
    public void switchToMasterShouldIgnoreIPv6WildcardInConfig() throws Exception
    {
        // SwitchToMaster is used to advertise to the rest of the cluster and advertising 0.0.0.0 makes no sense

        // given
        Config config = Config.defaults(
                stringMap( ClusterSettings.server_id.name(), "1", HaSettings.ha_server.name(), "[::]:6001" ) );
        URI me = new URI( "ha://[::1]" );

        MasterServer masterServer = mock( MasterServer.class );

        // when
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( "[fe80::1]", 6001 ) );

        URI result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://[fe80:0:0:0:0:0:0:1]:6001?serverId=1", result.toString() );

        // when masterServer is 0.0.0.0
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( 6001 ) );

        result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://[::1]:6001?serverId=1", result.toString() );
    }

    @Test
    public void switchToMasterShouldHandleNoIpInConfig() throws Exception
    {
        Config config = Config.defaults(
                stringMap( ClusterSettings.server_id.name(), "1", HaSettings.ha_server.name(), ":6001" ) );

        MasterServer masterServer = mock( MasterServer.class );
        URI me = new URI( "ha://127.0.0.1" );

        // when
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( "192.168.1.1", 6001 ) );

        URI result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://192.168.1.1:6001?serverId=1", result.toString() );

        // when masterServer is 0.0.0.0
        when( masterServer.getSocketAddress() ).thenReturn( new InetSocketAddress( 6001 ) );

        result = SwitchToMaster.getMasterUri( me, masterServer, config );

        // then
        assertEquals( "Wrong address", "ha://127.0.0.1:6001?serverId=1", result.toString() );
    }
}
