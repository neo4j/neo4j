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
package org.neo4j.kernel.ha.cluster;

import java.net.InetSocketAddress;
import java.net.URI;

import org.junit.Test;

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
        Config config = new Config(
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
    public void switchToMasterShouldIgnoreWildcardInConfig() throws Exception
    {
        // SwitchToMaster is used to advertise to the rest of the cluster and advertising 0.0.0.0 makes no sense

        // given
        Config config = new Config(
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
    public void switchToMasterShouldHandleNoIpInConfig() throws Exception
    {
        Config config = new Config(
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
