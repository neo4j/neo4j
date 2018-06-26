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
package org.neo4j.causalclustering.core;

import org.junit.Test;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import static org.junit.Assert.assertEquals;

public class HostnamePortAsListenAddressTest
{
    @Test
    public void shouldIgnoreRange()
    {

        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, "localhost:4343-4347" ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );

        assertEquals( new ListenSocketAddress( "localhost", 4343 ), listenSocketAddress );
    }

    @Test( expected = InvalidSettingException.class )
    public void shouldThrowInvalidSettingsExceptionOnEmptyConfig()
    {
        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, "" ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );
        assertEquals( OnlineBackupSettings.online_backup_server.getDefaultValue(), listenSocketAddress.toString() );
    }

    @Test( expected = InvalidSettingException.class )
    public void shouldThrowInvalidSettingsExceptionOnBadFormat()
    {
        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, "localhost" ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );
        assertEquals( OnlineBackupSettings.online_backup_server.getDefaultValue(), listenSocketAddress.toString() );
    }

    @Test
    public void shouldHandleIpv4()
    {
        String ipv4host = "localhost";
        int port = 1234;
        String ipv4 = ipv4host + ":" + port;

        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, ipv4 ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );

        assertEquals( new ListenSocketAddress( ipv4host, port ), listenSocketAddress );
    }

    @Test
    public void shouldHandleIpv6()
    {
        String ipv6host = "2001:cdba:0000:0000:0000:0000:3257:9652";
        int port = 1234;
        String ipv6 = '[' + ipv6host + "]:" + port;

        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, ipv6 ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );

        assertEquals( new ListenSocketAddress( ipv6host, port ), listenSocketAddress );
    }
}
