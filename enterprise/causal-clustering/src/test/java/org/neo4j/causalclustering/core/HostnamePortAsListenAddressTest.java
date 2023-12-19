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
package org.neo4j.causalclustering.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class HostnamePortAsListenAddressTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[Should ignore port range correctly]", "localhost", 4343, OptionalInt.of( 4646 ), false },
                        { "[Should handle hostname with dashes]", "foo-bar", 1234, OptionalInt.empty(), false },
                        { "[Should handle standard hostname with port]", "localhost", 251, OptionalInt.empty(), false },
                        { "[Should handle hostname with tld ext]", "neo4j.org", 1212, OptionalInt.empty(), false },
                        { "[Should ignore port range for hostname with tld ext]", "neo4j.org", 1212, OptionalInt.of( 2121 ), false },
                        { "[Should handle hostname with sub-domain]", "test.neo4j.org", 1212, OptionalInt.empty(), false },
                        { "[Should handle ipv4 hostname]", "8.8.8.8", 1212, OptionalInt.of( 2121 ), false },
                        { "[Should handle ipv6 hostname]", "[2001:cdba:0000:0000:0000:0000:3257:9652]", 1212, OptionalInt.empty(), true },
                        { "[Should handle ipv6 hostname with port range]", "[2001:cdba::3257:9652]", 1212, OptionalInt.of( 2121 ), true }
                }
        );
    }

    private final String hostname;
    private final int port;
    private final OptionalInt portRange;
    private final boolean isIpV6;

    public HostnamePortAsListenAddressTest( String ignoredName, String hostname, int port, OptionalInt portRange, boolean isIpV6 )
    {
        this.hostname = hostname;
        this.port = port;
        this.portRange = portRange;
        this.isIpV6 = isIpV6;
    }

    private String combinedHostname()
    {
        String portRangeStr = portRange.isPresent() ? "-" + portRange.getAsInt() : "";
        return hostname + ":" + port + portRangeStr;
    }

    private String getSanitizedHostname()
    {
        if ( isIpV6 )
        {
            if ( !( hostname.startsWith( "[" ) && hostname.endsWith( "]" ) ) )
            {
                throw new IllegalArgumentException( "Test indicates an IpV6 hostname and port but isn't surrounded by []" );
            }
            return hostname.substring( 1, hostname.length() - 1 );
        }
        else
        {
            return hostname;
        }
    }

    @Test
    public void shouldParseHostnamePortCorrectly()
    {
        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_server, combinedHostname() ).build();
        ListenSocketAddress listenSocketAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );

        assertEquals( new ListenSocketAddress( getSanitizedHostname(), port ), listenSocketAddress );
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
}
