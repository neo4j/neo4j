/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;

@ExtendWith( TestDirectoryExtension.class )
class ConvertersTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldSortFilesByNumberCleverly() throws Exception
    {
        // GIVEN
        File file1 = existenceOfFile( "file1" );
        File file123 = existenceOfFile( "file123" );
        File file12 = existenceOfFile( "file12" );
        File file2 = existenceOfFile( "file2" );
        File file32 = existenceOfFile( "file32" );

        // WHEN
        File[] files = regexFiles( true ).apply( directory.file( "file.*" ).getAbsolutePath() );

        // THEN
        assertArrayEquals( new File[]{file1, file2, file12, file32, file123}, files );
    }

    @Test
    void shouldConvertFullHostnamePortToAdvertisedSocketAddress()
    {
        HostnamePort hostnamePort = new HostnamePort( "neo4j.com", 7474 );

        AdvertisedSocketAddress address = Converters.toAdvertisedSocketAddress( hostnamePort, "", -1 );

        assertEquals( new AdvertisedSocketAddress( "neo4j.com", 7474 ), address );
    }

    @Test
    void shouldConvertFullIpV6HostnamePortToAdvertisedSocketAddress()
    {
        HostnamePort hostnamePort = new HostnamePort( "[fe80:1:2:3:4::5:123]", 7687 );

        AdvertisedSocketAddress address = Converters.toAdvertisedSocketAddress( hostnamePort, "", -1 );

        assertEquals( new AdvertisedSocketAddress( "fe80:1:2:3:4::5:123", 7687 ), address );
    }

    @Test
    void shouldConvertHostnamePortWithOnlyHostnameToAdvertisedSocketAddress()
    {
        HostnamePort hostnamePort = new HostnamePort( "hostname.neo4j.org" );

        AdvertisedSocketAddress address = Converters.toAdvertisedSocketAddress( hostnamePort, "", 4242 );

        assertEquals( new AdvertisedSocketAddress( "hostname.neo4j.org", 4242 ), address );
    }

    @Test
    void shouldConvertHostnamePortWithOnlyIpV6HostnameToAdvertisedSocketAddress()
    {
        HostnamePort hostnamePort = new HostnamePort( "[fe80:1:2:3:4::5]" );

        AdvertisedSocketAddress address = Converters.toAdvertisedSocketAddress( hostnamePort, "", 1234 );

        assertEquals( new AdvertisedSocketAddress( "fe80:1:2:3:4::5", 1234 ), address );
    }

    @Test
    void shouldConvertHostnamePortWithOnlyPortToAdvertisedSocketAddress()
    {
        HostnamePort hostnamePort = new HostnamePort( ":7687" );

        AdvertisedSocketAddress address = Converters.toAdvertisedSocketAddress( hostnamePort, "neo4j.com", -1 );

        assertEquals( new AdvertisedSocketAddress( "neo4j.com", 7687 ), address );
    }

    private File existenceOfFile( String name ) throws IOException
    {
        File file = directory.file( name );
        assertTrue( file.createNewFile() );
        return file;
    }
}
