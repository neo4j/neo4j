/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.values.storable;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class TimeZonesTest
{
    @Test
    void weSupportAllJavaZoneIds()
    {
        ZoneId.getAvailableZoneIds().forEach( s ->
        {
            short num = TimeZones.map( s );
            assertThat( num ).as( "Our time zone table does not have a mapping for " + s ).isGreaterThanOrEqualTo( (short) 0 );

            String nameFromTable = TimeZones.map( num );
            if ( !s.equals( nameFromTable ) )
            {
                // The test is running on an older Java version and `s` has been removed since, thus it points to a different zone now.
                // That zone should point to itself, however.
                assertThat( TimeZones.map( TimeZones.map( nameFromTable ) ) ).as(
                        "Our time zone table has inconsistent mapping for " + nameFromTable ).isEqualTo( nameFromTable );
            }
        } );
    }

    @Test
    void weSupportDeletedZoneIdEastSaskatchewan()
    {
        try
        {
            short eastSaskatchewan = TimeZones.map( "Canada/East-Saskatchewan" );
            assertThat( TimeZones.map( eastSaskatchewan ) ).as(
                    "Our time zone table does not remap Canada/East-Saskatchewan to Canada/Saskatchewan" ).isEqualTo( "Canada/Saskatchewan" );
        }
        catch ( IllegalArgumentException e )
        {
            fail( "Our time zone table does not support Canada/East-Saskatchewan" );
        }
    }

    /**
     * If this test fails, you have changed something in TZIDS. This is fine, as long as you only append lines to the end,
     * or add a mapping to a deleted timezone. You are not allowed to change the order of lines or remove a line.
     * p>
     * If your changes were legit, please change the expected byte[] below.
     */
    @Test
    void tzidsOrderMustNotChange() throws URISyntaxException, IOException
    {
        Path path = Paths.get( TimeZones.class.getResource( "/TZIDS" ).toURI() );
        String timeZonesInfo = Files.readString( path ).replace( "\r\n", "\n" );
        byte[] timeZonesHash = DigestUtils.sha256( timeZonesInfo );
        assertThat( timeZonesHash ).isEqualTo(
                new byte[]{-9, 121, 49, -61, 86, 11, 77, -117, 77, 105, -15, -16, 4, 109, 62, 107, -118, 99, 9, -121, -58, 76, -41, 29, 43, 86, -68, 118,
                        -86, 34, 99, 63} );
    }
}
