/*
 * Copyright (c) "Neo4j"
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

    @Test
    void weSupportDeletedZoneIdUSPacificNew()
    {
        try
        {
            short pacificNew = TimeZones.map( "US/Pacific-New" );
            assertThat( TimeZones.map( pacificNew ) ).as(
                    "Our time zone table does not remap US/Pacific-New to US/Pacific" ).isEqualTo( "US/Pacific" );
        }
        catch ( IllegalArgumentException e )
        {
            fail( "Our time zone table does not support US/Pacific-New" );
        }
    }

    @Test
    void weSupportDeletedZoneIdUSPacificNewForDeserialization()
    {
        try
        {
            short pacificNew = 58; // Old timezone id for US/Pacific-New
            assertThat( TimeZones.map( pacificNew ) ).as(
                    "Our time zone table does not remap US/Pacific-New to US/Pacific" ).isEqualTo( "US/Pacific" );
        }
        catch ( IllegalArgumentException e )
        {
            fail( "Our time zone table does not support US/Pacific-New" );
        }
    }

    /**
     * If this test fails, you have changed something in TZIDS. This is fine, as long as you only append lines to the end, or add a mapping to a deleted
     * timezone. You are not allowed to change the order of lines or remove a line. p> If your changes were legit, please change the expected byte[] below.
     */
    @Test
    void tzidsOrderMustNotChange() throws URISyntaxException, IOException
    {
        Path path = Path.of( TimeZones.class.getResource( "/TZIDS" ).toURI() );
        String timeZonesInfo = Files.readString( path ).replace( "\r\n", "\n" );
        byte[] timeZonesHash = DigestUtils.sha256( timeZonesInfo );
        assertThat( timeZonesHash ).isEqualTo(
                new byte[]{-31, 62, 67, 16, 63, 24, 6, 6, -3, -29, 114, -49, -97, 107, 54, -96, 75, 69, -29, -66, -9, 18, -49, 89, 118, -28, -119, 115, -107,
                           -64, -37, 30} );
    }
}
