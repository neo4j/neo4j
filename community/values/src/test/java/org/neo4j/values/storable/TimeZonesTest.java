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
package org.neo4j.values.storable;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TimeZonesTest
{
    @Test
    public void weSupportAllJavaZoneIds()
    {
        ZoneId.getAvailableZoneIds().forEach( s ->
        {
            short num = TimeZones.map( s );
            assertThat( "Our time zone table does not have a mapping for " + s, num, greaterThanOrEqualTo( (short) 0 ) );

            String nameFromTable = TimeZones.map( num );
            if ( !s.equals( nameFromTable ) )
            {
                // The test is running on an older Java version and `s` has been removed since, thus it points to a different zone now.
                // That zone should point to itself, however.
                assertThat( "Our time zone table has inconsistent mapping for " + nameFromTable,
                        TimeZones.map( TimeZones.map( nameFromTable ) ), equalTo( nameFromTable ) );
            }
        } );
    }

    @Test
    public void weSupportDeletedZoneIdEastSaskatchewan()
    {
        try
        {
            short eastSaskatchewan = TimeZones.map( "Canada/East-Saskatchewan" );
            assertThat( "Our time zone table does not remap Canada/East-Saskatchewan to Canada/Saskatchewan",
                    TimeZones.map( eastSaskatchewan ), equalTo( "Canada/Saskatchewan" ) );
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
    public void tzidsOrderMustNotChange() throws URISyntaxException, IOException
    {
        Path path = Paths.get( TimeZones.class.getResource( "/TZIDS" ).toURI() );
        byte[] timeZonesInfo = Files.readAllBytes( path );
        byte[] timeZonesHash = DigestUtils.sha256( timeZonesInfo );
        assertThat( timeZonesHash, equalTo(
                new byte[]{27, -102, 116, 117, -108, -114, 65, 81, 88, 52, 25, 112, -67, 3, -99, 69, -26, 100, -38, -2, 29, -41, 60, -85, -58, 102, -101, -122,
                        -40, -66, 49, -65} ) );
    }
}
