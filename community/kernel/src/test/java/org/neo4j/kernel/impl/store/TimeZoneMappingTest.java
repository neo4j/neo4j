/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TimeZoneMappingTest
{
    @Test
    public void weSupportAllJavaZoneIds()
    {
        ZoneId.getAvailableZoneIds().forEach( s ->
        {
            short num = TimeZoneMapping.map( s );
            assertThat( "Our time zone table does not have a mapping for " + s, num, greaterThanOrEqualTo( (short) 0 ) );

            String nameFromTable = TimeZoneMapping.map( num );
            if ( !s.equals( nameFromTable ) )
            {
                // The test is running on an older Java version and `s` has been removed since, thus it points to a different zone now.
                // That zone should point to itself, however.
                assertThat( "Our time zone table has inconsistent mapping for " + nameFromTable, TimeZoneMapping.map( TimeZoneMapping.map( nameFromTable ) ),
                        equalTo( nameFromTable ) );
            }
        } );
    }

    @Test
    public void weSupportDeletedZoneIdEastSaskatchewan()
    {
        try
        {
            short eastSaskatchewan = TimeZoneMapping.map( "Canada/East-Saskatchewan" );
            assertThat( "Our time zone table does not remap Canada/East-Saskatchewan to Canada/Saskatchewan", TimeZoneMapping.map( eastSaskatchewan ),
                    equalTo( "Canada/Saskatchewan" ) );
        }
        catch ( IllegalArgumentException e )
        {
            fail( "Our time zone table does not support Canada/East-Saskatchewan" );
        }
    }

    @Test
    public void updateTimeZoneMappings() throws IOException
    {
        String foo = "https://www.iana.org/time-zones/repository/tzdata-latest.tar.gz";
        List<String> versionsToUpgrade = new ArrayList<>();

        try ( TarArchiveInputStream tar = new TarArchiveInputStream( new GZIPInputStream( new URL( foo ).openStream() ) ) )
        {
            for ( ArchiveEntry entry; (entry = tar.getNextEntry()) != null; )
            {
                if ( "NEWS".equalsIgnoreCase( entry.getName() ) )
                {
                    try ( BufferedReader reader = new BufferedReader( new InputStreamReader( tar ) ) )
                    {
                        for ( String line; (line = reader.readLine()) != null; )
                        {
                            if ( line.startsWith( "Release " ) )
                            {
                                String substring = line.substring( line.indexOf( ' ' ) + 1 );
                                String release = substring.substring( 0, substring.indexOf( ' ' ) );
                                if ( !TimeZoneMapping.LATEST_SUPPORTED_IANA_VERSION.equals( release ) )
                                {
                                    versionsToUpgrade.add( release );
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        HashSet<String> upgradedAlready = new HashSet<>();
        StringBuilder builder = new StringBuilder();
        for ( int i = versionsToUpgrade.size(); i-- > 0; )
        {
            upgradeIANATo( versionsToUpgrade.get( i ), upgradedAlready, builder );
        }
        String s = builder.toString();
        assertThat( "Please append the following to the end of `TZIDS`: \n" + s, s, isEmptyString() );
    }

    private void upgradeIANATo( String release, Set<String> upgradedAlready, StringBuilder builder ) throws IOException
    {
        String foo = "https://data.iana.org/time-zones/releases/tzdata" + release + ".tar.gz";
        Set<String> ianaSupportedTzs = new HashSet<>();

        try ( TarArchiveInputStream tar = new TarArchiveInputStream( new GZIPInputStream( new URL( foo ).openStream() ) ) )
        {
            for ( ArchiveEntry entry; (entry = tar.getNextEntry()) != null; )
            {
                if ( "zone1970.tab".equalsIgnoreCase( entry.getName() ) )
                {
                    try ( BufferedReader reader = new BufferedReader( new InputStreamReader( tar ) ) )
                    {
                        for ( String line; (line = reader.readLine()) != null; )
                        {
                            if ( line.startsWith( "#" ) )
                            {
                                continue;
                            }
                            String tz = line.split( "\\t" )[2];
                            ianaSupportedTzs.add( tz );
                        }
                    }
                    break;
                }
            }
        }

        // TODO assertion for removals
        Collection<String> neo4jSupportedTzs = TimeZoneMapping.supportedTimeZones();
        Set<String> removedTzs = new HashSet<>( neo4jSupportedTzs );
        removedTzs.removeAll( ianaSupportedTzs );
        //assertThat( "There were removals from the IANA database. Please upgrade manually.", removedTzs, empty() );

        Set<String> addedTzs = new HashSet<>( ianaSupportedTzs );
        addedTzs.removeAll( neo4jSupportedTzs );
        addedTzs.removeAll( upgradedAlready );
        if ( !addedTzs.isEmpty() )
        {
            String[] added = addedTzs.toArray( new String[0] );
            Arrays.sort( added );
            builder.append( "# tzdata" );
            builder.append( release );
            builder.append( System.lineSeparator() );
            for ( String unsupported : added )
            {
                builder.append( unsupported ).append( System.lineSeparator() );
            }
            // Update for the next round
            Collections.addAll( upgradedAlready, added );
        }
    }
}
