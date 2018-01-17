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
package org.neo4j.commandline.dbms;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.StringReader;
import java.util.Map;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.bytesToString;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendHeapMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendOsMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendPageCacheMemory;
import static org.neo4j.configuration.ExternalSettings.initialHeapSize;
import static org.neo4j.configuration.ExternalSettings.maxHeapSize;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.ByteUnit.exbiBytes;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.buildSetting;

public class MemoryRecommendationsCommandTest
{
    @Test
    public void mustRecommendOSMemory()
    {
        assertThat( recommendOsMemory( mebiBytes( 100 ) ), between( mebiBytes( 45 ), mebiBytes( 55 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 1 ) ), between( mebiBytes( 450 ), mebiBytes( 550 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 3 ) ), between( mebiBytes( 1256 ), mebiBytes( 1356 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 192 ) ), between( gibiBytes( 17 ), gibiBytes( 19 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 1920 ) ), greaterThan( gibiBytes( 29 ) ) );
    }

    @Test
    public void mustRecommendHeapMemory()
    {
        assertThat( recommendHeapMemory( mebiBytes( 100 ) ), between( mebiBytes( 45 ), mebiBytes( 55 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 1 ) ), between( mebiBytes( 450 ), mebiBytes( 550 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 3 ) ), between( mebiBytes( 1256 ), mebiBytes( 1356 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 6 ) ), between( mebiBytes( 3000 ), mebiBytes( 3200 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 192 ) ), between( gibiBytes( 30 ), gibiBytes( 32 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 1920 ) ), between( gibiBytes( 30 ), gibiBytes( 32 ) ) );
    }

    @Test
    public void mustRecommendPageCacheMemory()
    {
        assertThat( recommendPageCacheMemory( mebiBytes( 100 ) ), between( mebiBytes( 95 ), mebiBytes( 105 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1 ) ), between( mebiBytes( 95 ), mebiBytes( 105 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 3 ) ), between( mebiBytes( 470 ), mebiBytes( 530 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 6 ) ), between( mebiBytes( 980 ), mebiBytes( 1048 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 192 ) ), between( gibiBytes( 140 ), gibiBytes( 150 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1920 ) ), between( gibiBytes( 1850 ), gibiBytes( 1900 ) ) );

        // Also never recommend more than 16 TiB of page cache memory, regardless of how much is available.
        assertThat( recommendPageCacheMemory( exbiBytes( 1 ) ), lessThan( tebiBytes( 17 ) ) );
    }

    @Test
    public void bytesToStringMustBeParseableBySettings()
    {
        Setting<Long> setting = buildSetting( "arg", BYTES ).build();
        for ( int i = 1; i < 10_000; i++ )
        {
            int mebibytes = 75 * i;
            long expectedBytes = mebiBytes( mebibytes );
            String bytesToString = bytesToString( expectedBytes );
            long actualBytes = setting.apply( s -> bytesToString );
            long tenPercent = (long) (expectedBytes * 0.1);
            assertThat( mebibytes + "m",
                    actualBytes,
                    between( expectedBytes - tenPercent, expectedBytes + tenPercent ) );
        }
    }

    private Matcher<Long> between( long lowerBound, long upperBound )
    {
        return both( greaterThanOrEqualTo( lowerBound ) ).and( lessThanOrEqualTo( upperBound ) );
    }

    @Test
    public void mustPrintRecommendationsAsConfigReadableOutput() throws Exception
    {
        StringBuilder output = new StringBuilder();
        OutsideWorld outsideWorld = new RealOutsideWorld()
        {
            @Override
            public void stdOutLine( String text )
            {
                output.append( text ).append( System.lineSeparator() );
            }
        };
        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand( outsideWorld );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ) ) );

        command.execute( new String[]{"--memory=8g"} );

        Map<String,String> stringMap = MapUtil.load( new StringReader( output.toString() ) );
        assertThat( stringMap.get( initialHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( maxHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( pagecache_memory.name() ), is( pagecache ) );
    }
}
