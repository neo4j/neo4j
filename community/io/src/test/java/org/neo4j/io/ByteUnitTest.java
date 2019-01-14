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
package org.neo4j.io;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ByteUnitTest
{
    @Test
    public void convertZero()
    {
        assertThat( ByteUnit.Byte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.KibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 0 ), is( 0L ) );
    }

    @Test
    public void convertOneToEIC()
    {
        assertThat( ByteUnit.KibiByte.toBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 1 ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 1 ), is( 1L ) );
    }

    @Test
    public void unitsAsBytes()
    {
        assertThat( ByteUnit.bytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.kibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.mebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.gibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.tebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.pebiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.exbiBytes( 1 ), is( 1152921504606846976L ) );

        assertThat( ByteUnit.parse( "1" ), is( 1L ) );
        assertThat( ByteUnit.parse( "1 KiB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1KiB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( " 1    Ki B" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1 KB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1KB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( " 1    KB " ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1 kB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1kB" ), is( 1024L ) );
        assertThat( ByteUnit.parse( " 1    kB " ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1 kb" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1kb" ), is( 1024L ) );
        assertThat( ByteUnit.parse( " 1    kb " ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1 k" ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1k" ), is( 1024L ) );
        assertThat( ByteUnit.parse( " 1    k"  ), is( 1024L ) );
        assertThat( ByteUnit.parse( "1 MiB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1MiB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( " 1    Mi B" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1 MB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1MB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( " 1    MB " ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1 mB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1mB" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( " 1    mB " ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1 mb" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1mb" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( " 1    mb " ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1 m" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1m" ), is( 1048576L ) );
        assertThat( ByteUnit.parse( " 1    m"  ), is( 1048576L ) );
        assertThat( ByteUnit.parse( "1 GiB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1GiB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( " 1    Gi B" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1 GB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1GB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( " 1    GB " ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1 gB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1gB" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( " 1    gB " ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1 gb" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1gb" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( " 1    gb " ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1 g" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1g" ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( " 1    g"  ), is( 1073741824L ) );
        assertThat( ByteUnit.parse( "1 TiB" ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( "1TiB" ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( " 1    Ti B" ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( "1 TB" ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( "1TB" ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( " 1    TB " ), is( 1099511627776L ) );
        assertThat( ByteUnit.parse( "1 PiB" ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( "1PiB" ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( " 1    Pi B" ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( "1 PB" ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( "1PB" ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( " 1    PB " ), is( 1125899906842624L ) );
        assertThat( ByteUnit.parse( "1 EiB" ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.parse( "1EiB" ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.parse( " 1    Ei B" ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.parse( "1 EB" ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.parse( "1EB" ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.parse( " 1    EB " ), is( 1152921504606846976L ) );
    }

    @Test
    public void bytesToString()
    {
        assertEquals( "1B", ByteUnit.bytesToString( 1 ) );
        assertEquals( "10B", ByteUnit.bytesToString( 10 ) );
        assertEquals( "1000B", ByteUnit.bytesToString( 1000 ) );
        assertEquals( "1.001KiB", ByteUnit.bytesToString( 1025 ) );
        assertEquals( "10.01KiB", ByteUnit.bytesToString( 10250 ) );
        assertEquals( "100.1KiB", ByteUnit.bytesToString( 102500 ) );
        assertEquals( "1001KiB", ByteUnit.bytesToString( 1025000 ) );
        assertEquals( "9.775MiB", ByteUnit.bytesToString( 10250000 ) );
        assertEquals( "97.75MiB", ByteUnit.bytesToString( 102500000 ) );
        assertEquals( "977.5MiB", ByteUnit.bytesToString( 1025000000 ) );
        assertEquals( "9.546GiB", ByteUnit.bytesToString( 10250000000L) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowWhenParsingInvalidUnit()
    {
        ByteUnit.parse( "1 XB" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowWhenParsingUnitInterjectedWithNumber()
    {
        ByteUnit.parse( "1K2i3B" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowWhenParsingNonNumbericTest()
    {
        ByteUnit.parse( "abc" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowWhenParsingOnlyUnit()
    {
        ByteUnit.parse( "MiB" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowWhenParsingUnitBeforeValue()
    {
        ByteUnit.parse( "MiB 1" );
    }
}
