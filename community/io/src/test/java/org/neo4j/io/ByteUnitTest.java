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
package org.neo4j.io;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.Byte;
import static org.neo4j.io.ByteUnit.ExbiByte;
import static org.neo4j.io.ByteUnit.GibiByte;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.io.ByteUnit.MebiByte;
import static org.neo4j.io.ByteUnit.PebiByte;
import static org.neo4j.io.ByteUnit.TebiByte;
import static org.neo4j.io.ByteUnit.bytes;
import static org.neo4j.io.ByteUnit.exbiBytes;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.parse;
import static org.neo4j.io.ByteUnit.pebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;

class ByteUnitTest
{
    @Test
    void convertZero()
    {
        assertThat( Byte.toBytes( 0 ), is( 0L ) );
        assertThat( Byte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( Byte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( Byte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( Byte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( Byte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( Byte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( KibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( KibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( MebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( MebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( GibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( GibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( TebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( TebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( PebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( PebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ExbiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ExbiByte.toExbiBytes( 0 ), is( 0L ) );
    }

    @Test
    void convertOneToEIC()
    {
        assertThat( KibiByte.toBytes( 1 ), is( 1024L ) );
        assertThat( KibiByte.toKibiBytes( 1 ), is( 1L ) );
        assertThat( KibiByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( KibiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( KibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( KibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( KibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( MebiByte.toBytes( 1 ), is( 1048576L ) );
        assertThat( MebiByte.toKibiBytes( 1 ), is( 1024L ) );
        assertThat( MebiByte.toMebiBytes( 1 ), is( 1L ) );
        assertThat( MebiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( MebiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( MebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( MebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( GibiByte.toBytes( 1 ), is( 1073741824L ) );
        assertThat( GibiByte.toKibiBytes( 1 ), is( 1048576L ) );
        assertThat( GibiByte.toMebiBytes( 1 ), is( 1024L ) );
        assertThat( GibiByte.toGibiBytes( 1 ), is( 1L ) );
        assertThat( GibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( GibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( GibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( TebiByte.toBytes( 1 ), is( 1099511627776L ) );
        assertThat( TebiByte.toKibiBytes( 1 ), is( 1073741824L ) );
        assertThat( TebiByte.toMebiBytes( 1 ), is( 1048576L ) );
        assertThat( TebiByte.toGibiBytes( 1 ), is( 1024L ) );
        assertThat( TebiByte.toTebiBytes( 1 ), is( 1L ) );
        assertThat( TebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( TebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( PebiByte.toBytes( 1 ), is( 1125899906842624L ) );
        assertThat( PebiByte.toKibiBytes( 1 ), is( 1099511627776L ) );
        assertThat( PebiByte.toMebiBytes( 1 ), is( 1073741824L ) );
        assertThat( PebiByte.toGibiBytes( 1 ), is( 1048576L ) );
        assertThat( PebiByte.toTebiBytes( 1 ), is( 1024L ) );
        assertThat( PebiByte.toPebiBytes( 1 ), is( 1L ) );
        assertThat( PebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ExbiByte.toBytes( 1 ), is( 1152921504606846976L ) );
        assertThat( ExbiByte.toKibiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ExbiByte.toMebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ExbiByte.toGibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ExbiByte.toTebiBytes( 1 ), is( 1048576L ) );
        assertThat( ExbiByte.toPebiBytes( 1 ), is( 1024L ) );
        assertThat( ExbiByte.toExbiBytes( 1 ), is( 1L ) );
    }

    @Test
    void unitsAsBytes()
    {
        assertThat( bytes( 1 ), is( 1L ) );
        assertThat( kibiBytes( 1 ), is( 1024L ) );
        assertThat( mebiBytes( 1 ), is( 1048576L ) );
        assertThat( gibiBytes( 1 ), is( 1073741824L ) );
        assertThat( tebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( pebiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( exbiBytes( 1 ), is( 1152921504606846976L ) );

        assertThat( parse( "1" ), is( 1L ) );
        assertThat( parse( "1 KiB" ), is( 1024L ) );
        assertThat( parse( "1KiB" ), is( 1024L ) );
        assertThat( parse( " 1    Ki B" ), is( 1024L ) );
        assertThat( parse( "1 KB" ), is( 1024L ) );
        assertThat( parse( "1KB" ), is( 1024L ) );
        assertThat( parse( " 1    KB " ), is( 1024L ) );
        assertThat( parse( "1 kB" ), is( 1024L ) );
        assertThat( parse( "1kB" ), is( 1024L ) );
        assertThat( parse( " 1    kB " ), is( 1024L ) );
        assertThat( parse( "1 kb" ), is( 1024L ) );
        assertThat( parse( "1kb" ), is( 1024L ) );
        assertThat( parse( " 1    kb " ), is( 1024L ) );
        assertThat( parse( "1 k" ), is( 1024L ) );
        assertThat( parse( "1k" ), is( 1024L ) );
        assertThat( parse( " 1    k" ), is( 1024L ) );
        assertThat( parse( "1 MiB" ), is( 1048576L ) );
        assertThat( parse( "1MiB" ), is( 1048576L ) );
        assertThat( parse( " 1    Mi B" ), is( 1048576L ) );
        assertThat( parse( "1 MB" ), is( 1048576L ) );
        assertThat( parse( "1MB" ), is( 1048576L ) );
        assertThat( parse( " 1    MB " ), is( 1048576L ) );
        assertThat( parse( "1 mB" ), is( 1048576L ) );
        assertThat( parse( "1mB" ), is( 1048576L ) );
        assertThat( parse( " 1    mB " ), is( 1048576L ) );
        assertThat( parse( "1 mb" ), is( 1048576L ) );
        assertThat( parse( "1mb" ), is( 1048576L ) );
        assertThat( parse( " 1    mb " ), is( 1048576L ) );
        assertThat( parse( "1 m" ), is( 1048576L ) );
        assertThat( parse( "1m" ), is( 1048576L ) );
        assertThat( parse( " 1    m" ), is( 1048576L ) );
        assertThat( parse( "1 GiB" ), is( 1073741824L ) );
        assertThat( parse( "1GiB" ), is( 1073741824L ) );
        assertThat( parse( " 1    Gi B" ), is( 1073741824L ) );
        assertThat( parse( "1 GB" ), is( 1073741824L ) );
        assertThat( parse( "1GB" ), is( 1073741824L ) );
        assertThat( parse( " 1    GB " ), is( 1073741824L ) );
        assertThat( parse( "1 gB" ), is( 1073741824L ) );
        assertThat( parse( "1gB" ), is( 1073741824L ) );
        assertThat( parse( " 1    gB " ), is( 1073741824L ) );
        assertThat( parse( "1 gb" ), is( 1073741824L ) );
        assertThat( parse( "1gb" ), is( 1073741824L ) );
        assertThat( parse( " 1    gb " ), is( 1073741824L ) );
        assertThat( parse( "1 g" ), is( 1073741824L ) );
        assertThat( parse( "1g" ), is( 1073741824L ) );
        assertThat( parse( " 1    g" ), is( 1073741824L ) );
        assertThat( parse( "1 TiB" ), is( 1099511627776L ) );
        assertThat( parse( "1TiB" ), is( 1099511627776L ) );
        assertThat( parse( " 1    Ti B" ), is( 1099511627776L ) );
        assertThat( parse( "1 TB" ), is( 1099511627776L ) );
        assertThat( parse( "1TB" ), is( 1099511627776L ) );
        assertThat( parse( " 1    TB " ), is( 1099511627776L ) );
        assertThat( parse( "1 PiB" ), is( 1125899906842624L ) );
        assertThat( parse( "1PiB" ), is( 1125899906842624L ) );
        assertThat( parse( " 1    Pi B" ), is( 1125899906842624L ) );
        assertThat( parse( "1 PB" ), is( 1125899906842624L ) );
        assertThat( parse( "1PB" ), is( 1125899906842624L ) );
        assertThat( parse( " 1    PB " ), is( 1125899906842624L ) );
        assertThat( parse( "1 EiB" ), is( 1152921504606846976L ) );
        assertThat( parse( "1EiB" ), is( 1152921504606846976L ) );
        assertThat( parse( " 1    Ei B" ), is( 1152921504606846976L ) );
        assertThat( parse( "1 EB" ), is( 1152921504606846976L ) );
        assertThat( parse( "1EB" ), is( 1152921504606846976L ) );
        assertThat( parse( " 1    EB " ), is( 1152921504606846976L ) );
    }

    @Test
    void bytesToString()
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
        assertEquals( "9.546GiB", ByteUnit.bytesToString( 10250000000L ) );
    }

    @Test
    void mustThrowWhenParsingInvalidUnit()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "1 XB" ) );
    }

    @Test
    void mustThrowWhenParsingUnitInterjectedWithNumber()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "1K2i3B" ) );
    }

    @Test
    void mustThrowWhenParsingNonNumbericTest()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "abc" ) );
    }

    @Test
    void mustThrowWhenParsingOnlyUnit()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "MiB" ) );
    }

    @Test
    void mustThrowWhenParsingUnitBeforeValue()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "MiB 1" ) );
    }
}
