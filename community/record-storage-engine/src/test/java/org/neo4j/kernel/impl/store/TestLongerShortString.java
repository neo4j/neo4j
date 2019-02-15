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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat.DEFAULT_PAYLOAD_SIZE;

@ExtendWith( RandomExtension.class )
class TestLongerShortString
{
    @Inject
    protected RandomRule random;

    @Test
    void testMasks()
    {
        assertEquals( 0, 1 & LongerShortString.invertedBitMask( LongerShortString.NUMERICAL ) );
        assertEquals( 0, 2 & LongerShortString.invertedBitMask( LongerShortString.DATE ) );
        assertEquals( LongerShortString.NUMERICAL.bitMask(),
                3 & LongerShortString.invertedBitMask( LongerShortString.DATE ) );
        assertEquals( 0, LongerShortString.NUMERICAL.bitMask() &
                         LongerShortString.invertedBitMask( LongerShortString.NUMERICAL, LongerShortString.DATE ) );
    }

    @Test
    void canEncodeEmptyString()
    {
        assertCanEncodeAndDecodeToSame( "" );
    }

    @Test
    void canEncodeNumerical()
    {
        assertCanEncodeAndDecodeToSame( "12345678901234567890" );
        assertCanEncodeAndDecodeToSame( "12345678901234567890 +-.,' 321,3" );
    }

    @Test
    void canEncodeDate()
    {
        assertCanEncodeAndDecodeToSame( "2011-10-10 12:45:22+0200" );
        assertCanEncodeAndDecodeToSame( "2011/10/10 12:45:22+0200" );
    }

    @Test
    void testRandomStrings()
    {
        for ( int i = 0; i < 1000; i++ )
        {
            for ( TestStringCharset charset : TestStringCharset.values() )
            {
                List<String> list = randomStrings( 100, charset, 30 );
                for ( String string : list )
                {
                    PropertyBlock record = new PropertyBlock();
                    if ( LongerShortString.encode( 10, string, record, DEFAULT_PAYLOAD_SIZE ) )
                    {
                        assertEquals( Values.stringValue( string ), LongerShortString.decode( record ) );
                    }
                }
            }
        }
    }

    @Test
    void canEncodeEmailAndUri()
    {
        assertCanEncodeAndDecodeToSame( "mattias@neotechnology.com" );
        assertCanEncodeAndDecodeToSame( "http://domain:7474/" );
    }

    @Test
    void canEncodeLower()
    {
        assertCanEncodeAndDecodeToSame( "folder/generators/templates/controller.ext" );
        assertCanEncodeAndDecodeToSame( "folder/generators/templates/controller.extr" );
        assertCannotEncode( "folder/generators/templates/controller.extra" );
    }

    @Test
    void canEncodeLowerHex()
    {
        assertCanEncodeAndDecodeToSame( "da39a3ee5e6b4b0d3255bfef95601890afd80709" ); // sha1hex('') len=40
        assertCanEncodeAndDecodeToSame(
                "0123456789" + "abcdefabcd" + "0a0b0c0d0e" + "1a1b1c1d1e" + "f9e8d7c6b5" + "a4f3" ); // len=54
        assertCannotEncode( "da39a3ee5e6b4b0d3255bfef95601890afd80709" + "0123456789" + "abcde" ); // len=55
        // test not failing on long illegal hex
        assertCannotEncode( "aaaaaaaaaa" + "bbbbbbbbbb" + "cccccccccc" + "dddddddddd" + "eeeeeeeeee" + "x" );
    }

    @Test
    void canEncodeUpperHex()
    {
        assertCanEncodeAndDecodeToSame( "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709" ); // sha1HEX('') len=40
        assertCanEncodeAndDecodeToSame(
                "0123456789" + "ABCDEFABCD" + "0A0B0C0D0E" + "1A1B1C1D1E" + "F9E8D7C6B5" + "A4F3" ); // len=54
        assertCannotEncode( "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709" + "0123456789" + "ABCDE" ); // len=55
        // test not failing on long illegal HEX
        assertCannotEncode( "AAAAAAAAAA" + "BBBBBBBBBB" + "CCCCCCCCCC" + "DDDDDDDDDD" + "EEEEEEEEEE" + "X" );
    }

    @Test
    void checkMarginalFit()
    {
        assertCanEncodeAndDecodeToSame( "^aaaaaaaaaaaaaaaaaaaaaaaaaa" );
        assertCannotEncode( "^aaaaaaaaaaaaaaaaaaaaaaaaaaa" );
    }

    @Test
    void canEncodeUUIDString()
    {
        assertCanEncodeAndDecodeToSame( "81fe144f-484b-4a34-8e36-17a021540318" );
    }

    private List<String> randomStrings( int count, TestStringCharset charset, int maxLen )
    {
        List<String> result = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            result.add( charset.randomString( maxLen, random.random() ) );
        }
        return result;
    }

    private void assertCanEncodeAndDecodeToSame( String string )
    {
        assertCanEncodeAndDecodeToSame( string, DEFAULT_PAYLOAD_SIZE );
    }

    private void assertCanEncodeAndDecodeToSame( String string, int payloadSize )
    {
        PropertyBlock target = new PropertyBlock();
        assertTrue( LongerShortString.encode( 0, string, target, payloadSize ) );
        assertEquals( Values.stringValue( string ), LongerShortString.decode( target ) );
    }

    private void assertCannotEncode( String string )
    {
        assertCannotEncode( string, DEFAULT_PAYLOAD_SIZE );
    }

    private void assertCannotEncode( String string, int payloadSize )
    {
        assertFalse( LongerShortString.encode( 0, string, new PropertyBlock(), payloadSize ) );
    }
}
