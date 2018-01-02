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

import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.TestShortString.Charset;
import org.neo4j.kernel.impl.store.record.PropertyBlock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLongerShortString
{

    @Test
    public void testMasks() throws Exception {
        assertEquals(0,1 & LongerShortString.invertedBitMask(LongerShortString.NUMERICAL));
        assertEquals(0,2 & LongerShortString.invertedBitMask(LongerShortString.DATE));
        assertEquals(LongerShortString.NUMERICAL.bitMask(),3 & LongerShortString.invertedBitMask(LongerShortString.DATE));
        assertEquals(0, (LongerShortString.NUMERICAL.bitMask()|LongerShortString.NUMERICAL.bitMask()) & LongerShortString.invertedBitMask(LongerShortString.NUMERICAL, LongerShortString.DATE));
    }

    @Test
    public void canEncodeEmptyString()
    {
        assertCanEncodeAndDecodeToSame( "" );
    }

    @Test
    public void canEncodeNumerical()
    {
        assertCanEncodeAndDecodeToSame( "12345678901234567890" );
        assertCanEncodeAndDecodeToSame( "12345678901234567890 +-.,' 321,3" );
    }

    @Test
    public void canEncodeDate() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "2011-10-10 12:45:22+0200" );
        assertCanEncodeAndDecodeToSame( "2011/10/10 12:45:22+0200" );
    }

    @Test
    public void testRandomStrings() throws Exception
    {
        for ( int i = 0; i < 1000; i++ )
        {
            for ( Charset charset : Charset.values() )
            {
                List<String> list = TestShortString.randomStrings( 100, charset, 30 );
                for ( String string : list )
                {
                    PropertyBlock record = new PropertyBlock();
                    if ( LongerShortString.encode( 10, string, record, PropertyStore.DEFAULT_PAYLOAD_SIZE ) )
                    {
                        assertEquals( string, LongerShortString.decode( record ) );
                    }
                }
            }
        }
    }

    @Test
    public void canEncodeEmailAndUri() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "mattias@neotechnology.com" );
        assertCanEncodeAndDecodeToSame( "http://domain:7474/" );
    }

    @Test
    public void canEncodeLower() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "folder/generators/templates/controller.ext" );
        assertCanEncodeAndDecodeToSame( "folder/generators/templates/controller.extr" );
        assertCannotEncode( "folder/generators/templates/controller.extra" );
    }

    @Test
    public void canEncodeLowerHex() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "da39a3ee5e6b4b0d3255bfef95601890afd80709" ); // sha1hex('') len=40
        assertCanEncodeAndDecodeToSame( "0123456789" + "abcdefabcd" + "0a0b0c0d0e" + "1a1b1c1d1e" + "f9e8d7c6b5" + "a4f3" ); // len=54
        assertCannotEncode( "da39a3ee5e6b4b0d3255bfef95601890afd80709" + "0123456789" + "abcde" ); // len=55
        // test not failing on long illegal hex
        assertCannotEncode( "aaaaaaaaaa" + "bbbbbbbbbb" + "cccccccccc" + "dddddddddd" + "eeeeeeeeee" + "x");
    }

    @Test
    public void canEncodeUpperHex() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709" ); // sha1HEX('') len=40
        assertCanEncodeAndDecodeToSame( "0123456789" + "ABCDEFABCD" + "0A0B0C0D0E" + "1A1B1C1D1E" + "F9E8D7C6B5" + "A4F3" ); // len=54
        assertCannotEncode( "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709" + "0123456789" + "ABCDE" ); // len=55
        // test not failing on long illegal HEX
        assertCannotEncode( "AAAAAAAAAA" + "BBBBBBBBBB" + "CCCCCCCCCC" + "DDDDDDDDDD" + "EEEEEEEEEE" + "X");
    }

    @Test
    public void checkMarginalFit() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "^aaaaaaaaaaaaaaaaaaaaaaaaaa" );
        assertCannotEncode( "^aaaaaaaaaaaaaaaaaaaaaaaaaaa" );
    }

    @Test
    public void canEncodeUUIDString() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "81fe144f-484b-4a34-8e36-17a021540318" );
    }

    private void assertCanEncodeAndDecodeToSame( String string )
    {
        assertCanEncodeAndDecodeToSame( string, PropertyStore.DEFAULT_PAYLOAD_SIZE );
    }

    private void assertCanEncodeAndDecodeToSame( String string, int payloadSize )
    {
        PropertyBlock target = new PropertyBlock();
        assertTrue( LongerShortString.encode( 0, string, target, payloadSize ) );
        assertEquals( string, LongerShortString.decode( target ) );
    }

    private void assertCannotEncode( String string )
    {
        assertCannotEncode( string, PropertyStore.DEFAULT_PAYLOAD_SIZE );
    }

    private void assertCannotEncode( String string, int payloadSize )
    {
        assertFalse( LongerShortString.encode( 0, string, new PropertyBlock(),
                payloadSize ) );
    }
}
