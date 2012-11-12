/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.TestShortString.Charset;

public class TestLongerShortString
{
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
    public void checkMarginalFit() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "^aaaaaaaaaaaaaaaaaaaaaaaaaa" );
        assertCannotEncode( "^aaaaaaaaaaaaaaaaaaaaaaaaaaa" );
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
