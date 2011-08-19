/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Test;

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
    public void canEncodeEmailAndUri() throws Exception
    {
        assertCanEncodeAndDecodeToSame( "mattias@neotechnology.com" );
        assertCanEncodeAndDecodeToSame( "http://domain:7474/" );
        // With payload size 32 we can fit 32*8/6=42 characters w/ URI encoding
        assertCanEncodeAndDecodeToSame( "http://neo4j.org/download?os=linux&v=1.4.1", 32 );
    }
    
    private void assertCanEncodeAndDecodeToSame( String string )
    {
        assertCanEncodeAndDecodeToSame( string, PropertyStore.DEFAULT_PAYLOAD_SIZE );
    }
    
    private void assertCanEncodeAndDecodeToSame( String string, int payloadSize )
    {
        PropertyRecord target = new PropertyRecord( 0 );
        assertTrue( LongerShortString.encode( 0, string, target, payloadSize ) );
        assertEquals( string, LongerShortString.decode( target ) );
    }
    
    private void assertCannotEncode( String string )
    {
        assertCannotEncode( string, PropertyStore.DEFAULT_PAYLOAD_SIZE );
    }
    
    private void assertCannotEncode( String string, int payloadSize )
    {
        assertFalse( LongerShortString.encode( 0, string, new PropertyRecord( 0 ), payloadSize ) );
    }
}
