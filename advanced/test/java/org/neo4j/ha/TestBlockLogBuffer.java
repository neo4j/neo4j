/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;
import org.neo4j.kernel.ha.BlockLogBuffer;

public class TestBlockLogBuffer
{
    @Test
    public void onlyOneNonFullBlock() throws IOException
    {
        byte[] bytes = new byte[255];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer );
        byte byteValue = 5;
        int intValue = 1234;
        long longValue = 574853;
        float floatValue = 304985.5f;
        double doubleValue = 48493.22d;
        final byte[] bytesValue = new byte[] { 1,5,2,6,3 };
        final char[] charsValue = "This is chars".toCharArray();
        buffer.put( byteValue );
        buffer.putInt( intValue );
        buffer.putLong( longValue );
        buffer.putFloat( floatValue );
        buffer.putDouble( doubleValue );
        buffer.put( bytesValue );
        buffer.put( charsValue );
        buffer.done();
        
        ByteBuffer verificationBuffer = ByteBuffer.wrap( bytes );
        assertEquals( 56, verificationBuffer.get() );
        assertEquals( byteValue, verificationBuffer.get() );
        assertEquals( intValue, verificationBuffer.getInt() );
        assertEquals( longValue, verificationBuffer.getLong() );
        assertEquals( floatValue, verificationBuffer.getFloat(), 0.0 );
        assertEquals( doubleValue, verificationBuffer.getDouble(), 0.0 );
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat(actualBytes, new ArrayMatches<byte[]>( bytesValue ));
        char[] actualChars = new char[charsValue.length];
        verificationBuffer.asCharBuffer().get( actualChars );
        assertThat(actualChars, new ArrayMatches<char[]>( charsValue ));
    }
    
    private class ArrayMatches<T> extends BaseMatcher<T> {
        private final T expected;
        private Object actual;

        public ArrayMatches(T expected)
        {
            this.expected = expected;
        }
        @Override
        public boolean matches( Object actual )
        {
            this.actual = actual;
            if ( expected instanceof byte[] && actual instanceof byte[] )
            {
                return Arrays.equals( (byte[]) actual, (byte[]) expected );
            }
            else if ( expected instanceof char[] && actual instanceof char[] )
            {
                return Arrays.equals( (char[]) actual, (char[]) expected );
            }
            return false;
        }

        @Override
        public void describeTo( Description descr )
        {
            descr.appendText( String.format( "expected %s, got %s",
                    toString( expected ), toString( actual ) ) );
        }
        private String toString( Object value )
        {
            if ( value instanceof byte[] )
            {
                return Arrays.toString( (byte[]) value);
            }
            if ( value instanceof char[] )
            {
                return Arrays.toString( (char[]) value);
            }
            return "" + value;
        }
    }
}
