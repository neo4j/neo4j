/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ha;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import org.neo4j.com.BlockLogBuffer;
import org.neo4j.com.BlockLogReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestBlockLogBuffer
{
    @Test
    public void onlyOneNonFullBlock()
    {
        byte[] bytes = new byte[255];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte byteValue = 5;
        int intValue = 1234;
        long longValue = 574853;
        float floatValue = 304985.5f;
        double doubleValue = 48493.22d;
        final byte[] bytesValue = new byte[] { 1, 5, 2, 6, 3 };
        buffer.put( byteValue );
        buffer.putInt( intValue );
        buffer.putLong( longValue );
        buffer.putFloat( floatValue );
        buffer.putDouble( doubleValue );
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        ByteBuffer verificationBuffer = ByteBuffer.wrap( bytes );
        assertEquals( 30, verificationBuffer.get() );
        assertEquals( byteValue, verificationBuffer.get() );
        assertEquals( intValue, verificationBuffer.getInt() );
        assertEquals( longValue, verificationBuffer.getLong() );
        assertEquals( floatValue, verificationBuffer.getFloat(), 0.0 );
        assertEquals( doubleValue, verificationBuffer.getDouble(), 0.0 );
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat( actualBytes, new ArrayMatches<>( bytesValue ) );
    }

    @Test
    public void readSmallPortions() throws IOException
    {
        byte[] bytes = new byte[255];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte byteValue = 5;
        int intValue = 1234;
        long longValue = 574853;
        buffer.put( byteValue );
        buffer.putInt( intValue );
        buffer.putLong( longValue );
        buffer.close();

        ReadableByteChannel reader = new BlockLogReader( wrappedBuffer );
        ByteBuffer verificationBuffer = ByteBuffer.wrap( new byte[1] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        assertEquals( byteValue, verificationBuffer.get() );
        verificationBuffer = ByteBuffer.wrap( new byte[4] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        assertEquals( intValue, verificationBuffer.getInt() );
        verificationBuffer = ByteBuffer.wrap( new byte[8] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        assertEquals( longValue, verificationBuffer.getLong() );
    }

    @Test
    public void readOnlyOneNonFullBlock() throws IOException
    {
        byte[] bytes = new byte[255];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte byteValue = 5;
        int intValue = 1234;
        long longValue = 574853;
        float floatValue = 304985.5f;
        double doubleValue = 48493.22d;
        final byte[] bytesValue = new byte[] { 1, 5, 2, 6, 3 };
        buffer.put( byteValue );
        buffer.putInt( intValue );
        buffer.putLong( longValue );
        buffer.putFloat( floatValue );
        buffer.putDouble( doubleValue );
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        ReadableByteChannel reader = new BlockLogReader( wrappedBuffer );
        ByteBuffer verificationBuffer = ByteBuffer.wrap( new byte[1000] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        assertEquals( byteValue, verificationBuffer.get() );
        assertEquals( intValue, verificationBuffer.getInt() );
        assertEquals( longValue, verificationBuffer.getLong() );
        assertEquals( floatValue, verificationBuffer.getFloat(), 0.0 );
        assertEquals( doubleValue, verificationBuffer.getDouble(), 0.0 );
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat( actualBytes, new ArrayMatches<>( bytesValue ) );
    }

    @Test
    public void onlyOneFullBlock()
    {
        byte[] bytes = new byte[256];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte[] bytesValue = new byte[255];
        bytesValue[0] = 1;
        bytesValue[254] = -1;
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        ByteBuffer verificationBuffer = ByteBuffer.wrap( bytes );
        assertEquals( (byte) 255, verificationBuffer.get() );
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat( actualBytes, new ArrayMatches<>( bytesValue ) );
    }

    @Test
    public void readOnlyOneFullBlock() throws Exception
    {
        byte[] bytes = new byte[256];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte[] bytesValue = new byte[255];
        bytesValue[0] = 1;
        bytesValue[254] = -1;
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        ReadableByteChannel reader = new BlockLogReader( wrappedBuffer );
        ByteBuffer verificationBuffer = ByteBuffer.wrap( new byte[1000] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat( actualBytes, new ArrayMatches<>( bytesValue ) );
    }

    @Test
    public void canWriteLargestAtomAfterFillingBuffer()
    {
        byte[] bytes = new byte[300];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte[] bytesValue = new byte[255];
        bytesValue[0] = 1;
        bytesValue[254] = -1;
        long longValue = 123456;
        buffer.put( bytesValue, bytesValue.length );
        buffer.putLong( longValue );
        buffer.close();

        ByteBuffer verificationBuffer = ByteBuffer.wrap( bytes );
        assertEquals( (byte) 0, verificationBuffer.get() );
        byte[] actualBytes = new byte[bytesValue.length];
        verificationBuffer.get( actualBytes );
        assertThat( actualBytes, new ArrayMatches<>( bytesValue ) );
        assertEquals( (byte) 8, verificationBuffer.get() );
        assertEquals( longValue, verificationBuffer.getLong() );
    }

    @Test
    public void canWriteReallyLargeByteArray()
    {
        byte[] bytes = new byte[650];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte[] bytesValue = new byte[600];
        bytesValue[1] = 1;
        bytesValue[99] = 2;
        bytesValue[199] = 3;
        bytesValue[299] = 4;
        bytesValue[399] = 5;
        bytesValue[499] = 6;
        bytesValue[599] = 7;
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        byte[] actual;
        ByteBuffer verificationBuffer = ByteBuffer.wrap( bytes );
        assertEquals( (byte) 0, verificationBuffer.get() );
        actual = new byte[255];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 0, 255 ) ) );
        assertEquals( (byte) 0, verificationBuffer.get() );
        actual = new byte[255];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 255, 510 ) ) );
        assertEquals( (byte) 90, verificationBuffer.get() );
        actual = new byte[90];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 510, 600 ) ) );
    }

    @Test
    public void canReaderReallyLargeByteArray()
    {
        byte[] bytes = new byte[650];
        ChannelBuffer wrappedBuffer = ChannelBuffers.wrappedBuffer( bytes );
        wrappedBuffer.resetWriterIndex();
        BlockLogBuffer buffer = new BlockLogBuffer( wrappedBuffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        byte[] bytesValue = new byte[600];
        bytesValue[1] = 1;
        bytesValue[99] = 2;
        bytesValue[199] = 3;
        bytesValue[299] = 4;
        bytesValue[399] = 5;
        bytesValue[499] = 6;
        bytesValue[599] = 7;
        buffer.put( bytesValue, bytesValue.length );
        buffer.close();

        byte[] actual;
        BlockLogReader reader = new BlockLogReader( wrappedBuffer );
        ByteBuffer verificationBuffer = ByteBuffer.wrap( new byte[1000] );
        reader.read( verificationBuffer );
        verificationBuffer.flip();
        actual = new byte[255];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 0, 255 ) ) );
        actual = new byte[255];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 255, 510 ) ) );
        actual = new byte[90];
        verificationBuffer.get( actual );
        assertThat( actual, new ArrayMatches<>( Arrays.copyOfRange( bytesValue, 510, 600 ) ) );
    }

    private class ArrayMatches<T> extends BaseMatcher<T>
    {
        private final T expected;
        private Object actual;

        ArrayMatches( T expected )
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
            descr.appendText( String.format( "expected %s, got %s", toString( expected ),
                    toString( actual ) ) );
        }

        private String toString( Object value )
        {
            if ( value instanceof byte[] )
            {
                return Arrays.toString( (byte[]) value ) + "; len=" + ( (byte[]) value ).length;
            }
            if ( value instanceof char[] )
            {
                return Arrays.toString( (char[]) value ) + "; len=" + ( (char[]) value ).length;
            }
            return "" + value;
        }
    }
}
