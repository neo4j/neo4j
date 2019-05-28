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
package org.neo4j.bolt.v1.packstream.utf8;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.neo4j.bolt.v1.packstream.PackStream.PackStreamException;
import org.neo4j.common.HexPrinter;

import static org.neo4j.util.FeatureToggles.getInteger;

/**
 * This is a specialized UTF-8 encoder that performs GC-free string encoding.
 * In Java 11, {@link String} could either be encoded in UTF16 or LATIN1 format.
 * The default conversion {@link String#getBytes(Charset)}
 * from Java 11 string format to UTF-8 generates a lot of byte array objects.
 * <p>
 * This solves with modified GC-free conversion code modified based on string internal implementation.
 * Future work here could stop use of String's for the main database paths.
 * We already have Token, which
 * could easily contain pre-encoded UTF-8 data, and "runtime" Strings could be
 * handled with a custom type that is more stability friendly, for instance
 * by building on to StringProperty.
 */
public class GCFreeUTF8Encoder implements UTF8Encoder
{
    private static final int BUFFER_SIZE = getInteger( GCFreeUTF8Encoder.class, "buffer_size", 1024 * 16 );

    private static final MethodHandle getByteArray = byteArrayGetter();
    private static final MethodHandle getCoder = coderGetter();
    private static final MethodHandle hasNegatives = hasNegatives();
    private static final MethodHandle getChar = getChar();

    private static final byte UTF16 = 1;

    private final UTF8Encoder fallbackEncoder = new VanillaUTF8Encoder();
    private final byte[] out = new byte[BUFFER_SIZE];
    private final ByteBuffer outBuf = ByteBuffer.wrap( out );

    @Override
    public ByteBuffer encode( String input )
    {
        try
        {
            byte coder = (byte) getCoder.invoke( input );
            final byte[] rawBytes = (byte[]) getByteArray.invoke( input );

            int len = encodeUTF8( coder, rawBytes, out );
            if ( len == -1 )
            {
                return fallbackEncoder.encode( input );
            }

            outBuf.position( 0 );
            outBuf.limit( len );
            return outBuf;
        }
        catch ( Throwable e )
        {
            throw new AssertionError( "This encoder depends on java.lang.StringCoding, which failed to load or apply: " + e.getMessage(), e );
        }
    }

    /**
     * Modified version of {@link StringCoding#encodeUTF8(byte, byte[], boolean)} without extra buffer.
     */
    private static int encodeUTF8( byte coder, byte[] val, byte[] dst ) throws Throwable
    {
        if ( coder == UTF16 )
        {
            return encodeUTF8_UTF16( val, dst );
        }

        if ( !(boolean) hasNegatives.invoke( val, 0, val.length ) )
        {
            if ( val.length > dst.length )
            {
                return -1;
            }
            System.arraycopy( val, 0, dst, 0, val.length );
            return val.length;
        }

        int len = val.length << 1;
        if ( len > dst.length )
        {
            return -1;
        }

        int dp = 0;
        for ( int sp = 0; sp < val.length; sp++ )
        {
            byte c = val[sp];
            if ( c < 0 )
            {
                dst[dp++] = (byte) (0xc0 | ((c & 0xff) >> 6));
                dst[dp++] = (byte) (0x80 | (c & 0x3f));
            }
            else
            {
                dst[dp++] = c;
            }
        }

        return dp;
    }

    /**
     * Modified version of {@link StringCoding#encodeUTF8_UTF16(byte[], boolean)} without extra buffer.
     */
    private static int encodeUTF8_UTF16( byte[] val, byte[] dst ) throws Throwable
    {
        int dp = 0;
        int sp = 0;
        int sl = val.length >> 1;

        int len = sl * 3;
        if ( len > dst.length )
        {
            return -1;
        }

        char c;
        while ( sp < sl && (c = (char) getChar.invoke( val, sp )) < '\u0080' )
        {
            // ascii fast loop;
            dst[dp++] = (byte) c;
            sp++;
        }
        while ( sp < sl )
        {
            c = (char) getChar.invoke( val, sp++ );
            if ( c < 0x80 )
            {
                dst[dp++] = (byte) c;
            }
            else if ( c < 0x800 )
            {
                dst[dp++] = (byte) (0xc0 | (c >> 6));
                dst[dp++] = (byte) (0x80 | (c & 0x3f));
            }
            else if ( Character.isSurrogate( c ) )
            {
                int uc = -1;
                char c2;
                if ( Character.isHighSurrogate( c ) && sp < sl && Character.isLowSurrogate( c2 = (char) getChar.invoke( val, sp ) ) )
                {
                    uc = Character.toCodePoint( c, c2 );
                }
                if ( uc < 0 )
                {
                    // encode without replacement.
                    throw new PackStreamException( String.format( "Failure when converting to UTF-8. " +
                            "String: %s\n%s", new String( val, StandardCharsets.UTF_16 ), HexPrinter.hex( val ) ) );
                }
                else
                {
                    dst[dp++] = (byte) (0xf0 | ((uc >> 18)));
                    dst[dp++] = (byte) (0x80 | ((uc >> 12) & 0x3f));
                    dst[dp++] = (byte) (0x80 | ((uc >> 6) & 0x3f));
                    dst[dp++] = (byte) (0x80 | (uc & 0x3f));
                    sp++;  // 2 chars
                }
            }
            else
            {
                // 3 bytes, 16 bits
                dst[dp++] = (byte) (0xe0 | ((c >> 12)));
                dst[dp++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                dst[dp++] = (byte) (0x80 | (c & 0x3f));
            }
        }

        return dp;
    }

    private static MethodHandle hasNegatives()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            final Method hasNegatives = Class.forName( "java.lang.StringCoding" ).getMethod( "hasNegatives", byte[].class, int.class, int.class );
            hasNegatives.setAccessible( true );
            return lookup.unreflect( hasNegatives );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends on java.lang.StringCoding, which failed to load: " +
                    e.getMessage(), e );
        }
    }

    private static MethodHandle getChar()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            final Method getChar = Class.forName( "java.lang.StringUTF16" ).getDeclaredMethod( "getChar", byte[].class, int.class );
            getChar.setAccessible( true );
            return lookup.unreflect( getChar );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends on java.lang.StringUTF16, which failed to load: " +
                            e.getMessage(), e );
        }
    }

    private static MethodHandle coderGetter()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            Field coder = String.class.getDeclaredField( "coder" );
            if ( coder.getType() != byte.class )
            {
                throw new AssertionError(
                        "This encoder depends being able to access raw byte in java.lang.String, but the class is backed by a " +
                                coder.getType().getCanonicalName() );
            }
            coder.setAccessible( true );
            return lookup.unreflectGetter( coder );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends being able to access raw byte in java.lang.String, which failed: " +
                            e.getMessage(), e );
        }
    }

    private static MethodHandle byteArrayGetter()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            Field value = String.class.getDeclaredField( "value" );
            if ( value.getType() != byte[].class )
            {
                throw new AssertionError(
                        "This encoder depends being able to access raw byte[] in java.lang.String, but the class is backed by a " +
                                value.getType().getCanonicalName() );
            }
            value.setAccessible( true );
            return lookup.unreflectGetter( value );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends being able to access raw byte[] in java.lang.String, which failed: " +
                    e.getMessage(), e );
        }
    }
}
