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
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import static org.neo4j.util.FeatureToggles.getInteger;

/**
 * This is a specialized UTF-8 encoder that solves two predicaments:
 * <p>
 * 1) There's no way using public APIs to do GC-free string encoding unless
 * you build a custom encoder, and GC output from UTF-8 encoding causes
 * production instability
 * 2) The ArrayEncoder provided by HotSpot is 2 orders faster for UTF-8 encoding
 * for a massive amount of real-world strings due to specialized handling of
 * ascii, and we can't import that since we need to compile on IBM J9
 * <p>
 * We can't solve (1) without solving (2), because the default GC-spewing String#getBytes()
 * uses the optimized ArrayEncoder, meaning it's easy to write an encoder that
 * is GC-free, but then it'll be two orders slower than the stdlib, and vice
 * versa.
 * <p>
 * This solves both issues using MethodHandles. Future work here could include
 * writing a custom UTF-8 encoder (which could then avoid using ArrayEncoder),
 * as well as stopping use of String's for the main database paths.
 * We already have  Token, which
 * could easily contain pre-encoded UTF-8 data, and "runtime" Strings could be
 * handled with a custom type that is more stability friendly, for instance
 * by building on to StringProperty.
 */
public class SunMiscUTF8Encoder implements UTF8Encoder
{
    private static final int BUFFER_SIZE = getInteger( SunMiscUTF8Encoder.class, "buffer_size", 1024 * 16 );
    private static final int fallbackAtStringLength =
            (int) (BUFFER_SIZE / StandardCharsets.UTF_8.newEncoder().averageBytesPerChar());
    private static final MethodHandle getCharArray = charArrayGetter();
    private static final MethodHandle arrayEncode = arrayEncode();
    private static final MethodHandle getOffset = offsetHandle();
    private final CharsetEncoder charsetEncoder = StandardCharsets.UTF_8.newEncoder();

    private final byte[] out = new byte[BUFFER_SIZE];
    private final ByteBuffer outBuf = ByteBuffer.wrap( out );
    private final UTF8Encoder fallbackEncoder = new VanillaUTF8Encoder();

    @Override
    public ByteBuffer encode( String input )
    {
        try
        {
            // If it's unlikely we will fit the encoded data, just use stdlib encoder
            if ( input.length() > fallbackAtStringLength )
            {
                return fallbackEncoder.encode( input );
            }

            char[] rawChars = (char[]) getCharArray.invoke( input );
            int len = (int) arrayEncode.invoke( charsetEncoder, rawChars, offset(input), input.length(), out );

            if ( len == -1 )
            {
                return fallbackEncoder.encode( input );
            }

            outBuf.position( 0 );
            outBuf.limit( len );
            return outBuf;
        }
        catch ( ArrayIndexOutOfBoundsException e )
        {
            // This happens when we can't fit the encoded string.
            // We try and avoid this altogether by falling back to the
            // vanilla encoder if the string looks like it'll not fit -
            // but this is probabilistic since we don't know until we've encoded.
            // So, if our guess is wrong, we fall back here instead.
            return fallbackEncoder.encode( input );
        }
        catch ( Throwable e )
        {
            throw new AssertionError( "This encoder depends on sun.nio.cs.ArrayEncoder, which failed to load: " +
                                      e.getMessage(), e );
        }
    }

    private static MethodHandle arrayEncode()
    {
        // Because we need to be able to compile on IBM's JVM, we can't
        // depend on ArrayEncoder. Unfortunately, ArrayEncoders encode method
        // is twoish orders of magnitude faster than regular encoders for ascii
        // so we go through the hurdle of calling that encode method via
        // a MethodHandle.
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            return lookup.unreflect( Class.forName( "sun.nio.cs.ArrayEncoder" )
                    .getMethod( "encode", char[].class, int.class, int.class, byte[].class ) );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends on sun.nio.cs.ArrayEncoder, which failed to load: " +
                    e.getMessage(), e );
        }
    }

    private static MethodHandle charArrayGetter()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            Field value = String.class.getDeclaredField( "value" );
            if ( value.getType() != char[].class )
            {
                throw new AssertionError(
                        "This encoder depends being able to access raw char[] in java.lang.String, but the class is backed by a " +
                                value.getType().getCanonicalName() );
            }
            value.setAccessible( true );
            return lookup.unreflectGetter( value );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends being able to access raw char[] in java.lang.String, which failed: " +
                    e.getMessage(), e );
        }
    }

    /*
     * If String.class is backed by a char[] together with an offset, return
     * the offset otherwise return 0.
     */
    private static int offset( String value )
    {
        try
        {
            return getOffset == null ? 0 : (int) getOffset.invoke( value );
        }
        catch ( Throwable e )
        {
            throw new AssertionError(
                    "This encoder depends being able to access the offset in the char[] array in java.lang.String, " +
                    "which failed: " +
                    e.getMessage(), e );
        }
    }

    private static MethodHandle offsetHandle()
    {
        //We need access to the internal char[] in order to do gc free
        //encoding. However for ibm jdk it is not always true that
        //"foo" is backed by exactly ['f', 'o', 'o'], for example single
        //ascii characters strings like "a" is backed by:
        //
        //    value = ['0', '1', ..., 'A', 'B', ..., 'a', 'b', ...]
        //    offset = 'a'
        //
        //Hence we need access both to `value` and `offset`
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            Field value = String.class.getDeclaredField( "offset" );
            value.setAccessible( true );
            return lookup.unreflectGetter( value );
        }
        catch ( Throwable e )
        {
            //there is no offset in String implementation
            return null;
        }
    }
}
