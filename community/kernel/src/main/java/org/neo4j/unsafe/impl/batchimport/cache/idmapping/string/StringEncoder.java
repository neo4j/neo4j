/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.Arrays;

/**
 * Encodes String into a long with very small chance of collision, i.e. two different Strings encoded into
 * the same long value.
 *
 * Assumes a single thread making all calls to {@link #encode(String)}.
 */
public class StringEncoder
{
    private static long UPPER_INT_MASK = 0x00000000_FFFFFFFFL;
    private static final int FOURTH_BYTE = 0x000000FF;

    private final int numCodes;
    // A state-ful byte[] that changes as part of each call to encode
    private final byte[] reMap = new byte[256];
    private final int encodingThreshold = 7;

    private int numChars;

    // scratch data structures
    private int[] encodedInt;
    private byte[] encodedBytes = new byte[128];
    private final int[] codes;

    public StringEncoder( int codingStrength )
    {
        this.numCodes = codingStrength > 2 ? codingStrength : 2;
        this.codes = new int[numCodes];
        Arrays.fill( reMap, (byte)-1 );
    }

    public long encode( String s )
    {
        int[] val = encodeInt( s );
        return (long) val[0] << 32 | val[1] & UPPER_INT_MASK;
    }

    private int[] encodeInt( String s )
    {
        // construct bytes from string
        int length = s.length();
        byte[] bytes = encodedBytes( length );
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) ((s.charAt( i )) % 127);
        }
        reMap( bytes, length );
        // encode
        if ( length <= encodingThreshold )
        {
            return simplestCode( bytes, length, encodedInt );
        }
        for ( int i = 0; i < codes.length; )
        {
            codes[i] = getCode( bytes, length, 1 );//relPrimes[index][i]);
            codes[i + 1] = getCode( bytes, length, bytes.length - 1 );//relPrimes[index][i+1]);
            i += 2;
        }
        int carryOver = lengthEncoder( length ) << 1;
        int temp = 0;
        for ( int i = 0; i < codes.length; i++ )
        {
            temp = codes[i] & FOURTH_BYTE;
            codes[i] = codes[i] >>> 8 | carryOver << 24;
            carryOver = temp;
        }
        return codes;
    }

    private byte[] encodedBytes( int length )
    {
        if ( length > encodedBytes.length )
        {
            encodedBytes = new byte[length];
        }
        return encodedBytes;
    }

    private static int lengthEncoder( int length )
    {
        if ( length < 32 )
        {
            return length;
        }
        else if ( length <= 96 )
        {
            return length >> 1;
        }
        else if ( length <= 324 )
        {
            return length >> 2;
        }
        else if ( length <= 580 )
        {
            return length >> 3;
        }
        else if ( length <= 836 )
        {
            return length >> 4;
        }
        else
        {
            return 127;
        }
    }

    private void reMap( byte[] bytes, int length )
    {
        for ( int i = 0; i < length; i++ )
        {
            if ( reMap[bytes[i]] == -1 )
            {
                reMap[bytes[i]] = (byte) (numChars++ % 256);
            }
            bytes[i] = reMap[bytes[i]];
        }
    }

    private static int[] simplestCode( byte[] bytes, int length, int[] target )
    {
        target[0] = bytes.length << 25;
        for ( int i = 0; i < 3 && i < length; i++ )
        {
            target[0] = target[0] | bytes[i] << ((2 - i) * 8);
        }
        for ( int i = 3; i < 7 && i < length; i++ )
        {
            target[1] = target[1] | (bytes[i]) << ((6 - i) * 8);
        }
        return target;
    }

    private static int getCode( byte[] bytes, int length, int order )
    {
        long code = 0;
        for ( int i = 0; i < length; i++ )
        {
            //code += (((long)bytes[(i*order) % size]) << (i % 7)*8);
            long val = bytes[(i * order) % length];
            for ( int k = 1; k <= i; k++ )
            {
                long prev = val;
                val = ((val << 4) + prev);//% Integer.MAX_VALUE;
            }
            code += val;
        }
        return (int) code;
    }

    public static int GCD( int a, int b )
    {
        int temp;
        if ( a < b )
        {
            temp = a;
            a = b;
            b = temp;
        }
        if ( a % b == 0 )
        {
            return (b);
        }
        return (GCD( a % b, b ));
    }
}
