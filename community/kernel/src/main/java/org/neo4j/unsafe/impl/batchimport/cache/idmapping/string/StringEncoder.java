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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.Arrays;

import static java.lang.Math.max;

/**
 * Encodes String into a long with very small chance of collision, i.e. two different Strings encoded into
 * the same long value.
 *
 * Assumes a single thread making all calls to {@link #encode(String)}.
 */
public class StringEncoder implements Encoder
{
    private static long UPPER_INT_MASK = 0x00000000_FFFFFFFFL;
    private static final int FOURTH_BYTE = 0x000000FF;

    // fixed values
    private final int numCodes;
    private final int encodingThreshold = 7;

    // data changing over time, potentially with each encoding
    private final byte[] reMap = new byte[256];
    private int numChars;

    public StringEncoder()
    {
        this( 2 );
    }

    public StringEncoder( int codingStrength )
    {
        numCodes = codingStrength > 2 ? codingStrength : 2;
        Arrays.fill( reMap, (byte)-1 );
    }

    @Override
    public long encode( Object s )
    {
        int[] val = encodeInt( (String) s );
        return (long) val[0] << 32 | val[1] & UPPER_INT_MASK;
    }

    private int[] encodeInt( String s )
    {
        // construct bytes from string
        int inputLength = s.length();
        byte[] bytes = new byte[inputLength];
        for ( int i = 0; i < inputLength; i++ )
        {
            bytes[i] = (byte) ((s.charAt( i )) % 127);
        }
        reMap( bytes, inputLength );
        // encode
        if ( inputLength <= encodingThreshold )
        {
            return simplestCode( bytes, inputLength );
        }
        int[] codes = new int[numCodes];
        for ( int i = 0; i < numCodes; )
        {
            codes[i] = getCode( bytes, inputLength, 1 );
            codes[i + 1] = getCode( bytes, inputLength, inputLength - 1 );
            i += 2;
        }
        int carryOver = lengthEncoder( inputLength ) << 1;
        int temp = 0;
        for ( int i = 0; i < numCodes; i++ )
        {
            temp = codes[i] & FOURTH_BYTE;
            codes[i] = codes[i] >>> 8 | carryOver << 24;
            carryOver = temp;
        }
        return codes;
    }

    private int lengthEncoder( int length )
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

    private void reMap( byte[] bytes, int inputLength )
    {
        for ( int i = 0; i < inputLength; i++ )
        {
            if ( reMap[bytes[i]] == -1 )
            {
                reMap[bytes[i]] = (byte) (numChars++ % 256);
            }
            bytes[i] = reMap[bytes[i]];
        }
    }

    private int[] simplestCode( byte[] bytes, int inputLength )
    {
        int[] codes = new int[]{0, 0};
        codes[0] = max( inputLength, 1 ) << 25;
        codes[1] = 0;
        for ( int i = 0; i < 3 && i < inputLength; i++ )
        {
            codes[0] = codes[0] | bytes[i] << ((2 - i) * 8);
        }
        for ( int i = 3; i < 7 && i < inputLength; i++ )
        {
            codes[1] = codes[1] | (bytes[i]) << ((6 - i) * 8);
        }
        return codes;
    }

    private int getCode( byte[] bytes, int inputLength, int order )
    {
        long code = 0;
        int size = inputLength;
        for ( int i = 0; i < size; i++ )
        {
            //code += (((long)bytes[(i*order) % size]) << (i % 7)*8);
            long val = bytes[(i * order) % size];
            for ( int k = 1; k <= i; k++ )
            {
                long prev = val;
                val = ((val << 4) + prev);//% Integer.MAX_VALUE;
            }
            code += val;
        }
        return (int) code;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + numCodes + "]";
    }
}
