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

public class StringEncoder
{
    private static long UPPER_INT_MASK = 0x00000000_FFFFFFFFL;
    private static final int FOURTH_BYTE = 0x000000FF;

    private final int numCodes;
    private final byte[] reMap = new byte[256];
    private final int numShorts, numInts, numLongs;
    private final int encodingThreshold = 7;
    private final int codeLengthBytes;

    private int numChars;
    private int maxIdLengthEncountered;
    private int longIds;

    public StringEncoder( int maxLength, int codingStrength )
    {
        this.numCodes = codingStrength > 2 ? codingStrength : 2;
        this.codeLengthBytes = numCodes * 4;
        this.numShorts = codeLengthBytes / 2;
        this.numInts = codeLengthBytes / 4;
        this.numLongs = codeLengthBytes % 8 == 0 ? codeLengthBytes / 8 : codeLengthBytes / 8 + 1;
        Arrays.fill( reMap, (byte)-1 );
    }

    public int[] getLongIdStats()
    {
        return new int[] { longIds, maxIdLengthEncountered };
    }

    public long encode( String s )
    {
        int[] val = encodeInt( s );
        return (long) val[0] << 32 | val[1] & UPPER_INT_MASK;
    }

    private int[] encodeInt( String s )
    {
        int[] codes = new int[numCodes];
        // construct bytes from string
        byte[] bytes = new byte[s.length()];
        for ( int i = 0; i < s.length(); i++ )
        {
            bytes[i] = (byte) ((s.charAt( i )) % 127);
        }
        reMap( bytes );
        if ( bytes.length > 127 )
        {
            longIds++;
            if ( bytes.length > maxIdLengthEncountered )
            {
                maxIdLengthEncountered = bytes.length;
            }
        }
        // encode
        if ( bytes.length <= encodingThreshold )
        {
            return simplestCode( bytes );
        }
        for ( int i = 0; i < codes.length; )
        {
            codes[i] = getCode( bytes, 1 );//relPrimes[index][i]);
            codes[i + 1] = getCode( bytes, bytes.length - 1 );//relPrimes[index][i+1]);
            i += 2;
        }
        int carryOver = lengthEncoder( bytes.length ) << 1;
        int temp = 0;
        for ( int i = 0; i < codes.length; i++ )
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

    private void reMap( byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            if ( reMap[bytes[i]] == -1 )
            {
                reMap[bytes[i]] = (byte) (numChars++ % 256);
            }
            bytes[i] = reMap[bytes[i]];
        }
    }

    private int[] simplestCode( byte[] bytes )
    {
        int[] codes = new int[] { 0, 0 };
        codes[0] = bytes.length << 25;
        for ( int i = 0; i < 3 && i < bytes.length; i++ )
        {
            codes[0] = codes[0] | bytes[i] << ((2 - i) * 8);
        }
        for ( int i = 3; i < 7 && i < bytes.length; i++ )
        {
            codes[1] = codes[1] | (bytes[i]) << ((6 - i) * 8);
        }
        return codes;
    }

    private int getCode( byte[] bytes, int order )
    {
        long code = 0;
        int size = bytes.length;
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
