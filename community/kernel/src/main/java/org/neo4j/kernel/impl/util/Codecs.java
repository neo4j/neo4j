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
package org.neo4j.kernel.impl.util;

public class Codecs
{
    final private static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Converts a byte array to a hexadecimal string.
     *
     * @param bytes Bytes to be encoded
     * @return A string of hex characters [0-9A-F]
     */
    public static String encodeHexString( byte[] bytes )
    {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ )
        {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String( hexChars );
    }

    /**
     * Converts a hexadecimal string to a byte array
     *
     * @param hexString A string of hexadecimal characters [0-9A-Fa-f] to decode
     * @return Decoded bytes, or null if the {@param hexString} is not valid
     */
    public static byte[] decodeHexString( String hexString )
    {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for ( int i = 0, j = 0; i < len; i += 2, j++ )
        {
            int highByte = Character.digit( hexString.charAt( i ), 16 ) << 4;
            int lowByte = Character.digit( hexString.charAt( i + 1 ), 16 );
            if ( highByte < 0 || lowByte < 0 )
            {
                return null;
            }
            data[j] = (byte) ( highByte + lowByte );
        }
        return data;
    }
}
