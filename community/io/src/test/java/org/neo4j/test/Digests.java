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
package org.neo4j.test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Digests
{
    public static String md5Hex( String message )
    {
        try
        {
            MessageDigest m = MessageDigest.getInstance( "MD5" );
            m.update( message.getBytes(), 0, message.getBytes().length);
            return hex(m.digest());
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "MD5 hash algorithm is not available on this platform: " + e.getMessage(),e );
        }
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    // TODO: Replace with BytePrinter#compactHex once auth work is merged
    private static String hex(byte[] bytes)
    {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
