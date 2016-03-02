/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.security.auth;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.string.UTF8;

public abstract class AuthUtils
{
    /**
     * Returns a hash of the store id
     * @param storeId the store id to hash
     * @return a hash of the store id
     */
    public static String uniqueIdentifier( StoreId storeId )
    {
        MessageDigest messageDigest;
        try
        {
            messageDigest = MessageDigest.getInstance( "SHA-256" );
            messageDigest.update( UTF8.encode( storeId.toString() ) );
            byte[] digest = messageDigest.digest();
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream( digest.length );
            PrintStream stream = new PrintStream( byteArrayStream );
            new HexPrinter( stream )
                    .withByteSeparator( "" )
                    .withGroupSeparator( "" )
                    .append( digest );
            stream.flush();
            return byteArrayStream.toString();
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "Hash algorithm is not available on this platform: " + e.getMessage(), e );
        }
    }
}
