/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.string.HexString;
import org.neo4j.string.UTF8;

public class Credential
{
    public static final String DIGEST_ALGO = "SHA-256";

    public static final Credential INACCESSIBLE = new Credential( new byte[]{}, new byte[]{} );

    private final byte[] salt;
    private final byte[] passwordHash;

    public static Credential forPassword( String password )
    {
        byte[] salt = randomSalt();
        return new Credential( salt, hash( salt, password ) );
    }

    public Credential( byte[] salt, byte[] passwordHash )
    {
        this.salt = salt;
        this.passwordHash = passwordHash;
    }

    public byte[] salt()
    {
        return salt;
    }

    public byte[] passwordHash()
    {
        return passwordHash;
    }

    public boolean matchesPassword( String password )
    {
        return byteEquals( passwordHash, hash( salt, password ) );
    }

    /**
     * <p>Utility method that replaces Arrays.equals() to avoid timing attacks.
     * The length of the loop executed will always be the length of the given password.
     * Remember {@link #INACCESSIBLE} credentials should still execute loop for the length of given password.</p>
     *
     * @param actual the actual password
     * @param given password given by the user
     * @return whether the two byte arrays are equal
     */
    private boolean byteEquals( byte[] actual, byte[] given )
    {
        if ( actual == given )
        {
            return true;
        }
        if ( actual == null || given == null )
        {
            return false;
        }
        boolean result = true;
        boolean accessible = true;
        int actualLength = actual.length;
        int givenLength = given.length;
        for ( int i = 0; i < givenLength; ++i )
        {
            if ( actualLength == 0 )
            {
                accessible = false;
            }
            else
            {
                result &= actual[i % actualLength] == given[i];
            }
        }
        return result && actualLength == givenLength && accessible;
    }

    /**
     * <p>Equality to always check for both salt and password hash as a safeguard against timing attack.</p>
     */
    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Credential that = (Credential) o;

        boolean saltEquals = byteEquals( this.salt, that.salt );
        boolean passwordEquals = byteEquals( this.passwordHash, that.passwordHash );
        return saltEquals && passwordEquals;
    }

    @Override
    public int hashCode()
    {
        return 31 * Arrays.hashCode( salt ) + Arrays.hashCode( passwordHash );
    }

    @Override
    public String toString()
    {
        return "Credential{" +
               "salt=0x" + HexString.encodeHexString( salt ) +
               ", passwordHash=0x" + HexString.encodeHexString( passwordHash ) +
               '}';
    }

    private static byte[] hash( byte[] salt, String password )
    {
        try
        {
            byte[] passwordBytes = UTF8.encode( password );
            MessageDigest m = MessageDigest.getInstance( DIGEST_ALGO );
            m.update( salt, 0, salt.length );
            m.update( passwordBytes, 0, passwordBytes.length );
            return m.digest();
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "Hash algorithm is not available on this platform: " + e.getMessage(), e );
        }
    }

    private static byte[] randomSalt()
    {
        byte[] salt = new byte[16];
        ThreadLocalRandom.current().nextBytes( salt );
        return salt;
    }
}
