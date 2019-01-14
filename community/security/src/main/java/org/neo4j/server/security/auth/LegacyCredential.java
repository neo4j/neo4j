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
package org.neo4j.server.security.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.string.HexString;
import org.neo4j.string.UTF8;

/**
 * This class is used for community security, InternalFlatFile, SetDefaultAdminCommand and SetInitialPasswordCommand
 * The new commercial security has its own more secure version of Credential
 */
public class LegacyCredential implements Credential
{
    public static final String DIGEST_ALGO = "SHA-256";

    public static final LegacyCredential INACCESSIBLE = new LegacyCredential( new byte[]{}, new byte[]{} );

    private static final Random random = new SecureRandom();

    private final byte[] salt;
    private final byte[] passwordHash;

    public static LegacyCredential forPassword( byte[] password )
    {
        byte[] salt = randomSalt();
        return new LegacyCredential( salt, hash( salt, password ) );
    }

    // For testing purposes only!
    public static LegacyCredential forPassword( String password )
    {
        return forPassword( UTF8.encode( password ) );
    }

    public LegacyCredential( byte[] salt, byte[] passwordHash )
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

    @Override
    public boolean matchesPassword( byte[] password )
    {
        return byteEquals( passwordHash, hash( salt, password ) );
    }

    // For testing purposes only!
    @Override
    public boolean matchesPassword( String password )
    {
        return byteEquals( passwordHash, hash( salt, UTF8.encode( password ) ) );
    }

    @Override
    public String serialize()
    {
        return new UserSerialization().serialize( this );
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
    private static boolean byteEquals( byte[] actual, byte[] given )
    {
        if ( actual == given )
        {
            return true;
        }
        if ( actual == null || given == null )
        {
            return false;
        }

        int actualLength = actual.length;
        int givenLength = given.length;
        boolean result = true;

        for ( int i = 0; i < givenLength; ++i )
        {
            if ( actualLength > 0 )
            {
                result &= actual[i % actualLength] == given[i];
            }
        }
        return result && actualLength == givenLength;
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

        LegacyCredential that = (LegacyCredential) o;

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

    private static byte[] hash( byte[] salt, byte[] password )
    {
        try
        {
            MessageDigest m = MessageDigest.getInstance( DIGEST_ALGO );
            m.update( salt, 0, salt.length );
            m.update( password, 0, password.length );
            return m.digest();
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "Hash algorithm is not available on this platform: " + e.getMessage(), e );
        }
    }

    private static byte[] randomSalt()
    {
        byte[] salt = new byte[32];
        random.nextBytes( salt );
        return salt;
    }
}
