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
package org.neo4j.server.security.auth;

import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.kernel.impl.util.Codecs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

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
        return Arrays.equals( passwordHash, hash( salt, password ) );
    }

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

        return Arrays.equals( salt, that.salt ) && Arrays.equals( passwordHash, that.passwordHash );
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
                "salt=0x" + Codecs.encodeHexString( salt ) +
                ", passwordHash=0x" + Codecs.encodeHexString( passwordHash ) +
                '}';
    }

    private static byte[] hash( byte[] salt, String password )
    {
        try
        {
            byte[] passwordBytes = password.getBytes( Charsets.UTF_8 );
            MessageDigest m = MessageDigest.getInstance( DIGEST_ALGO );
            m.update( salt, 0, salt.length );
            m.update( passwordBytes, 0, passwordBytes.length );
            return m.digest();
        } catch ( NoSuchAlgorithmException e )
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
