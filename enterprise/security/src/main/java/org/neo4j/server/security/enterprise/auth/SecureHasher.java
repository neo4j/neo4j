/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.crypto.RandomNumberGenerator;
import org.apache.shiro.crypto.SecureRandomNumberGenerator;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

public class SecureHasher
{
    // TODO: Do we need to make this configurable?
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int HASH_ITERATIONS = 1024;
    private static final int SALT_BYTES_SIZE = 16;

    private RandomNumberGenerator randomNumberGenerator;
    private HashedCredentialsMatcher hashedCredentialsMatcher;

    private RandomNumberGenerator getRandomNumberGenerator()
    {
        if ( randomNumberGenerator == null )
        {
            randomNumberGenerator = new SecureRandomNumberGenerator();
        }

        return randomNumberGenerator;
    }

    public SimpleHash hash( byte[] source )
    {
        ByteSource salt = generateRandomSalt( SALT_BYTES_SIZE );
        return new SimpleHash( HASH_ALGORITHM, source, salt, HASH_ITERATIONS );
    }

    public HashedCredentialsMatcher getHashedCredentialsMatcher()
    {
        if ( hashedCredentialsMatcher == null )
        {
            hashedCredentialsMatcher = new HashedCredentialsMatcher( HASH_ALGORITHM );
            hashedCredentialsMatcher.setHashIterations( HASH_ITERATIONS );
        }

        return hashedCredentialsMatcher;
    }

    private ByteSource generateRandomSalt( int bytesSize )
    {
        return getRandomNumberGenerator().nextBytes( bytesSize );
    }
}
