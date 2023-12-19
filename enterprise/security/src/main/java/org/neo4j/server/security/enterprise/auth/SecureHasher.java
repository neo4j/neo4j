/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
