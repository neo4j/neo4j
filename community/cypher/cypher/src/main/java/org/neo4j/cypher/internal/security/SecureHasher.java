/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.security;

import java.util.HashMap;
import java.util.Map;
import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.crypto.RandomNumberGenerator;
import org.apache.shiro.crypto.SecureRandomNumberGenerator;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.lang.util.ByteSource;

public class SecureHasher {
    private final String hashAlgorithm;
    private final int hashIterations;
    private static final int SALT_BYTES_SIZE = 32;

    private RandomNumberGenerator randomNumberGenerator;
    private HashedCredentialsMatcher hashedCredentialsMatcher;
    private Map<Integer, HashedCredentialsMatcher> hashedCredentialsMatchers;

    public SecureHasher() {
        this(SecureHasherConfigurations.CURRENT_VERSION);
    }

    public SecureHasher(String version) {
        SecureHasherConfiguration configuration = SecureHasherConfigurations.configurations.get(version);
        hashAlgorithm = configuration.algorithm;
        hashIterations = configuration.iterations;
    }

    private RandomNumberGenerator getRandomNumberGenerator() {
        if (randomNumberGenerator == null) {
            randomNumberGenerator = new SecureRandomNumberGenerator();
        }

        return randomNumberGenerator;
    }

    public SimpleHash hash(byte[] source) {
        ByteSource salt = generateRandomSalt();
        return new SimpleHash(hashAlgorithm, source, salt, hashIterations);
    }

    public HashedCredentialsMatcher getHashedCredentialsMatcher() {
        if (hashedCredentialsMatcher == null) {
            hashedCredentialsMatcher = new HashedCredentialsMatcher(hashAlgorithm);
            hashedCredentialsMatcher.setHashIterations(hashIterations);
        }

        return hashedCredentialsMatcher;
    }

    public HashedCredentialsMatcher getHashedCredentialsMatcherWithIterations(int iterations) {
        if (hashedCredentialsMatchers == null) {
            hashedCredentialsMatchers = new HashMap<>();
        }

        HashedCredentialsMatcher matcher = hashedCredentialsMatchers.get(iterations);
        if (matcher == null) {
            matcher = new HashedCredentialsMatcher(hashAlgorithm);
            matcher.setHashIterations(iterations);
            hashedCredentialsMatchers.put(iterations, matcher);
        }

        return matcher;
    }

    private ByteSource generateRandomSalt() {
        return getRandomNumberGenerator().nextBytes(SecureHasher.SALT_BYTES_SIZE);
    }
}
