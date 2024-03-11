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

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SaltedAuthenticationInfo;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.lang.codec.Hex;
import org.apache.shiro.lang.util.ByteSource;
import org.apache.shiro.subject.PrincipalCollection;
import org.neo4j.kernel.impl.security.Credential;

public class SystemGraphCredential implements Credential {
    private final SecureHasher secureHasher;
    private final SimpleHash hashedCredentials;

    private SystemGraphCredential(SecureHasher secureHasher, SimpleHash hash) {
        this.secureHasher = secureHasher;
        this.hashedCredentials = hash;
    }

    @Override
    public boolean matchesPassword(byte[] password) {
        // TODO: Create a new CredentialMatcher class that extends HashedCredentialsMatcher
        //       and adds a tailored match-method so we do not need to create these
        //       virtual AuthenticationToken and AuthenticationInfo objects
        return secureHasher
                .getHashedCredentialsMatcherWithIterations(hashedCredentials.getIterations())
                .doCredentialsMatch(
                        new AuthenticationToken()
                        // This is just password wrapped in an AuthenticationToken
                        {
                            @Override
                            public Object getCredentials() {
                                return password;
                            }

                            @Override
                            public Object getPrincipal() {
                                return null;
                            }
                        },
                        new SaltedAuthenticationInfo()
                        // This is just hashedCredentials wrapped in an AuthenticationInfo
                        {
                            @Override
                            public Object getCredentials() {
                                return hashedCredentials.getBytes();
                            }

                            @Override
                            public ByteSource getCredentialsSalt() {
                                return hashedCredentials.getSalt();
                            }

                            @Override
                            public PrincipalCollection getPrincipals() {
                                return null;
                            }
                        });
    }

    public static SystemGraphCredential createCredentialForPassword(byte[] password, SecureHasher secureHasher) {
        SimpleHash hash = secureHasher.hash(password);
        return new SystemGraphCredential(secureHasher, hash);
    }

    @Override
    public String serialize() {
        return serialize(this);
    }

    public static String serialize(SystemGraphCredential credential) {
        String algorithm = credential.hashedCredentials.getAlgorithmName();
        String iterations = Integer.toString(credential.hashedCredentials.getIterations());
        String encodedSalt = credential.hashedCredentials.getSalt().toHex();
        String encodedPassword = credential.hashedCredentials.toHex();
        return String.join(CREDENTIAL_SEPARATOR, algorithm, encodedPassword, encodedSalt, iterations);
    }

    public static String serialize(byte[] encodedCredential) throws IllegalArgumentException {
        Pattern validEncryptedPassword =
                Pattern.compile(String.join(CREDENTIAL_SEPARATOR, "^([0-9])", "([A-Fa-f0-9]+)", "([A-Fa-f0-9]+)"));
        String encryptedPasswordString = new String(encodedCredential, StandardCharsets.UTF_8);

        Matcher matcher = validEncryptedPassword.matcher(encryptedPasswordString);
        if (matcher.matches()) {
            String version = matcher.group(1);
            String hash = matcher.group(2);
            String salt = matcher.group(3);
            SecureHasherConfiguration configuration = SecureHasherConfigurations.configurations.get(version);

            if (configuration == null) {
                throw new IllegalArgumentException("The encryption version specified is not available.");
            }

            return String.join(
                    CREDENTIAL_SEPARATOR,
                    configuration.algorithm,
                    hash,
                    salt,
                    String.valueOf(configuration.iterations));
        } else {
            throw new IllegalArgumentException(
                    "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'.");
        }
    }

    public static String maskSerialized(String serialized) throws IllegalArgumentException {
        Pattern validSerialized = Pattern.compile(
                String.join(CREDENTIAL_SEPARATOR, "^([A-Za-z0-9\\-]+)", "([A-Fa-f0-9]+)", "([A-Fa-f0-9]+)")
                        + "(?:,([0-9]+))?");

        Matcher matcher = validSerialized.matcher(serialized);
        if (matcher.matches()) {
            String algorithm = matcher.group(1);
            String hash = matcher.group(2);
            String salt = matcher.group(3);
            String iterationGroup = matcher.group(4);
            int iterations = iterationGroup != null ? Integer.parseInt(iterationGroup) : 1;
            String version = SecureHasherConfigurations.getVersionForConfiguration(algorithm, iterations);

            return String.join(CREDENTIAL_SEPARATOR, version, hash, salt);
        } else {
            throw new IllegalArgumentException("Invalid serialized credential.");
        }
    }

    public static SystemGraphCredential deserialize(String part, SecureHasher secureHasher) throws FormatException {
        String[] split = part.split(CREDENTIAL_SEPARATOR, -1);
        if (split.length < 3 || split.length > 4) {
            throw new FormatException("wrong number of credential fields");
        }
        String algorithm = split[0];
        byte[] decodedPassword = Hex.decode(split[1]);
        byte[] decodedSalt = Hex.decode(split[2]);
        int iterations = split.length == 4 ? Integer.parseInt(split[3]) : 1;
        SimpleHash hash = new SimpleHash(algorithm);
        hash.setBytes(decodedPassword);
        hash.setSalt(ByteSource.Util.bytes(decodedSalt));
        hash.setIterations(iterations);
        return new SystemGraphCredential(secureHasher, hash);
    }
}
