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
package org.neo4j.pki;

import static java.lang.String.format;
import static org.neo4j.pki.DerUtils.beginDerSequence;
import static org.neo4j.pki.DerUtils.getDerContext;
import static org.neo4j.pki.DerUtils.readDerInteger;
import static org.neo4j.pki.DerUtils.readDerOctetString;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AlgorithmParameters;
import java.security.KeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.DSAPrivateKeySpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPrivateKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HexFormat;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Function;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Supported PEM format
 */
final class PemFormats {
    private static final KeyFactory RSA_KEY_FACTORY;
    private static final KeyFactory DSA_KEY_FACTORY;
    private static final KeyFactory EC_KEY_FACTORY;

    private static final Function<PKCS8EncodedKeySpec, PrivateKey> ALL_KEY_FACTORIES;

    static {
        try {
            // All exists as part of openJDK
            RSA_KEY_FACTORY = KeyFactory.getInstance("RSA");
            DSA_KEY_FACTORY = KeyFactory.getInstance("DSA");
            EC_KEY_FACTORY = KeyFactory.getInstance("EC");
            ALL_KEY_FACTORIES = keySpec -> {
                try {
                    return RSA_KEY_FACTORY.generatePrivate(keySpec);
                } catch (InvalidKeySpecException e) {
                    try {
                        return DSA_KEY_FACTORY.generatePrivate(keySpec);
                    } catch (InvalidKeySpecException ex) {
                        try {
                            return EC_KEY_FACTORY.generatePrivate(keySpec);
                        } catch (InvalidKeySpecException exc) {
                            // We tried...
                            e.addSuppressed(ex);
                            e.addSuppressed(exc);
                            throw new IllegalStateException("Key does not match RSA, DSA or EC spec.", e);
                        }
                    }
                }
            };
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Non-conforming JDK implementation.", e);
        }
    }

    private PemFormats() {}

    interface PemFormat {
        PrivateKey decodePrivate(byte[] der, Map<String, String> headers, String password) throws KeyException;

        PublicKey decodePublicKey(byte[] der) throws KeyException;
    }

    /**
     * PEM format at described by <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC7468</a> section 10.
     */
    static class Pkcs8 implements PemFormat {
        static final String PRIVATE_LABEL = "PRIVATE KEY";
        static final String PUBLIC_LABEL = "PUBLIC KEY";

        @Override
        public PrivateKey decodePrivate(byte[] der, Map<String, String> headers, String password) throws KeyException {
            assertNoPassword(password);
            return ALL_KEY_FACTORIES.apply(new PKCS8EncodedKeySpec(der));
        }

        @Override
        public PublicKey decodePublicKey(byte[] der) throws KeyException {
            KeySpec encodedKeySpec = new X509EncodedKeySpec(der);
            try {
                return RSA_KEY_FACTORY.generatePublic(encodedKeySpec);
            } catch (InvalidKeySpecException e) {
                try {
                    return DSA_KEY_FACTORY.generatePublic(encodedKeySpec);
                } catch (InvalidKeySpecException ex) {
                    try {
                        return EC_KEY_FACTORY.generatePublic(encodedKeySpec);
                    } catch (InvalidKeySpecException exc) {
                        // We tried...
                        e.addSuppressed(ex);
                        e.addSuppressed(exc);
                        throw new KeyException("Public key does not match RSA, DSA or EC spec.", e);
                    }
                }
            }
        }
    }

    /**
     * PEM format at described by <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC7468</a> section 11.
     */
    static class Pkcs8Encrypted extends Pkcs8 {
        static final String ENCRYPTED_LABEL = "ENCRYPTED PRIVATE KEY";

        @Override
        public PrivateKey decodePrivate(byte[] der, Map<String, String> headers, String password) throws KeyException {
            assertPassword(password);
            try {
                EncryptedPrivateKeyInfo keyInfo = new EncryptedPrivateKeyInfo(der);
                SecretKey pbeKey = getSecretKey(keyInfo, password);
                Cipher cipher = getCipher(keyInfo);
                cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.getAlgParameters());
                return ALL_KEY_FACTORIES.apply(keyInfo.getKeySpec(cipher));
            } catch (Exception e) {
                throw new KeyException("Unable to decrypt private key.", e);
            }
        }

        private static SecretKey getSecretKey(EncryptedPrivateKeyInfo keyInfo, String password)
                throws InvalidKeySpecException, NoSuchAlgorithmException {
            SecretKeyFactory keyFactory;
            try {
                // Try to find by algorithm name first
                keyFactory = SecretKeyFactory.getInstance(keyInfo.getAlgName());
            } catch (NoSuchAlgorithmException e) {
                // Maybe the algorithm parameter have a descent toString()?
                keyFactory =
                        SecretKeyFactory.getInstance(keyInfo.getAlgParameters().toString());
            }
            return keyFactory.generateSecret(new PBEKeySpec(password.toCharArray()));
        }

        private static Cipher getCipher(EncryptedPrivateKeyInfo keyInfo)
                throws NoSuchPaddingException, NoSuchAlgorithmException {
            try {
                return Cipher.getInstance(keyInfo.getAlgName());
            } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
                return Cipher.getInstance(keyInfo.getAlgParameters().toString());
            }
        }
    }

    /**
     * PEM Legacy described by <a href="https://datatracker.ietf.org/doc/html/rfc1421">RFC1421</a>.
     * Main difference is that the type of key is described in the label, and not as PKCS#8 as part
     * of the structure. Encryption is supported and is defined in the header.
     */
    abstract static class PemLegacy implements PemFormat {

        @Override
        public PrivateKey decodePrivate(byte[] der, Map<String, String> headers, String password) throws KeyException {
            // Handle decryption
            String procType = headers.get("Proc-Type");
            if (procType != null && procType.equals("4,ENCRYPTED")) {
                assertPassword(password);
                String deckInfo = headers.get("DEK-Info");
                if (deckInfo == null) {
                    throw new KeyException("Missing 'DEK-Info' in encrypted PRIVATE KEY.");
                }
                StringTokenizer tokenizer = new StringTokenizer(deckInfo, ",");
                String algorithm = tokenizer.nextToken();
                byte[] iv = HexFormat.of().parseHex(tokenizer.nextToken());
                der = decryptLegacyPem(der, algorithm, iv, password);
            } else {
                assertNoPassword(password);
            }

            // Here we have an un-encrypted DER that can be parsed by the corresponding algorithm
            ByteBuffer buffer = ByteBuffer.wrap(der);
            if (beginDerSequence(buffer) != buffer.remaining()) {
                throw new IllegalArgumentException("Malformed ASN.1 input.");
            }
            if (!version().equals(readDerInteger(buffer))) {
                throw new IllegalArgumentException("PrivateKey version mismatch.");
            }
            try {
                return decodePrivate0(buffer);
            } catch (InvalidKeySpecException e) {
                throw new KeyException(e);
            }
        }

        protected abstract PrivateKey decodePrivate0(ByteBuffer buffer) throws InvalidKeySpecException;

        protected abstract BigInteger version();
    }

    /**
     * Parser for PKCS1 encoded keys as described by
     * <a href="https://datatracker.ietf.org/doc/html/rfc3447">RFC3447</a>
     *
     * Unlike PKCS8, the type of the key is not encoded in the structure, so
     * we need to parse the ASN.1 structures directly.
     */
    static class PemPKCS1Rsa extends PemLegacy {
        static final String PRIVATE_LABEL = "RSA PRIVATE KEY";
        static final String PUBLIC_LABEL = "RSA PUBLIC KEY";

        /**
         * <pre>
         * RSAPublicKey ::= SEQUENCE {
         *     modulus           INTEGER,  -- n
         *     publicExponent    INTEGER   -- e
         * }
         * </pre>
         */
        @Override
        public PublicKey decodePublicKey(byte[] der) throws KeyException {
            ByteBuffer input = ByteBuffer.wrap(der);
            if (beginDerSequence(input) != input.remaining()) {
                throw new IllegalArgumentException("Malformed RSAPublicKey");
            }
            BigInteger n = readDerInteger(input); // INTEGER modulus
            BigInteger e = readDerInteger(input); // INTEGER publicExponent
            try {
                return RSA_KEY_FACTORY.generatePublic(new RSAPublicKeySpec(n, e));
            } catch (InvalidKeySpecException ex) {
                throw new KeyException(ex);
            }
        }

        /**
         * <pre>
         * RSAPrivateKey ::= SEQUENCE {
         *   version           Version,
         *   modulus           INTEGER,  -- n
         *   publicExponent    INTEGER,  -- e
         *   privateExponent   INTEGER,  -- d
         *   prime1            INTEGER,  -- p
         *   prime2            INTEGER,  -- q
         *   exponent1         INTEGER,  -- d mod (p-1)
         *   exponent2         INTEGER,  -- d mod (q-1)
         *   coefficient       INTEGER,  -- (inverse of q) mod p
         *   otherPrimeInfos   OtherPrimeInfos OPTIONAL
         * }
         * </pre>
         */
        @Override
        protected PrivateKey decodePrivate0(ByteBuffer buffer) throws InvalidKeySpecException {
            BigInteger n = readDerInteger(buffer); // INTEGER modulus
            BigInteger e = readDerInteger(buffer); // INTEGER publicExponent
            BigInteger d = readDerInteger(buffer); // INTEGER privateExponent
            BigInteger p = readDerInteger(buffer); // INTEGER prime1
            BigInteger q = readDerInteger(buffer); // INTEGER prime2
            BigInteger ep = readDerInteger(buffer); // INTEGER exponent1
            BigInteger eq = readDerInteger(buffer); // INTEGER exponent2
            BigInteger c = readDerInteger(buffer); // INTEGER coefficient
            return RSA_KEY_FACTORY.generatePrivate(new RSAPrivateCrtKeySpec(n, e, d, p, q, ep, eq, c));
        }

        @Override
        protected BigInteger version() {
            return BigInteger.ZERO;
        }
    }

    /**
     * Parser for DSA keys, no available standard but exists as a de facto standard as implemented by openSSL
     */
    static class PemPKCS1Dsa extends PemLegacy {
        static final String PRIVATE_LABEL = "DSA PRIVATE KEY";

        @Override
        public PublicKey decodePublicKey(byte[] der) {
            throw new UnsupportedOperationException();
        }

        /**
         * <pre>
         * DSSPrivatKey_OpenSSL ::= SEQUENCE
         *     version INTEGER,
         *     p INTEGER,
         *     q INTEGER,
         *     g INTEGER,
         *     y INTEGER,
         *     x INTEGER
         * }
         * </pre>
         */
        @Override
        protected PrivateKey decodePrivate0(ByteBuffer buffer) throws InvalidKeySpecException {
            BigInteger p = readDerInteger(buffer); // INTEGER p
            BigInteger q = readDerInteger(buffer); // INTEGER q
            BigInteger g = readDerInteger(buffer); // INTEGER g
            readDerInteger(buffer); // public key 'y' is not used in the private key
            BigInteger x = readDerInteger(buffer); // INTEGER x
            return DSA_KEY_FACTORY.generatePrivate(new DSAPrivateKeySpec(x, p, q, g));
        }

        @Override
        protected BigInteger version() {
            return BigInteger.ZERO;
        }
    }

    /**
     * Parser of Elliptic Curve keys as described by <a href="https://datatracker.ietf.org/doc/html/rfc5915">RFC5915</a>
     */
    static class PemPKCS1Ec extends PemLegacy {
        static final String PRIVATE_LABEL = "EC PRIVATE KEY";

        @Override
        public PublicKey decodePublicKey(byte[] der) {
            throw new UnsupportedOperationException();
        }

        /**
         * <pre>
         * ECPrivateKey ::= SEQUENCE {
         *   version        INTEGER { ecPrivkeyVer1(1) } (ecPrivkeyVer1),
         *   privateKey     OCTET STRING,
         *   parameters [0] ECParameters {{ NamedCurve }} OPTIONAL,
         *   publicKey  [1] BIT STRING OPTIONAL
         * }
         * </pre>
         */
        @Override
        protected PrivateKey decodePrivate0(ByteBuffer buffer) throws InvalidKeySpecException {
            try {
                BigInteger s = new BigInteger(1, readDerOctetString(buffer));
                byte[] parameters = getDerContext(buffer, (byte) 0);
                AlgorithmParameters ecParams = AlgorithmParameters.getInstance("EC");
                ecParams.init(parameters);
                ECParameterSpec parameterSpec = ecParams.getParameterSpec(ECParameterSpec.class);
                return EC_KEY_FACTORY.generatePrivate(new ECPrivateKeySpec(s, parameterSpec));
            } catch (NoSuchAlgorithmException | IOException | InvalidParameterSpecException e) {
                throw new IllegalArgumentException("Failed to decode EC private key", e);
            }
        }

        @Override
        protected BigInteger version() {
            return BigInteger.ONE;
        }
    }

    private static void assertNoPassword(String password) throws KeyException {
        if (password != null) {
            throw new KeyException("Passphrase was provided but found un-encrypted private key.");
        }
    }

    private static void assertPassword(String password) throws KeyException {
        if (password == null) {
            throw new KeyException("Found encrypted private key but no passphrase was provided.");
        }
    }

    /**
     * Supported encryption schemas for legacy PEM.
     */
    private enum DecryptSchema {
        DES_CBC(8, "DES", "DES/CBC/PKCS5Padding"),
        DES_EDE3_CBC(24, "DESede", "DESede/CBC/PKCS5Padding"),
        AES_128_CBC(16, "AES", "AES/CBC/PKCS5Padding"),
        AES_192_CBC(24, "AES", "AES/CBC/PKCS5Padding"),
        AES_256_CBC(32, "AES", "AES/CBC/PKCS5Padding");

        final int keySize;
        final String family;
        final String cipher;

        DecryptSchema(int keySize, String family, String cipher) {
            this.keySize = keySize;
            this.family = family;
            this.cipher = cipher;
        }
    }

    private static byte[] decryptLegacyPem(byte[] der, String algorithm, byte[] iv, String password)
            throws KeyException {
        try {
            DecryptSchema decryptSchema;
            try {
                decryptSchema = DecryptSchema.valueOf(algorithm.replace("-", "_"));
            } catch (IllegalArgumentException e) {
                throw new KeyException(format("Encryption scheme %s is not supported.", algorithm));
            }

            // Take as many bytes from the digest as needed
            byte[] kdf = keyDerivationFunction(iv, password);
            byte[] key = new byte[decryptSchema.keySize];
            System.arraycopy(kdf, 0, key, 0, decryptSchema.keySize);
            SecretKeySpec secret = new SecretKeySpec(key, decryptSchema.family);

            // Decrypt
            Cipher cipher = Cipher.getInstance(decryptSchema.cipher);
            cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(iv));
            return cipher.doFinal(der);
        } catch (Exception e) {
            throw new KeyException("Failed to decrypt PEM file.", e);
        }
    }

    /**
     * OpenSSL de facto standard for generating keys for symmetric cyphers for PEM file encryption.
     * It's basically PBKDF1 as described in PKCS#5 v1.5 with MD5 hash and iteration count of 1.
     *
     * @param iv initialization vector.
     * @param password user password.
     * @return an array of 32 bytes that should be used as keys for symmetric cyphers.
     * @throws NoSuchAlgorithmException if MD5 message digest is not available.
     */
    private static byte[] keyDerivationFunction(byte[] iv, String password) throws NoSuchAlgorithmException {
        // https://github.com/openssl/openssl/blob/e4fd3fc379d76d9cd33ea6699268485606447737/crypto/pem/pem_lib.c#L378
        // https://github.com/openssl/openssl/blob/e4fd3fc379d76d9cd33ea6699268485606447737/crypto/evp/evp_key.c#L78
        byte[] pw = password.getBytes(StandardCharsets.UTF_8);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(pw);
        md5.update(iv, 0, 8);
        byte[] d0 = md5.digest();
        md5.update(d0);
        md5.update(pw);
        md5.update(iv, 0, 8);
        byte[] d1 = md5.digest();
        byte[] kdf = new byte[32];
        System.arraycopy(d0, 0, kdf, 0, 16);
        System.arraycopy(d1, 0, kdf, 16, 16);
        return kdf;
    }
}
