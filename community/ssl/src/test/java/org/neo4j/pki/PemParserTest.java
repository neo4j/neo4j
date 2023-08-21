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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InputStream;
import java.security.KeyException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.DSAPrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PemParserTest {

    @Nested
    class EncryptionSupportValidation {
        private static final String BASE = "pem/encrypted/";

        @Test
        void publicPKCS8() throws Exception {
            verifyPublicKey(BASE + "public.pem", PublicKey.class);
        }

        @Test
        void unencryptedPKCS8() throws Exception {
            verifyPrivateKey(BASE + "pkcs8.key", null, RSAPrivateKey.class);
        }

        @Test
        void throwOnBadKey() {
            KeyException exception = assertThrows(
                    KeyException.class,
                    () -> verifyPrivateKey(BASE + "PBEWithHmacSHA512AndAES_256.key", "invalid", RSAPrivateKey.class));
            assertThat(exception).hasMessage("Unable to decrypt private key.");
        }

        @Test
        void throwOnMissingPassphrase() {
            KeyException exception = assertThrows(
                    KeyException.class,
                    () -> verifyPrivateKey(BASE + "PBEWithHmacSHA512AndAES_256.key", null, RSAPrivateKey.class));
            assertThat(exception).hasMessage("Found encrypted private key but no passphrase was provided.");
        }

        @Test
        void throwOnPassphraseProvided() {
            KeyException exception = assertThrows(
                    KeyException.class, () -> verifyPrivateKey(BASE + "pkcs8.key", "pass", RSAPrivateKey.class));
            assertThat(exception).hasMessage("Passphrase was provided but found un-encrypted private key.");
        }

        @Test
        void supportedAlgorithms() throws Exception {
            // PBKDF1 must be MD5 or SHA1 (technically MD2 as well, but no one should be using that)
            // PBES1 must be DES or RC2 in CBC mode
            verifyPrivateKey(BASE + "PBEWithMD5AndDES.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithSHA1AndRC2_40.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithSHA1AndRC2_128.key", "neo4j", RSAPrivateKey.class);
            // PKCS#12 extended with DESede(TripleDES) and RC4
            verifyPrivateKey(BASE + "PBEWithSHA1AndDESede.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithSHA1AndRC4_40.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithSHA1AndRC4_128.key", "neo4j", RSAPrivateKey.class);

            // PBKDF2 can be any generator, typically only HMAC with SHA-1, SHA-224, SHA-256, SHA-384, SHA-512
            // PBES2 can be any encryption scheme, Java only supports AES though
            verifyPrivateKey(BASE + "PBEWithHmacSHA1AndAES_128.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA224AndAES_128.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA256AndAES_128.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA384AndAES_128.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA512AndAES_128.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA1AndAES_256.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA224AndAES_256.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA256AndAES_256.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA384AndAES_256.key", "neo4j", RSAPrivateKey.class);
            verifyPrivateKey(BASE + "PBEWithHmacSHA512AndAES_256.key", "neo4j", RSAPrivateKey.class);
        }

        @Test
        void unsupportedAlgorithms() throws Exception {
            // The following algorithms are not supported by Java out of the box
            // All are considered weak/deprecated and should be avoided
            // They are here to keep track of support across different JDKs
            invalidPrivateKey(BASE + "PBEWithSHA1And2DES.key", "neo4j");
            invalidPrivateKey(BASE + "PBEWithSHA1AndRC2_64.key", "neo4j");
            invalidPrivateKey(BASE + "PBEWithMD5AndRC2_64.key", "neo4j");
            invalidPrivateKey(BASE + "PBEWithSHA1AndDES.key", "neo4j");
            invalidPrivateKey(BASE + "PBEWithHmacSHA1AndDESede.key", "neo4j");
        }
    }

    @Nested
    class Pkcs8 {
        private static final String BASE = "pem/pkcs8/";

        @Test
        void rsa() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs8.key", null, RSAPrivateKey.class);
        }

        @Test
        void dsa() throws Exception {
            verifyPrivateKey(BASE + "dsa.pkcs8.key", null, DSAPrivateKey.class);
        }

        @Test
        void ec() throws Exception {
            verifyPrivateKey(BASE + "ec.pkcs8.key", null, ECPrivateKey.class);
        }
    }

    @Nested
    class LegacyPem {
        private static final String BASE = "pem/legacy/";

        @Test
        void rsaPublic() throws Exception {
            verifyPublicKey(BASE + "rsa.pkcs1.public.pem", RSAPublicKey.class);
        }

        @Test
        void rsa() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.key", null, RSAPrivateKey.class);
        }

        @Test
        void dsa() throws Exception {
            verifyPrivateKey(BASE + "dsa.pkcs1.key", null, DSAPrivateKey.class);
        }

        @Test
        void ec() throws Exception {
            verifyPrivateKey(BASE + "ec.pkcs1.key", null, ECPrivateKey.class);
        }
    }

    @Nested
    class LegacyPemEncryption {
        private static final String BASE = "pem/legacy/";

        @Test
        void aes128() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.aes128.key", "neo4j", RSAPrivateKey.class);
        }

        @Test
        void aes192() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.aes192.key", "neo4j", RSAPrivateKey.class);
        }

        @Test
        void aes256() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.aes256.key", "neo4j", RSAPrivateKey.class);
        }

        @Test
        void des() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.des.key", "neo4j", RSAPrivateKey.class);
        }

        @Test
        void des3() throws Exception {
            verifyPrivateKey(BASE + "rsa.pkcs1.des3.key", "neo4j", RSAPrivateKey.class);
        }
    }

    static void verifyPrivateKey(String res, String password, Class<? extends PrivateKey> expectedType)
            throws Exception {
        PemParser parser = new PemParser(res(res));
        assertThat(parser.getPrivateKey(password)).isInstanceOf(expectedType);
    }

    static void invalidPrivateKey(String res, String password) throws Exception {
        PemParser parser = new PemParser(res(res));
        assertThrows(KeyException.class, () -> parser.getPrivateKey(password));
    }

    static void verifyPublicKey(String res, Class<? extends PublicKey> expectedType) throws Exception {
        PemParser parser = new PemParser(res(res));
        assertThat(parser.getPublicKey()).isInstanceOf(expectedType);
    }

    static InputStream res(String path) {
        return PemParserTest.class.getResourceAsStream(path);
    }
}
