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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;

public final class PkiUtils {
    public static final CertificateFactory CERTIFICATE_FACTORY;

    static {
        try {
            CERTIFICATE_FACTORY = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    private PkiUtils() {}

    @SuppressWarnings("unchecked")
    public static X509Certificate[] loadCertificates(FileSystemAbstraction fs, Path certFile)
            throws CertificateException, IOException {
        Collection<X509Certificate> c =
                (Collection<X509Certificate>) CERTIFICATE_FACTORY.generateCertificates(pathToByteStream(fs, certFile));
        return c.toArray(new X509Certificate[0]);
    }

    public static PrivateKey loadPrivateKey(FileSystemAbstraction fs, Path keyFile) throws IOException, KeyException {
        return loadPrivateKey(fs, keyFile, null);
    }

    public static PrivateKey loadPrivateKey(FileSystemAbstraction fs, Path keyFile, String password)
            throws IOException, KeyException {
        return new PemParser(pathToByteStream(fs, keyFile)).getPrivateKey(password);
    }

    public static PrivateKey loadPrivateKey(InputStream inputStream, String password) throws IOException, KeyException {
        return new PemParser(inputStream).getPrivateKey(password);
    }

    public static PublicKey loadPublicKey(FileSystemAbstraction fs, Path publicKey) throws IOException, KeyException {
        return new PemParser(pathToByteStream(fs, publicKey)).getPublicKey();
    }

    private static InputStream pathToByteStream(FileSystemAbstraction fs, Path path) throws IOException {
        return new ByteArrayInputStream(FileSystemUtils.readAllBytes(fs, path, EmptyMemoryTracker.INSTANCE));
    }
}
