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
package org.neo4j.bolt.testing.client;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.OCSPException;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.neo4j.bolt.testing.client.error.BoltTestClientClosedException;

public final class CertConfiguredSecureSocketConnection extends SecureSocketConnection {

    private final X509Certificate rootCert;

    public CertConfiguredSecureSocketConnection(InetSocketAddress address, X509Certificate trustedRootCertificate) {
        super(address);
        this.rootCert = trustedRootCertificate;
    }

    @Override
    protected SslContext sslContext() throws SSLException {
        try {
            var ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, "".toCharArray());
            ks.setCertificateEntry("rootCert", rootCert);

            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, new char[] {});

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            // TODO: This does not support retrieval of seen certificates at the moment
            return SslContextBuilder.forClient()
                    .keyManager(kmf)
                    .trustManager(tmf)
                    .build();
        } catch (KeyStoreException
                | IOException
                | NoSuchAlgorithmException
                | CertificateException
                | UnrecoverableKeyException ex) {
            throw new BoltTestClientClosedException("Failed to initialize SslContext", ex);
        }
    }

    public Set<BasicOCSPResp> getSeenOcspResponses() throws IOException, OCSPException {
        var sslEngine = this.sslEngine;
        if (sslEngine == null) {
            return Set.of();
        }

        Set<BasicOCSPResp> ocspResponses = new HashSet<>();
        var binaryStatusResponses = ((ExtendedSSLSession) sslEngine.getSession()).getStatusResponses();
        for (byte[] bResp : binaryStatusResponses) {
            if (bResp.length > 0) {
                var ocspResp = new OCSPResp(bResp);
                ocspResponses.add((BasicOCSPResp) ocspResp.getResponseObject());
            }
        }

        return ocspResponses;
    }
}
