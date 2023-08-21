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
package org.neo4j.bolt.testing.client.tls;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.OCSPException;
import org.bouncycastle.cert.ocsp.OCSPResp;

public class CertConfiguredSecureSocketConnection extends SecureSocketConnection {
    private final X509Certificate rootCert;

    public CertConfiguredSecureSocketConnection(SocketAddress address, X509Certificate trustedRootCertificate) {
        super(address);
        this.rootCert = trustedRootCertificate;
    }

    @Override
    protected SSLContext createSslContext()
            throws IOException, NoSuchAlgorithmException, KeyManagementException, KeyStoreException,
                    UnrecoverableKeyException, CertificateException {
        var context = SSLContext.getInstance("TLS");

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, "".toCharArray());
        ks.setCertificateEntry("rootCert", rootCert);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[] {});

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context;
    }

    @Override
    public Set<X509Certificate> getServerCertificatesSeen() {
        try {
            return Arrays.stream(((SSLSocket) this.socket).getSession().getPeerCertificates())
                    .map(cert -> (X509Certificate) cert)
                    .collect(Collectors.toSet());
        } catch (SSLPeerUnverifiedException e) {
            throw new RuntimeException("Failed retrieving client-seen certificates", e);
        }
    }

    public Set<BasicOCSPResp> getSeenOcspResponses() throws IOException, OCSPException {
        Set<BasicOCSPResp> ocspResponses = new HashSet<>();

        List<byte[]> binaryStatusResponses =
                ((ExtendedSSLSession) ((SSLSocket) this.socket).getSession()).getStatusResponses();

        for (byte[] bResp : binaryStatusResponses) {
            if (bResp.length > 0) {
                OCSPResp ocspResp = new OCSPResp(bResp);
                ocspResponses.add((BasicOCSPResp) ocspResp.getResponseObject());
            }
        }

        return ocspResponses;
    }
}
