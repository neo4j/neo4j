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
import java.net.Socket;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;

public class SecureSocketConnection extends SocketConnection {
    private static final Factory factory = new Factory();
    private final Set<X509Certificate> serverCertificatesSeen = new HashSet<>();

    public static TransportConnection.Factory factory() {
        return factory;
    }

    public SecureSocketConnection(SocketAddress address) {
        super(address);
    }

    @Override
    protected Socket createSocket() {
        try {
            var context = this.createSslContext();
            return context.getSocketFactory().createSocket();
        } catch (KeyManagementException | NoSuchAlgorithmException ex) {
            throw new UnsupportedOperationException("Failed to configure SSL context", ex);
        } catch (KeyStoreException | UnrecoverableKeyException | CertificateException ex) {
            throw new IllegalStateException("Failed to configure SSL context", ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create socket", ex);
        }
    }

    protected SSLContext createSslContext()
            throws IOException, NoSuchAlgorithmException, KeyManagementException, KeyStoreException,
                    UnrecoverableKeyException, CertificateException {
        var context = SSLContext.getInstance("TLS");
        context.init(
                new KeyManager[0],
                new TrustManager[] {new NaiveTrustManager(serverCertificatesSeen::add)},
                new SecureRandom());
        return context;
    }

    public Set<X509Certificate> getServerCertificatesSeen() {
        return serverCertificatesSeen;
    }

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(SocketAddress address) {
            return new SecureSocketConnection(address);
        }

        @Override
        public String toString() {
            return "TLS Socket";
        }
    }
}
