/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.client;

import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.OCSPException;
import org.bouncycastle.cert.ocsp.OCSPResp;

import java.io.IOException;
import java.net.Socket;
import java.security.KeyStore;
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

public class CertConfiguredSecureSocketConnection extends SecureSocketConnection
{
    private final X509Certificate rootCert;

    public CertConfiguredSecureSocketConnection( X509Certificate trustedRootCertificate )
    {
        this.rootCert = trustedRootCertificate;
        setSocket( createTrustedCertSocket() );
    }

    private Socket createTrustedCertSocket()
    {
        try
        {
            SSLContext context = SSLContext.getInstance( "TLS" );

            KeyStore ks = KeyStore.getInstance( KeyStore.getDefaultType() );
            ks.load( null, "".toCharArray() );
            ks.setCertificateEntry( "rootCert", rootCert );

            KeyManagerFactory kmf = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
            kmf.init( ks, new char[]{} );

            TrustManagerFactory tmf = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
            tmf.init( ks );

            context.init( kmf.getKeyManagers(), tmf.getTrustManagers(), null );
            return context.getSocketFactory().createSocket();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to create security context", e );
        }
    }

    @Override
    public Set<X509Certificate> getServerCertificatesSeen()
    {
        try
        {
            return Arrays.stream( ((SSLSocket) getSocket()).getSession().getPeerCertificates() )
                         .map( cert -> (X509Certificate) cert )
                         .collect( Collectors.toSet() );
        }
        catch ( SSLPeerUnverifiedException e )
        {
            throw new RuntimeException( "Failed retrieving client-seen certificates", e );
        }
    }

    public Set<BasicOCSPResp> getSeenOcspResponses() throws IOException, OCSPException
    {
        Set<BasicOCSPResp> ocspResponses = new HashSet<>();

        List<byte[]> binaryStatusResponses = ((ExtendedSSLSession) ((SSLSocket) getSocket()).getSession()).getStatusResponses();

        for ( byte[] bResp : binaryStatusResponses )
        {
            if ( bResp.length > 0 )
            {
                OCSPResp ocspResp = new OCSPResp( bResp );
                ocspResponses.add( (BasicOCSPResp) ocspResp.getResponseObject() );
            }
        }

        return ocspResponses;
    }
}
