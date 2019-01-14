/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v1.transport.socket.client;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.function.Consumer;
import javax.net.ssl.X509TrustManager;

/** Trust self-signed certificates */
public class NaiveTrustManager implements X509TrustManager
{
    private final Consumer<X509Certificate> certSink;

    public NaiveTrustManager( Consumer<X509Certificate> certSink )
    {
        this.certSink = certSink;
    }

    @Override
    public void checkClientTrusted( X509Certificate[] x509Certificates, String s )
    {
        for ( X509Certificate x509Certificate : x509Certificates )
        {
            certSink.accept( x509Certificate );
        }
    }

    @Override
    public void checkServerTrusted( X509Certificate[] x509Certificates, String s )
    {
        for ( X509Certificate x509Certificate : x509Certificates )
        {
            certSink.accept( x509Certificate );
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
        return null;
    }
}
