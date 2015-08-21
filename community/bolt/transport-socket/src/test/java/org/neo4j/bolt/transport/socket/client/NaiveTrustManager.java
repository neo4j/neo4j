/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.transport.socket.client;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/** Trust self-signed certificates */
public class NaiveTrustManager implements X509TrustManager
{
    @Override
    public void checkClientTrusted( X509Certificate[] x509Certificates, String s ) throws CertificateException
    {

    }

    @Override
    public void checkServerTrusted( X509Certificate[] x509Certificates, String s ) throws CertificateException
    {

    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
        return null;
    }
}
