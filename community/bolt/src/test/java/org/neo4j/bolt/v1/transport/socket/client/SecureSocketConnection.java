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

import java.net.Socket;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

public class SecureSocketConnection extends SocketConnection
{
    private final Set<X509Certificate> serverCertificatesSeen = new HashSet<>();

    public SecureSocketConnection()
    {
        setSocket( createSecureSocket() );
    }

    private Socket createSecureSocket()
    {
        try
        {
            SSLContext context = SSLContext.getInstance( "TLS" );
            context.init( new KeyManager[0], new TrustManager[]{new NaiveTrustManager( serverCertificatesSeen::add )}, new SecureRandom() );

            return context.getSocketFactory().createSocket();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public Set<X509Certificate> getServerCertificatesSeen()
    {
        return serverCertificatesSeen;
    }

}
