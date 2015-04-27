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
package org.neo4j.server.security.ssl;

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.Certificate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.security.ssl.SslCertificateFactory;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TestSslCertificateFactory
{
    private File cPath;
    private File pkPath;

    @Test
    public void shouldCreateASelfSignedCertificate() throws Exception
    {
        SslCertificateFactory sslFactory = new SslCertificateFactory();
        sslFactory.createSelfSignedCertificate( cPath, pkPath, "myhost" );

        // Attempt to load certificate
        Certificate[] certificates = sslFactory.loadCertificates( cPath );
        assertThat( certificates.length, is( greaterThan( 0 ) ) );

        // Attempt to load private key
        PrivateKey pk = sslFactory.loadPrivateKey( pkPath );
        assertThat( pk, notNullValue() );
    }
    
    @Before
    public void createFiles() throws Exception
    {
        cPath = File.createTempFile( "cert", "test" );
        pkPath = File.createTempFile( "privatekey", "test" );
    }

    @After
    public void deleteFiles() throws Exception
    {
        pkPath.delete();
        cPath.delete();
    }
}
