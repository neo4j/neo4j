/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.security;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.Certificate;

import org.junit.Test;

public class TestSslCertificateFactory {

    @Test
    public void shouldCreateASelfSignedCertificate() throws Exception {
        File cPath = File.createTempFile("cert", "test");
        File pkPath = File.createTempFile("privatekey", "test");
        
        SslCertificateFactory sslFactory = new SslCertificateFactory();
        sslFactory.createSelfSignedCertificate(cPath, pkPath, "myhost");
        
        // Attempt to load certificate
        Certificate c = sslFactory.loadCertificate(cPath);
        assertThat(c, notNullValue());
        
        // Attempt to load private key
        PrivateKey pk = sslFactory.loadPrivateKey(pkPath);
        assertThat(pk, notNullValue());
    }
    
}
