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
package org.neo4j.ssl;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.security.PrivateKey;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
class PkiUtilsTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldCreateASelfSignedCertificate() throws Exception
    {
        // Given
        var sslFactory = new SelfSignedCertificateFactory();
        var cPath = new File( testDirectory.homeDir(), "certificate" );
        var pkPath = new File( testDirectory.homeDir(), "key" );

        // When
        sslFactory.createSelfSignedCertificate( cPath, pkPath, "myhost" );

        // Then
        // Attempt to load certificate
        var certificates = PkiUtils.loadCertificates( cPath );
        assertThat( certificates.length ).isGreaterThan( 0 );

        // Attempt to load private key
        PrivateKey pk = PkiUtils.loadPrivateKey( pkPath, null );
        assertThat( pk ).isNotNull();
    }

    @Test
    void shouldLoadPEMCertificates() throws Throwable
    {
        // Given
        var cert = new SelfSignedCertificate( "example.com" );

        var pemCertificate = cert.certificate();

        // When
        var certificates = PkiUtils.loadCertificates( pemCertificate );

        // Then
        assertThat( certificates.length ).isEqualTo( 1 );
    }

    @Test
    void shouldLoadPEMPrivateKey() throws Throwable
    {
        // Given
        var cert = new SelfSignedCertificate( "example.com" );

        var privateKey = cert.privateKey();

        // When
        var pk = PkiUtils.loadPrivateKey( privateKey, null );

        // Then
        assertNotNull( pk );
    }

    @Test
    void shouldReadEncryptedPrivateKey() throws Exception
    {
        File keyFile = testDirectory.file( "private.key" );
        URL resource = this.getClass().getResource( "test-certificates/encrypted/private.key" );
        copy( resource, keyFile );

        PrivateKey pk = PkiUtils.loadPrivateKey( keyFile, "neo4j" );
        assertThat( pk.getAlgorithm() ).isEqualTo( "RSA" );
    }

    @Test
    void shouldThrowOnMissingPassphraseForEncryptedPrivateKey() throws Exception
    {
        File keyFile = testDirectory.file( "private.key" );
        URL resource = this.getClass().getResource( "test-certificates/encrypted/private.key" );
        copy( resource, keyFile );

        assertThrows( IOException.class, () -> PkiUtils.loadPrivateKey( keyFile, null ) );
    }

    private void copy( URL in, File outFile ) throws IOException
    {
        try ( InputStream is = in.openStream();
                OutputStream os = testDirectory.getFileSystem().openAsOutputStream( outFile, false ) )
        {
            while ( is.available() > 0 )
            {
                byte[] buf = new byte[8192];
                int nBytes = is.read( buf );
                os.write( buf, 0, nBytes );
            }
        }
    }
}
