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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivateKey;
import java.security.cert.Certificate;

import org.neo4j.io.fs.FileUtils;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TestSslCertificateFactory
{
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void shouldCreateASelfSignedCertificate() throws Exception
    {
        // Given
        PkiUtils sslFactory = new PkiUtils();
        File cPath = new File( tmpDir.getRoot(), "certificate" );
        File pkPath = new File( tmpDir.getRoot(), "key" );

        // When
        sslFactory.createSelfSignedCertificate( cPath, pkPath, "myhost" );

        // Then
        // Attempt to load certificate
        Certificate[] certificates = sslFactory.loadCertificates( cPath );
        assertThat( certificates.length, is( greaterThan( 0 ) ) );

        // Attempt to load private key
        PrivateKey pk = sslFactory.loadPrivateKey( pkPath );
        assertThat( pk, notNullValue() );
    }

    @Test
    public void shouldLoadPEMCertificates() throws Throwable
    {
        // Given
        SelfSignedCertificate cert = new SelfSignedCertificate( "example.com" );
        PkiUtils certs = new PkiUtils();

        File pemCertificate = cert.certificate();

        // When
        Certificate[] certificates = certs.loadCertificates( pemCertificate );

        // Then
        assertThat(certificates.length, equalTo(1));
    }

    @Test
    public void shouldLoadPEMPrivateKey() throws Throwable
    {
        // Given
        SelfSignedCertificate cert = new SelfSignedCertificate( "example.com" );
        PkiUtils certs = new PkiUtils();

        File privateKey = cert.privateKey();

        // When
        PrivateKey pk = certs.loadPrivateKey( privateKey );

        // Then
        assertNotNull( pk );
    }

    /**
     * For backwards-compatibility reasons, we support both PEM-encoded certificates *and* raw binary files containing
     * the certificate data.
     */
    @Test
    public void shouldLoadBinaryCertificates() throws Throwable
    {
        // Given
        SelfSignedCertificate cert = new SelfSignedCertificate( "example.com" );
        PkiUtils certs = new PkiUtils();

        File cPath = tmpDir.newFile( "certificate" );
        byte[] raw = certs.loadCertificates(cert.certificate())[0].getEncoded();

        try ( FileChannel ch = FileChannel.open( cPath.toPath(), WRITE ) )
        {
            FileUtils.writeAll( ch, ByteBuffer.wrap( raw ) );
        }

        // When
        Certificate[] certificates = certs.loadCertificates( cPath );

        // Then
        assertThat( certificates.length, equalTo( 1 ) );
    }

    /**
     * For backwards-compatibility reasons, we support both PEM-encoded private keys *and* raw binary files containing
     * the private key data
     */
    @Test
    public void shouldLoadBinaryPrivateKey() throws Throwable
    {
        // Given
        SelfSignedCertificate cert = new SelfSignedCertificate( "example.com" );
        PkiUtils certs = new PkiUtils();

        File keyFile = tmpDir.newFile( "certificate" );
        byte[] raw = certs.loadPrivateKey( cert.privateKey() ).getEncoded();

        try ( FileChannel ch = FileChannel.open( keyFile.toPath(), WRITE ) )
        {
            FileUtils.writeAll( ch, ByteBuffer.wrap( raw ) );
        }

        // When
        PrivateKey pk = certs.loadPrivateKey( keyFile );

        // Then
        assertNotNull( pk );
    }
}
