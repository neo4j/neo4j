/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SslSystemInternalSettings;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.newBuilder;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

@TestDirectoryExtension
class SslPolicyLoaderTest
{
    private static final String REVOCATION_ERROR_MSG = "Could not load CRL";
    private static final String TRUSTED_CERTS_ERROR_MSG = "Failed to create trust manager";

    @Inject
    private TestDirectory testDirectory;

    private File home;
    private File publicCertificateFile;
    private File privateKeyFile;
    private File trustedDir;
    private File revokedDir;
    private File baseDir;

    @BeforeEach
    void setup() throws Exception
    {
        home = testDirectory.directory( "home" );
        baseDir = new File( home, "certificates/default" );
        publicCertificateFile = new File( baseDir, "public.crt" );
        privateKeyFile = new File( baseDir, "private.key" );

        new SelfSignedCertificateFactory().createSelfSignedCertificate( publicCertificateFile, privateKeyFile, "localhost" );

        trustedDir = makeDir( baseDir, "trusted" );
        FileUtils.copyFile( publicCertificateFile, new File( trustedDir, "public.crt" ) );

        revokedDir = makeDir( baseDir, "revoked" );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldLoadBaseCryptographicObjects( boolean ignoreDotfiles ) throws Exception
    {
        // when
        SslPolicyLoader sslPolicyLoader = createSslPolicyLoader( ignoreDotfiles );

        // then
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( TESTING );
        assertPolicyValid( sslPolicy );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldComplainIfUnparseableFilesPresentInTrusted( boolean ignoreDotfiles ) throws IOException
    {
        // given
        writeJunkToFile( trustedDir, "foo.txt" );
        assertTrue( (new File( revokedDir, "empty.crt" )).createNewFile() );

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy( TRUSTED_CERTS_ERROR_MSG, CertificateException.class, ignoreDotfiles );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldComplainIfDirectoriesPresentInTrusted( boolean ignoreDotfiles ) throws IOException
    {
        // given
        makeDir( trustedDir, "foo" );

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy( TRUSTED_CERTS_ERROR_MSG, Exception.class, ignoreDotfiles );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldComplainIfUnparseableFilesPresentInRevoked( boolean ignoreDotfiles ) throws IOException
    {
        // given
        writeJunkToFile( revokedDir, "foo.txt" );

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy( REVOCATION_ERROR_MSG, CRLException.class, ignoreDotfiles );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldComplainIfDirectoriesPresentInRevoked( boolean ignoreDotfiles ) throws IOException
    {
        // given
        makeDir( revokedDir, "foo" );

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy( REVOCATION_ERROR_MSG, Exception.class, ignoreDotfiles );
    }

    private void shouldThrowCertificateExceptionCreatingSslPolicy( String expectedMessage, Class<? extends Exception> expectedCause, boolean ignoreDotfiles )
    {
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toPath().toAbsolutePath() )
                .set( SslSystemInternalSettings.ignore_dotfiles, ignoreDotfiles )
                .set( policyConfig.enabled, Boolean.TRUE )
                .set( policyConfig.base_directory, Path.of( "certificates/default" ) )
                .build();

        // when
        Exception exception = assertThrows( Exception.class, () -> SslPolicyLoader.create( config, NullLogProvider.getInstance() ) );
        assertThat( exception.getMessage() ).contains( expectedMessage );
        assertThat( exception.getCause() ).isInstanceOf( expectedCause );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void correctBehaviourIfDotfilesPresent( boolean ignoreDotfiles ) throws IOException
    {
        // given
        writeJunkToFile( baseDir, ".README" );
        writeJunkToFile( trustedDir, ".README" );
        writeJunkToFile( revokedDir, ".README" );

        // when
        SslPolicyLoader sslPolicyLoader;
        if ( !ignoreDotfiles )
        {
            assertThrows( Exception.class, () -> createSslPolicyLoader( ignoreDotfiles ) );
            return;
        }
        else
        {
            sslPolicyLoader = createSslPolicyLoader( ignoreDotfiles );
        }

        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( TESTING );

        // then
        assertPolicyValid( sslPolicy );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldNotComplainIfDotdirsPresent( boolean ignoreDotfiles ) throws IOException
    {
        // given
        makeDir( baseDir, "..data" );
        makeDir( trustedDir, "..data" );
        makeDir( revokedDir, "..data" );

        // when
        SslPolicyLoader sslPolicyLoader;
        if ( !ignoreDotfiles )
        {
            Exception exception = assertThrows( Exception.class, () -> createSslPolicyLoader( ignoreDotfiles ) );
            assertThat( exception.getMessage() ).contains( "Failed to create trust manager" );
            return;
        }
        else
        {
            sslPolicyLoader = createSslPolicyLoader( ignoreDotfiles );
        }

        //then
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( TESTING );
        assertPolicyValid( sslPolicy );
    }

    @Test
    void shouldComplainIfMissingPrivateKey()
    {
        shouldComplainIfMissingFile( privateKeyFile, "Failed to load private key" );
    }

    @Test
    void shouldComplainIfMissingPublicCertificate()
    {
        shouldComplainIfMissingFile( publicCertificateFile, "Failed to load public certificate chain" );
    }

    private void shouldComplainIfMissingFile( File file, String expectedErrorMessage )
    {
        // given
        FileUtils.deleteFile( file );

        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toPath().toAbsolutePath() )
                .set( policyConfig.enabled, Boolean.TRUE )
                .set( policyConfig.base_directory, Path.of( "certificates/default" ) )
                .build();

        // when
        Exception exception = assertThrows( Exception.class, () -> SslPolicyLoader.create( config, NullLogProvider.getInstance() ) );
        assertThat( exception.getMessage() ).contains( expectedErrorMessage );
        assertThat( exception.getCause() ).isInstanceOf( NoSuchFileException.class );
    }

    @Test
    void shouldThrowIfPolicyNameDoesNotExist()
    {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toPath().toAbsolutePath() )
                .set( policyConfig.base_directory, Path.of( "certificates/default" ) )
                .build();

        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( config, NullLogProvider.getInstance() );

        // when
        assertThrows( IllegalArgumentException.class, () -> sslPolicyLoader.getPolicy( BOLT ) );
    }

    @Test
    void shouldReturnNullPolicyIfNullRequested()
    {
        // given
        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( Config.defaults(), NullLogProvider.getInstance() );

        // when
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( null );

        // then
        assertNull( sslPolicy );
    }

    private File makeDir( File parent, String child ) throws IOException
    {
        File file = new File( parent, child );
        Files.createDirectory( file.toPath() );
        return file;
    }

    private void writeJunkToFile( File parent, String child ) throws IOException
    {
        File file = new File( parent, child );
        Files.write( file.toPath(), "junk data".getBytes() );
        file.setReadable( false, false );
        file.setWritable( false, false );
        file.setReadable( true );
        file.setWritable( true );
    }

    private SslPolicyLoader createSslPolicyLoader( boolean ignoreDotfiles )
    {
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toPath().toAbsolutePath() )
                .set( SslSystemInternalSettings.ignore_dotfiles, ignoreDotfiles )
                .set( policyConfig.enabled, Boolean.TRUE )
                .set( policyConfig.base_directory, Path.of( "certificates/default" ) )
                .build();

        return SslPolicyLoader.create( config, NullLogProvider.getInstance() );
    }

    private void assertPolicyValid( SslPolicy sslPolicy ) throws SSLException
    {
        assertNotNull( sslPolicy );
        assertNotNull( sslPolicy.privateKey() );
        assertNotNull( sslPolicy.certificateChain() );
        assertNotNull( sslPolicy.nettyClientContext() );
        assertNotNull( sslPolicy.nettyServerContext() );
    }
}
