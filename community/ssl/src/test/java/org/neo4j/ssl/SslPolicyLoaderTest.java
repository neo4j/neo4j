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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static java.nio.file.Files.createDirectories;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.Config.newBuilder;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

@TestDirectoryExtension
class SslPolicyLoaderTest
{
    @Inject
    private TestDirectory testDirectory;

    private Path home;
    private Path publicCertificateFile;
    private Path privateKeyFile;

    @BeforeEach
    void setup() throws Exception
    {
        home = testDirectory.directoryPath( "home" );
        Path baseDir = home.resolve( "certificates/default" );
        publicCertificateFile = baseDir.resolve( "public.crt" );
        privateKeyFile = baseDir.resolve( "private.key" );

        new SelfSignedCertificateFactory().createSelfSignedCertificate( publicCertificateFile, privateKeyFile, "localhost" );

        Path trustedDir = baseDir.resolve( "trusted" );
        createDirectories( trustedDir );
        FileUtils.copyFile( publicCertificateFile, trustedDir.resolve( "public.crt" ) );
        createDirectories( baseDir.resolve( "revoked" ) );
    }

    @Test
    void shouldLoadBaseCryptographicObjects() throws Exception
    {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toAbsolutePath() )
                .set( policyConfig.enabled, Boolean.TRUE )
                .set( policyConfig.base_directory, Path.of("certificates/default" ) )
                .build();

        // when
        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( config, NullLogProvider.getInstance() );

        // then
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( TESTING );
        assertNotNull( sslPolicy );
        assertNotNull( sslPolicy.privateKey() );
        assertNotNull( sslPolicy.certificateChain() );
        assertNotNull( sslPolicy.nettyClientContext() );
        assertNotNull( sslPolicy.nettyServerContext() );
    }

    @Test
    void shouldComplainIfMissingPrivateKey() throws IOException
    {
        shouldComplainIfMissingFile( privateKeyFile );
    }

    @Test
    void shouldComplainIfMissingPublicCertificate() throws IOException
    {
        shouldComplainIfMissingFile( publicCertificateFile );
    }

    private void shouldComplainIfMissingFile( Path file ) throws IOException
    {
        // given
        Files.delete( file );

        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toAbsolutePath() )
                .set( policyConfig.enabled, Boolean.TRUE )
                .set( policyConfig.base_directory, Path.of( "certificates/default" ) )
                .build();

        // when
        Exception exception = assertThrows( Exception.class, () -> SslPolicyLoader.create( config, NullLogProvider.getInstance() ) );
        assertThat( exception.getCause() ).isInstanceOf( NoSuchFileException.class );
    }

    @Test
    void shouldThrowIfPolicyNameDoesNotExist()
    {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( TESTING );

        Config config = newBuilder()
                .set( neo4j_home, home.toAbsolutePath() )
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
}
