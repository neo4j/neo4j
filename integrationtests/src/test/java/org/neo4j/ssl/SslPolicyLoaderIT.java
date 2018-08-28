/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.bouncycastle.operator.OperatorCreationException;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.ssl.HostnameVerificationHelper.POLICY_NAME;
import static org.neo4j.ssl.HostnameVerificationHelper.aConfig;
import static org.neo4j.ssl.HostnameVerificationHelper.trust;

public class SslPolicyLoaderIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    @Test
    public void certificatesWithInvalidCommonNameAreRejected() throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException
    {
        // given server has a certificate that matches an invalid hostname
        Config serverConfig = aConfig( "invalid-not-localhost", testDirectory );

        // and client has any certificate (valid), since hostname validation is done from the client side
        Config clientConfig = aConfig( "localhost", testDirectory );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        int port = secureServer.port();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // when client connects to server with a non-matching hostname
        try
        {
            secureClient.connect( port );

            // then handshake complete with exception describing hostname mismatch
            secureClient.sslHandshakeFuture().get( 1, MINUTES );
        }
        catch ( ExecutionException e )
        {
            String expectedMessage = "No subject alternative DNS name matching localhost found.";
            assertThat( causes( e ).map( Throwable::getMessage ).collect( Collectors.toList() ),
                    IsCollectionContaining.hasItem( expectedMessage ) );
        }
        catch ( TimeoutException e )
        {
            e.printStackTrace();
        }
        finally
        {
            secureServer.stop();
        }
    }

    @Test
    public void normalBehaviourIfServerCertificateMatchesClientExpectation()
            throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException, TimeoutException, ExecutionException
    {
        // given server has valid hostname
        Config serverConfig = aConfig( "localhost", testDirectory );

        // and client has invalid hostname (which is irrelevant for hostname verification)
        Config clientConfig = aConfig( "invalid-localhost", testDirectory );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void legacyPolicyDoesNotHaveHostnameVerification()
            throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException, TimeoutException, ExecutionException
    {
        // given server has an invalid hostname
        Config serverConfig = aConfig( "invalid-localhost", testDirectory );

        // and client has invalid hostname (which is irrelevant for hostname verification)
        Config clientConfig = aConfig( "invalid-localhost", testDirectory );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, LOG_PROVIDER ).getPolicy( "legacy" );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, LOG_PROVIDER ).getPolicy( "legacy" );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    private void clientCanCommunicateWithServer( SecureClient secureClient, SecureServer secureServer )
            throws InterruptedException, TimeoutException, ExecutionException
    {
        int port = secureServer.port();
        try
        {
            secureClient.connect( port );
            ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( new byte[]{1, 2, 3, 4} );
            secureClient.channel().writeAndFlush( request );

            ByteBuf expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
            assertTrue( secureClient.sslHandshakeFuture().get( 1, MINUTES ).isActive() );
            secureClient.assertResponse( expected );
        }
        finally
        {
            secureServer.stop();
        }
    }

    private Stream<Throwable> causes( Throwable throwable )
    {
        Stream<Throwable> thisStream = Stream.of( throwable ).filter( Objects::nonNull );
        if ( throwable != null && throwable.getCause() != null )
        {
            return Stream.concat( thisStream, causes( throwable.getCause() ) );
        }
        else
        {
            return thisStream;
        }
    }
}
