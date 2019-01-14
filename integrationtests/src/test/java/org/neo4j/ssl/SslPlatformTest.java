/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import io.netty.handler.ssl.SslProvider;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.ssl.SslContextFactory.makeSslContext;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@SuppressWarnings( "FieldCanBeLocal" )
public class SslPlatformTest
{
    private static final byte[] REQUEST = {1, 2, 3, 4};

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private SecureServer server;
    private SecureClient client;
    private ByteBuf expected;

    @Test
    public void shouldSupportOpenSSLOnSupportedPlatforms() throws Exception
    {
        // depends on the statically linked uber-jar with boring ssl: http://netty.io/wiki/forked-tomcat-native.html
        assumeTrue( SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC_OSX );
        assumeThat( System.getProperty( "os.arch" ), equalTo( "x86_64" ) );
        assumeThat( SystemUtils.JAVA_VENDOR, isOneOf( "Oracle Corporation", "Sun Microsystems Inc." ) );

        // given
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true, SslProvider.OPENSSL.name() ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false, SslProvider.OPENSSL.name() ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel().writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
        client.sslHandshakeFuture().get( 1, MINUTES );
        client.assertResponse( expected );
    }
}
