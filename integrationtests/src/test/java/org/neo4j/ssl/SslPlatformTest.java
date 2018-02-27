/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
