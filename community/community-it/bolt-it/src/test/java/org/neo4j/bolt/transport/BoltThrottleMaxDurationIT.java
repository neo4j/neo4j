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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.runtime.DefaultBoltConnection;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.OtherThreadRule;

import static java.util.Collections.singletonMap;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith( OtherThreadExtension.class )
public class BoltThrottleMaxDurationIT
{
    @Inject
    private Neo4jWithSocket server;
    @Inject
    private OtherThreadRule otherThread;

    private AssertableLogProvider logProvider;

    private HostnamePort address;
    private TransportConnection client;
    private TransportTestUtil util;

    public static Stream<Arguments> factoryProvider()
    {
        // we're not running with WebSocketChannels because of their duplex communication model
        return Stream.of( Arguments.of( SocketConnection.class ), Arguments.of( SecureSocketConnection.class ) );
    }

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setGraphDatabaseFactory( getTestGraphDatabaseFactory() );
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );

        otherThread.set( 5, TimeUnit.MINUTES );

        address = server.lookupDefaultConnector();
        util = new TransportTestUtil();
    }

    @AfterEach
    public void cleanup() throws IOException
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();

        logProvider = new AssertableLogProvider();

        factory.setInternalLogProvider( logProvider );

        return factory;
    }

    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofMinutes( 5 ) );
            settings.put( GraphDatabaseInternalSettings.bolt_outbound_buffer_throttle_max_duration, Duration.ofSeconds( 30 ) );
            settings.put( BoltConnector.encryption_level, OPTIONAL );
        };
    }

    @ParameterizedTest( name = "{displayName} {index}" )
    @MethodSource( "factoryProvider" )
    public void sendingButNotReceivingClientShouldBeKilledWhenWriteThrottleMaxDurationIsReached( Class<? extends TransportConnection> c ) throws Exception
    {
        this.client =  c.getDeclaredConstructor().newInstance();

        int numberOfRunDiscardPairs = 10_000;
        String largeString = " ".repeat( 8 * 1024 );

        client.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        assertThat( client ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        Future<?> sender = otherThread.execute( () ->
        {
            for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
            {
                client.send( util.defaultRunAutoCommitTx( "RETURN $data as data", asMapValue( singletonMap( "data", largeString ) ) ) );
            }

            return null;
        } );

        var e = assertThrows( ExecutionException.class, () -> otherThread.get().awaitFuture( sender ) );

        assertThat( getRootCause( e ) ).isInstanceOf( SocketException.class );

        assertThat( logProvider ).forClass( DefaultBoltConnection.class ).forLevel( ERROR )
                .assertExceptionForLogMessage( "Unexpected error detected in bolt session" )
                .hasStackTraceContaining( "will be closed because the client did not consume outgoing buffers for " );
    }

}
