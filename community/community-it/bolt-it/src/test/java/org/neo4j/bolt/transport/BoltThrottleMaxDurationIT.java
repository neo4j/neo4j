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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.SocketException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.DefaultBoltConnection;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

@RunWith( Parameterized.class )
public class BoltThrottleMaxDurationIT
{
    private AssertableLogProvider logProvider;
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );
    @Rule
    public OtherThreadRule<Void> otherThread = new OtherThreadRule<>( 5, TimeUnit.MINUTES );
    @Parameterized.Parameter
    public Factory<TransportConnection> cf;

    private HostnamePort address;
    private TransportConnection client;
    private TransportTestUtil util;

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        // we're not running with WebSocketChannels because of their duplex communication model
        return asList( SocketConnection::new, SecureSocketConnection::new );
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
            settings.put( GraphDatabaseSettings.bolt_outbound_buffer_throttle_max_duration, Duration.ofSeconds( 30 ) );
            settings.put( BoltConnector.encryption_level, OPTIONAL );
        };
    }

    @Before
    public void setup()
    {
        client = cf.newInstance();
        address = server.lookupDefaultConnector();
        util = new TransportTestUtil();
    }

    @After
    public void after() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    @Test
    public void sendingButNotReceivingClientShouldBeKilledWhenWriteThrottleMaxDurationIsReached() throws Exception
    {
        int numberOfRunDiscardPairs = 10_000;
        String largeString = " ".repeat( 8 * 1024 );

        client.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        assertThat( client ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        Future<?> sender = otherThread.execute( state ->
        {
            for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
            {
                client.send( util.defaultRunAutoCommitTx( "RETURN $data as data", asMapValue( singletonMap( "data", largeString ) ) ) );
            }

            return null;
        } );

        try
        {
            otherThread.get().awaitFuture( sender );

            fail( "should throw ExecutionException instead" );
        }
        catch ( ExecutionException e )
        {
            assertThat( getRootCause( e ) ).isInstanceOf( SocketException.class );
        }

        assertThat( logProvider ).forClass( DefaultBoltConnection.class ).forLevel( ERROR )
                .assertExceptionForLogMessage( "Unexpected error detected in bolt session" )
                .hasStackTraceContaining( "will be closed because the client did not consume outgoing buffers for " );
    }

}
