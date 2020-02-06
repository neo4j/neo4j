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
package org.neo4j.bolt.v4.runtime;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.scheduling.ExecutorBoltScheduler;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.SpiedAssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.MessageMatchers.msgSuccess;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

public class ResetFuzzIT
{
    private static final int TEST_EXECUTION_TIME = 2000;

    private final int seed = new Random().nextInt();
    private final Random rand = new Random( seed );

    private final AssertableLogProvider internalLogProvider = new SpiedAssertableLogProvider( ExecutorBoltScheduler.class );
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );
    private final TransportTestUtil util = new TransportTestUtil();
    private HostnamePort address;

    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String SHORT_QUERY_3 = "RETURN 1";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( fsRule ).around( server );

    @Before
    public void setup()
    {
        address = server.lookupDefaultConnector();
    }

    @After
    public void tearDown()
    {
        userLogProvider.print( System.out );
        internalLogProvider.print( System.out );
    }

    @Test
    public void shouldTerminateAutoCommitQuery() throws Exception
    {
        List<Pair<byte[],Integer>> sequences = asList(
                Pair.of( util.defaultRunAutoCommitTx( SHORT_QUERY_1 ), 2 ),
                Pair.of( util.defaultRunAutoCommitTxWithoutResult( SHORT_QUERY_2 ), 2 ),
                Pair.of( util.chunk( BoltV4Messages.run( SHORT_QUERY_3 ) ), 1 )
        );

        execute( sequences );
    }

    @Test
    public void shouldTerminateLongRunningAutoCommitQuery() throws Exception
    {
        // It takes a while for kernel to notice the tx get killed.
        List<Pair<byte[],Integer>> sequences = singletonList( Pair.of( util.defaultRunAutoCommitTxWithoutResult( LONG_QUERY ), 2 ) );
        execute( sequences );
    }

    @Test
    public void shouldTerminateQueryInExplicitTransaction() throws Exception
    {
        List<Pair<byte[],Integer>> sequences = asList(
                Pair.of( util.defaultRunExplicitCommitTxAndRollBack( SHORT_QUERY_1 ), 4 ),
                Pair.of( util.defaultRunExplicitCommitTxAndCommit( SHORT_QUERY_2 ), 4 ),
                Pair.of( util.chunk( BoltV4Messages.begin(), BoltV4Messages.run( SHORT_QUERY_3 ), BoltV4Messages.pullAll() ), 3 ),
                Pair.of( util.chunk( BoltV4Messages.begin(), BoltV4Messages.run( SHORT_QUERY_1 ) ), 2 ),
                Pair.of( util.chunk( BoltV4Messages.begin() ), 1 )
        );

        execute( sequences );
    }

    @Test
    public void shouldTerminateLongRunningQueryInExplicitTransaction() throws Exception
    {
        List<Pair<byte[],Integer>> sequences = singletonList( Pair.of( util.defaultRunExplicitCommitTxAndRollBack( LONG_QUERY ), 4 ) );
        execute( sequences );
    }

    private void execute( List<Pair<byte[],Integer>> sequences ) throws Exception
    {
        var connection = connectAndAuthenticate();
        long deadline = System.currentTimeMillis() + TEST_EXECUTION_TIME;

        // when
        while ( System.currentTimeMillis() < deadline )
        {
            int sent = dispatchRandomSequenceOfMessages( connection, sequences );
            assertResetWorks( connection, sent );
        }
    }

    private void assertResetWorks( TransportConnection connection, int sent ) throws IOException
    {
        connection.send( util.defaultReset() );
        assertThat( connection, util.eventuallyReceives( sent, msgSuccess() ) );
    }

    private int dispatchRandomSequenceOfMessages( TransportConnection connection, List<Pair<byte[],Integer>> sequences ) throws IOException
    {
        Pair<byte[],Integer> pair = sequences.get( rand.nextInt( sequences.size() ) );
        connection.send( pair.first() );
        return pair.other();
    }

    private TransportConnection connectAndAuthenticate() throws Exception
    {
        var connection = new SocketConnection();

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );

        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        return connection;
    }

    private TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();
        factory.setInternalLogProvider( internalLogProvider );
        factory.setUserLogProvider( userLogProvider );
        return factory;
    }

    private static Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings -> {
            settings.put( BoltConnector.encryption_level, OPTIONAL );
            settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
            settings.put( BoltConnector.unsupported_thread_pool_queue_size, -1 );
            settings.put( BoltConnector.thread_pool_min_size, 1 );
            settings.put( BoltConnector.thread_pool_max_size, 1 );
        };
    }
}

