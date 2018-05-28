/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v1.transport.integration;

import org.apache.commons.lang.StringUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.runtime.BoltChannelAutoReadLimiter;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.collection.RawIterator;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

@RunWith( Parameterized.class )
public class BoltChannelAutoReadLimiterIT
{
    private AssertableLogProvider logProvider;
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(),
            fsRule::get, getSettingsFunction() );
    private TransportConnection client;

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<TransportConnection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();

        logProvider = new AssertableLogProvider();

        factory.setInternalLogProvider( logProvider );

        return factory;

    }

    protected Consumer<Map<String, String>> getSettingsFunction()
    {
        return settings -> settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" );
    }

    @Before
    public void setup() throws Exception
    {
        installSleepProcedure( server.graphDatabaseService() );

        this.client = cf.newInstance();
    }

    @After
    public void after() throws Exception
    {
        this.client.disconnect();
    }

    @Test
    public void largeNumberOfSlowRunningJobsShouldChangeAutoReadState() throws Exception
    {
        int numberOfRunDiscardPairs = 1000;
        String largeString = StringUtils.repeat( " ", 8 * 1024  );

        client.connect( address )
            .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
            .send( TransportTestUtil.chunk(
                    init( "TestClient/1.1", emptyMap() ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // when
        for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
        {
            client.send( TransportTestUtil.chunk(
                    run( "CALL boltissue.sleep( $data )", singletonMap( "data", largeString ) ),
                    discardAll()
            ) );
        }

        // expect
        for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
        {
            assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
        }

        logProvider.assertAtLeastOnce(
                AssertableLogProvider.inLog( BoltChannelAutoReadLimiter.class ).warn( CoreMatchers.containsString( "disabled" ), CoreMatchers.anything(),
                        CoreMatchers.anything() ) );
        logProvider.assertAtLeastOnce(
                AssertableLogProvider.inLog( BoltChannelAutoReadLimiter.class ).warn( CoreMatchers.containsString( "enabled" ), CoreMatchers.anything(),
                        CoreMatchers.anything() ) );
    }

    private static void installSleepProcedure( GraphDatabaseService db ) throws ProcedureException
    {
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) db;

        dbApi.getDependencyResolver().resolveDependency( Procedures.class ).register(
                new CallableProcedure.BasicProcedure(
                        procedureSignature("boltissue", "sleep")
                                .in( "data", Neo4jTypes.NTString )
                                .out( ProcedureSignature.VOID )
                                .build())
                {
                    @Override
                    public RawIterator<Object[],ProcedureException> apply(
                            Context context, Object[] objects, ResourceTracker resourceTracker ) throws ProcedureException
                    {
                        try
                        {
                            Thread.sleep( 50 );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new ProcedureException( Status.General.UnknownError, e, "Interrupted" );
                        }
                        return RawIterator.empty();
                    }
                } );
    }

}
