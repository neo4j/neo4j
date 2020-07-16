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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.scheduling.BoltConnectionReadLimiter;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

import static java.util.Collections.singletonMap;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
class BoltChannelAutoReadLimiterIT
{
    @Inject
    private Neo4jWithSocket server;

    private AssertableLogProvider logProvider;
    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws Exception
    {
        server.setGraphDatabaseFactory( getTestGraphDatabaseFactory() );
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
        connection = new SocketConnection();
        util = new TransportTestUtil();

        installSleepProcedure( server.graphDatabaseService() );
    }

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();

        logProvider = new AssertableLogProvider();

        factory.setInternalLogProvider( logProvider );

        return factory;

    }

    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled, false );
            settings.put( GraphDatabaseInternalSettings.bolt_inbound_message_throttle_high_water_mark, 8 );
            settings.put( GraphDatabaseInternalSettings.bolt_inbound_message_throttle_low_water_mark, 3 );
        };
    }

    @Test
    public void largeNumberOfSlowRunningJobsShouldChangeAutoReadState() throws Exception
    {
        int numberOfRunDiscardPairs = 20;
        String largeString = " ".repeat( 8 * 1024 );

        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // when
        for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
        {
            connection.send( util.defaultRunAutoCommitTxWithoutResult(
                    "CALL boltissue.sleep( $data )", ValueUtils.asMapValue( singletonMap( "data", largeString ) ) ) );
        }

        // expect
        for ( int i = 0; i < numberOfRunDiscardPairs; i++ )
        {
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess(), msgSuccess() ) );
        }

        assertThat( logProvider ).forClass( BoltConnectionReadLimiter.class )
                .forLevel( WARN ).containsMessages( "disabled", "enabled" );
    }

    private static void installSleepProcedure( GraphDatabaseService db ) throws ProcedureException
    {
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) db;

        dbApi.getDependencyResolver().resolveDependency( GlobalProcedures.class ).register(
                new CallableProcedure.BasicProcedure(
                        procedureSignature( "boltissue", "sleep" )
                                .in( "data", Neo4jTypes.NTString )
                                .out( ProcedureSignature.VOID )
                                .build() )
                {
                    @Override
                    public RawIterator<AnyValue[],ProcedureException> apply(
                            Context context, AnyValue[] objects, ResourceTracker resourceTracker ) throws ProcedureException
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
