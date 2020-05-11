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
package org.neo4j.procedure.builtin.routing;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.stringValue;

public class SingleInstanceGetRoutingTableProcedureTest
{
    private static final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private static final NamedDatabaseId ID = databaseIdRepository.defaultDatabase();
    private static final NamedDatabaseId UNKNOWN_ID = databaseIdRepository.getRaw( "unknown_database_name" );

    @Test
    void shouldHaveCorrectSignature()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = Config.defaults();
        var proc = newProcedure( portRegister, config );

        var signature = proc.signature();

        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );
        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
        assertTrue( signature.systemProcedure() );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = Config.defaults();

        var proc = newProcedure( portRegister, config );

        var name = proc.signature().name();

        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    @Test
    void shouldThrowWhenNoBoltConnectors()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofSeconds( 123 ), null );

        var proc = newProcedure( portRegister, config );

        var exception = assertThrows( ProcedureException.class, () -> proc.invoke( ID, MapValue.EMPTY ) );

        assertEquals( Status.Procedure.ProcedureCallFailed, exception.status() );
        assertThat( exception.getLocalizedMessage(),
                    endsWith( " Please update your configuration for '" + BoltConnector.enabled.name() + "'" ) );
    }

    @Test
    void shouldReturnRoutingTable() throws Exception
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofMinutes( 42 ), new SocketAddress( "neo4j.com", 7687 ) );

        var proc = newProcedure( portRegister, config );

        var result = proc.invoke( ID, MapValue.EMPTY );

        assertEquals( Duration.ofMinutes( 42 ).toMillis(), result.ttlMillis() );

        var address = new SocketAddress( "neo4j.com", 7687 );
        assertEquals( singletonList( address ), result.readEndpoints() );
        assertEquals( expectedWriters( address ), result.writeEndpoints() );
        assertEquals( singletonList( address ), result.routeEndpoints() );
    }

    @Test
    void shouldThrowWhenDatabaseDoesNotExist()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = Config.defaults();
        var procedure = newProcedure( portRegister, config );

        databaseIdRepository.filter( UNKNOWN_ID.name() );
        var input = new AnyValue[]{MapValue.EMPTY, stringValue( UNKNOWN_ID.name() )};
        var error = assertThrows( ProcedureException.class, () -> procedure.apply( null, input, null ) );
        assertEquals( DatabaseNotFound, error.status() );
    }

    @Test
    void shouldThrowWhenDatabaseIsStopped()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = Config.defaults();
        var databaseManager = databaseManagerMock( config, false );
        var procedure = newProcedure( databaseManager, portRegister, config, nullLogProvider() );

        var input = new AnyValue[]{MapValue.EMPTY, stringValue( ID.name() )};

        var error = assertThrows( ProcedureException.class, () -> procedure.apply( null, input, null ) );
        assertEquals( DatabaseUnavailable, error.status() );
    }

    @Test
    void shouldThrowWhenAddressCtxIsPresentButEmpty()
    {
        // given
        var ctxContents = new MapValueBuilder();
        ctxContents.add( SingleInstanceGetRoutingTableProcedure.ADDRESS_CONTEXT_KEY, Values.EMPTY_STRING );
        var ctx = ctxContents.build();

        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofSeconds( 100 ), new SocketAddress( "neo4j.com", 7687 ) );
        var databaseManager = databaseManagerMock( config, true );
        var logProvider = new AssertableLogProvider();

        var procedure = newProcedure( databaseManager, portRegister, config, logProvider );
        var expectedMessage = "An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.";

        // when/then
        assertThrows( ProcedureException.class, () -> procedure.invoke( ID, ctx ), expectedMessage );
    }

    @Test
    void shouldThrowWhenHostCtxIsInvalid()
    {
        // given
        var ctxContents = new MapValueBuilder();
        ctxContents.add( SingleInstanceGetRoutingTableProcedure.ADDRESS_CONTEXT_KEY, Values.stringValue( "not a socket address" ) );
        var ctx = ctxContents.build();

        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofSeconds( 100 ), new SocketAddress( "neo4j.com", 7687 ) );
        var databaseManager = databaseManagerMock( config, true );
        var logProvider = new AssertableLogProvider();

        var procedure = newProcedure( databaseManager, portRegister, config, logProvider );
        var expectedMessage = "An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.";

        // when/then
        assertThrows( ProcedureException.class, () -> procedure.invoke( ID, ctx ), expectedMessage );
    }

    @Test
    void shouldUseClientProvidedHostAsAdvertisedAddress() throws Exception
    {
        // given
        var advertisedBoldPort = 8776;
        var advertisedBoltAddress = new SocketAddress( "neo4j.com", advertisedBoldPort );
        var clientProvidedHost = "my.neo4j-service.com";

        var ctxContents = new MapValueBuilder();
        ctxContents.add( SingleInstanceGetRoutingTableProcedure.ADDRESS_CONTEXT_KEY, Values.stringValue( clientProvidedHost ) );
        var ctx = ctxContents.build();

        var portRegister = mock( ConnectorPortRegister.class );
        when( portRegister.getLocalAddress( BoltConnector.NAME ) ).thenReturn( new HostnamePort( "neo4j.com", advertisedBoldPort ) );
        var config = newConfig( Duration.ofSeconds( 100 ), advertisedBoltAddress );
        var databaseManager = databaseManagerMock( config, true );
        var logProvider = new AssertableLogProvider();

        var procedure = newProcedure( databaseManager, portRegister, config, logProvider );
        var expectedAddress = new SocketAddress( clientProvidedHost, advertisedBoldPort );

        // when
        var result = procedure.invoke( ID, ctx );

        // then
        assertEquals( singletonList( expectedAddress ), result.readEndpoints() );
        assertEquals( expectedWriters( expectedAddress ), result.writeEndpoints() );
        assertEquals( singletonList( expectedAddress ), result.routeEndpoints() );
    }

    @Test
    void shouldUseClientProvidedHostAndPortAsAdvertisedAddress() throws Exception
    {
        // given
        var advertisedBoltAddress = new SocketAddress( "neo4j.com", 7687 );
        var clientProvidedPort = 8888;
        var clientProvidedHost = "my.neo4j-service.com";
        var clientProvidedHostPortStr = String.format( "%s:%d", clientProvidedHost, clientProvidedPort );

        var ctxContents = new MapValueBuilder();
        ctxContents.add( SingleInstanceGetRoutingTableProcedure.ADDRESS_CONTEXT_KEY, Values.stringValue( clientProvidedHostPortStr ) );
        var ctx = ctxContents.build();

        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofSeconds( 100 ), advertisedBoltAddress );
        var databaseManager = databaseManagerMock( config, true );
        var logProvider = new AssertableLogProvider();

        var procedure = newProcedure( databaseManager, portRegister, config, logProvider );
        var expectedAddress = new SocketAddress( clientProvidedHost, clientProvidedPort );

        // when
        var result = procedure.invoke( ID, ctx );

        // then
        assertEquals( singletonList( expectedAddress ), result.readEndpoints() );
        assertEquals( expectedWriters( expectedAddress ), result.writeEndpoints() );
        assertEquals( singletonList( expectedAddress ), result.routeEndpoints() );
    }

    protected BaseGetRoutingTableProcedure newProcedure( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister, Config config,
                                                         LogProvider logProvider )
    {
        return new SingleInstanceGetRoutingTableProcedure( DEFAULT_NAMESPACE, databaseManager, portRegister, config, logProvider );
    }

    protected List<SocketAddress> expectedWriters( SocketAddress selfAddress )
    {
        return singletonList( selfAddress );
    }

    private BaseGetRoutingTableProcedure newProcedure( ConnectorPortRegister portRegister, Config config )
    {
        var databaseManager = databaseManagerMock( config, true );
        return newProcedure( databaseManager, portRegister, config, nullLogProvider() );
    }

    private static Config newConfig( Duration routingTtl, SocketAddress boltAddress )
    {
        var builder = Config.newBuilder();
        if ( routingTtl != null )
        {
            builder.set( GraphDatabaseSettings.routing_ttl, routingTtl );
        }
        if ( boltAddress != null )
        {
            builder.set( BoltConnector.enabled, true );
            builder.set( BoltConnector.listen_address, boltAddress );
            builder.set( BoltConnector.advertised_address, boltAddress );
        }
        return builder.build();
    }

    @SuppressWarnings( "unchecked" )
    private static DatabaseManager<DatabaseContext> databaseManagerMock( Config config, boolean databaseAvailable )
    {
        var databaseManager = mock( DatabaseManager.class );
        var databaseContext = mock( DatabaseContext.class );
        var database = mock( Database.class );
        var availabilityGuard = mock( DatabaseAvailabilityGuard.class );

        when( databaseContext.database() ).thenReturn( database );
        when( database.getConfig() ).thenReturn( config );
        when( database.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        when( availabilityGuard.isAvailable() ).thenReturn( databaseAvailable );
        when( databaseManager.getDatabaseContext( ID ) ).thenReturn( Optional.of( databaseContext ) );
        when( databaseManager.databaseIdRepository() ).thenReturn( databaseIdRepository );

        return databaseManager;
    }
}
