/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.mockito.Answers;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.stringValue;

public class SingleInstanceGetRoutingTableProcedureTest
{
    private static final NamedDatabaseId ID = from( DEFAULT_DATABASE_NAME, UUID.randomUUID() );
    private static final String UNKNOWN_DATABASE_NAME = "unknownDatabaseName";

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
    void shouldReturnEmptyRoutingTableWhenNoBoltConnectors() throws Exception
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = newConfig( Duration.ofSeconds( 123 ), null );

        var proc = newProcedure( portRegister, config );

        var result = proc.invoke( ID, MapValue.EMPTY );

        assertEquals( Duration.ofSeconds( 123 ).toMillis(), result.ttlMillis() );
        assertEquals( emptyList(), result.readEndpoints() );
        assertEquals( emptyList(), result.writeEndpoints() );
        assertEquals( emptyList(), result.routeEndpoints() );
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

        var input = new AnyValue[]{MapValue.EMPTY, stringValue( UNKNOWN_DATABASE_NAME )};

        var error = assertThrows( ProcedureException.class, () -> procedure.apply( null, input, null ) );
        assertEquals( DatabaseNotFound, error.status() );
    }

    @Test
    void shouldThrowWhenDatabaseIsStopped()
    {
        var portRegister = mock( ConnectorPortRegister.class );
        var config = Config.defaults();
        var databaseManager = databaseManagerMock( config, false );
        var procedure = newProcedure( databaseManager, portRegister, config );

        var input = new AnyValue[]{MapValue.EMPTY, stringValue( ID.name() )};

        var error = assertThrows( ProcedureException.class, () -> procedure.apply( null, input, null ) );
        assertEquals( DatabaseUnavailable, error.status() );
    }

    protected BaseGetRoutingTableProcedure newProcedure( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister, Config config )
    {
        return new SingleInstanceGetRoutingTableProcedure( DEFAULT_NAMESPACE, databaseManager, portRegister, config, nullLogProvider() );
    }

    protected List<SocketAddress> expectedWriters( SocketAddress selfAddress )
    {
        return singletonList( selfAddress );
    }

    private BaseGetRoutingTableProcedure newProcedure( ConnectorPortRegister portRegister, Config config )
    {
        var databaseManager = databaseManagerMock( config, true );
        return newProcedure( databaseManager, portRegister, config );
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
        var databaseIdRepository = mock( DatabaseIdRepository.Caching.class, Answers.RETURNS_DEEP_STUBS );

        when( databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ) ).thenReturn( Optional.of( ID ) );
        when( databaseContext.database() ).thenReturn( database );
        when( database.getConfig() ).thenReturn( config );
        when( database.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        when( availabilityGuard.isAvailable() ).thenReturn( databaseAvailable );
        when( databaseManager.getDatabaseContext( ID ) ).thenReturn( Optional.of( databaseContext ) );
        when( databaseManager.databaseIdRepository() ).thenReturn( databaseIdRepository );

        return databaseManager;
    }
}
