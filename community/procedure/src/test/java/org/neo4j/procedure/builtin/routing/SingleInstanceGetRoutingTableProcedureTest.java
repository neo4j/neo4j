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

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.connectors.Connector.ConnectorType.BOLT;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;

public class SingleInstanceGetRoutingTableProcedureTest
{
    private static final DatabaseId ID = new DatabaseId( DEFAULT_DATABASE_NAME );
    private static final DatabaseId UNKNOWN_ID = new DatabaseId( "unknown_database_name" );

    @Test
    void shouldHaveCorrectSignature()
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = Config.defaults();
        CallableProcedure proc = newProcedure( portRegister, config );

        ProcedureSignature signature = proc.signature();

        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );

        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = Config.defaults();

        CallableProcedure proc = newProcedure( portRegister, config );

        QualifiedName name = proc.signature().name();

        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    @Test
    void shouldReturnEmptyRoutingTableWhenNoBoltConnectors() throws Exception
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = newConfig( "123s", null );

        BaseGetRoutingTableProcedure proc = newProcedure( portRegister, config );

        RoutingResult result = proc.invoke( ID, MapValue.EMPTY );

        assertEquals( Duration.ofSeconds( 123 ).toMillis(), result.ttlMillis() );
        assertEquals( emptyList(), result.readEndpoints() );
        assertEquals( emptyList(), result.writeEndpoints() );
        assertEquals( emptyList(), result.routeEndpoints() );
    }

    @Test
    void shouldReturnRoutingTable() throws Exception
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = newConfig( "42m", "neo4j.com:7687" );

        BaseGetRoutingTableProcedure proc = newProcedure( portRegister, config );

        RoutingResult result = proc.invoke( ID, MapValue.EMPTY );

        assertEquals( Duration.ofMinutes( 42 ).toMillis(), result.ttlMillis() );

        AdvertisedSocketAddress address = new AdvertisedSocketAddress( "neo4j.com", 7687 );
        assertEquals( singletonList( address ), result.readEndpoints() );
        assertEquals( expectedWriters( address ), result.writeEndpoints() );
        assertEquals( singletonList( address ), result.routeEndpoints() );
    }

    @Test
    void shouldThrowWhenDatabaseDoesNotExist()
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = Config.defaults();
        BaseGetRoutingTableProcedure procedure = newProcedure( portRegister, config );

        ProcedureException error = assertThrows( ProcedureException.class, () -> procedure.invoke( UNKNOWN_ID, MapValue.EMPTY ) );

        assertThat( error.getMessage(), both( containsString( UNKNOWN_ID.name() ) ).and( containsString( "does not exist" ) ) );
    }

    protected BaseGetRoutingTableProcedure newProcedure( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister, Config config )
    {
        return new SingleInstanceGetRoutingTableProcedure( DEFAULT_NAMESPACE, databaseManager, portRegister, config );
    }

    protected List<AdvertisedSocketAddress> expectedWriters( AdvertisedSocketAddress selfAddress )
    {
        return singletonList( selfAddress );
    }

    @SuppressWarnings( "unchecked" )
    private BaseGetRoutingTableProcedure newProcedure( ConnectorPortRegister portRegister, Config config )
    {
        DatabaseManager<DatabaseContext> databaseManager = mock( DatabaseManager.class );
        DatabaseContext databaseContext = mock( DatabaseContext.class );
        Database database = mock( Database.class );
        when( databaseContext.database() ).thenReturn( database );
        when( database.getConfig() ).thenReturn( config );
        when( databaseManager.getDatabaseContext( ID ) ).thenReturn( Optional.of( databaseContext ) );
        return newProcedure( databaseManager, portRegister, config );
    }

    private static Config newConfig( String routingTtl, String boltAddress )
    {
        Config.Builder builder = Config.builder();
        if ( routingTtl != null )
        {
            builder.withSetting( GraphDatabaseSettings.routing_ttl, routingTtl );
        }
        if ( boltAddress != null )
        {
            BoltConnector connector = new BoltConnector( "my_bolt" );
            builder.withSetting( connector.enabled, TRUE );
            builder.withSetting( connector.type, BOLT.toString() );
            builder.withSetting( connector.listen_address, boltAddress );
            builder.withSetting( connector.advertised_address, boltAddress );
        }
        return builder.build();
    }
}
