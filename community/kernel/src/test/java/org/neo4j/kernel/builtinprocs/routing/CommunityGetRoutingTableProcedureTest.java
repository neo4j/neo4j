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
package org.neo4j.kernel.builtinprocs.routing;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.values.AnyValue;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.connectors.Connector.ConnectorType.BOLT;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.kernel.builtinprocs.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;

public class CommunityGetRoutingTableProcedureTest
{
    @Test
    void shouldHaveCorrectSignature()
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        Config config = Config.defaults();
        CallableProcedure proc = newProcedure( portRegister, config );

        ProcedureSignature signature = proc.signature();

        assertThat( signature.inputSignature(), containsInAnyOrder(
                inputField( "context", NTMap ) ) );

        assertThat( signature.outputSignature(), containsInAnyOrder(
                outputField( "ttl", NTInteger ),
                outputField( "servers", NTList( NTMap ) ) ) );
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

        RoutingResult result = proc.invoke( new AnyValue[0] );

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

        RoutingResult result = proc.invoke( new AnyValue[0] );

        assertEquals( Duration.ofMinutes( 42 ).toMillis(), result.ttlMillis() );

        AdvertisedSocketAddress address = new AdvertisedSocketAddress( "neo4j.com", 7687 );
        assertEquals( singletonList( address ), result.readEndpoints() );
        assertEquals( expectedWriters( address ), result.writeEndpoints() );
        assertEquals( singletonList( address ), result.routeEndpoints() );
    }

    protected BaseGetRoutingTableProcedure newProcedure( ConnectorPortRegister portRegister, Config config )
    {
        return new CommunityGetRoutingTableProcedure( DEFAULT_NAMESPACE, portRegister, config );
    }

    protected List<AdvertisedSocketAddress> expectedWriters( AdvertisedSocketAddress selfAddress )
    {
        return singletonList( selfAddress );
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
