/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.docs;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;

@RunWith( Parameterized.class )
public class BoltFullExchangesDocTest extends BoltFullDocTest
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( settings -> {
        settings.put( GraphDatabaseSettings.auth_enabled, "true" );
        settings.put( GraphDatabaseSettings.auth_store, this.getClass().getResource( "/authorization/auth" ).getPath() );
    } );

    @Parameterized.Parameter( 0 )
    public String testName;

    @Parameterized.Parameter( 1 )
    public DocExchangeExample example;

    @Parameterized.Parameter( 2 )
    public Supplier<Connection> client;

    @Parameterized.Parameter( 3 )
    public HostnamePort address;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedFullProtocolExamples()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        HostnamePort address = new HostnamePort( "localhost:7687" );
        for ( DocExchangeExample ex : DocsRepository.docs().read(
                "dev/transport.asciidoc",
                "code[data-lang=\"bolt_exchange\"]",
                DocExchangeExample.exchange_example ) )
        {
            mappings.add( new Object[]{"Socket    - " + ex.name(), ex,
                    (Supplier<Connection>) SecureSocketConnection::new, address});
            mappings.add( new Object[]{"WebSocket - " + ex.name(), ex,
                    (Supplier<Connection>) SecureWebSocketConnection::new , address} );
        }

        for ( DocExchangeExample ex : DocsRepository.docs().read(
                "dev/examples.asciidoc",
                "code[data-lang=\"bolt_exchange\"]",
                DocExchangeExample.exchange_example ) )
        {
            mappings.add( new Object[]{"Socket    - " + ex.name(), ex,
                    (Supplier<Connection>) SecureSocketConnection::new, address});
            mappings.add( new Object[]{"WebSocket - " + ex.name(), ex,
                    (Supplier<Connection>) SecureWebSocketConnection::new , address} );
        }

        return mappings;
    }

    @Override
    protected Connection createClient()
    {
        return client.get();
    }

    @Override
    protected HostnamePort address()
    {
        return address;
    }

    @Override
    protected DocExchangeExample example()
    {
        return example;
    }
}
