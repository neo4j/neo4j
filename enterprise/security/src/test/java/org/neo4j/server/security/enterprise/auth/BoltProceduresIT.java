/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;

@RunWith( Parameterized.class )
public class BoltProceduresIT extends AuthProceduresTestLogic<BoltInteraction.BoltSubject>
{
    @BeforeClass
    public static void beforeClass() throws IOException
    {
        Path IMPERMANENT_DB_ROLES_PATH = Paths.get( "target/test-data/impermanent-db/data/dbms/roles" );
        if ( Files.exists( IMPERMANENT_DB_ROLES_PATH ) )
        {
            Files.delete( IMPERMANENT_DB_ROLES_PATH );
        }
    }

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(), getSettingsFunction() );

    public BoltProceduresIT()
    {
        super();
        IS_EMBEDDED = false;
        IS_BOLT = true;
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return settings -> {
            settings.put( GraphDatabaseSettings.auth_enabled, "true" );
            settings.put( GraphDatabaseSettings.auth_manager, "enterprise-auth-manager" );
        };
    }

    @Parameterized.Parameter( 0 )
    public Factory<Connection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    protected Connection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<Connection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer() throws Throwable
    {
        return new BoltInteraction(server, cf, address);
    }
}
