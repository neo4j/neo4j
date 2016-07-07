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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;

public class PluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction().andThen( pluginOnlyAuthSettings );
    }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic", "realm", "TestAuthenticationPlugin" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic", "realm", "TestAuthPlugin" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );


        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Write operations are not allowed" ) ) ) );

    }
}
