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

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.security.enterprise.auth.SecuritySettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.Messages.init;
import static org.neo4j.bolt.v1.messaging.message.Messages.pullAll;
import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.helpers.collection.MapUtil.map;

@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "Test",
        partitions = {@CreatePartition(
                name = "example",
                suffix = "dc=example,dc=com", contextEntry = @ContextEntry(
                entryLdif = "dn: dc=example,dc=com\n" +
                        "dc: example\n" +
                        "o: example\n" +
                        "objectClass: top\n" +
                        "objectClass: dcObject\n" +
                        "objectClass: organization\n\n" ) )}, loadedSchemas = {
        @LoadSchema( name = "nis", enabled = true ),
        @LoadSchema( name = "posix", enabled = false )} )
@CreateLdapServer( transports = {@CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" )} )
public class LdapAuthenticationIT extends AbstractLdapTestUnit
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(), getSettingsFunction() );

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return settings -> {
            settings.put( GraphDatabaseSettings.auth_enabled, "true" );
            settings.put( GraphDatabaseSettings.auth_manager, "enterprise-auth-manager" );
            settings.put( SecuritySettings.internal_authentication_enabled, "true" );
            settings.put( SecuritySettings.internal_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_enabled, "false" );
            settings.put( SecuritySettings.ldap_server, "0.0.0.0:10389" );
            settings.put( SecuritySettings.ldap_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_system_username, "uid=admin,ou=system" );
            settings.put( SecuritySettings.ldap_system_password, "secret" );
        };
    }

    public Factory<Connection> cf = (Factory<Connection>) SecureSocketConnection::new;

    public HostnamePort address = new HostnamePort( "localhost:7687" );

    protected Connection client;

    @Test
    @ApplyLdifFiles( "ldap_test_data.ldif" )
    public void shouldBeAbleToLoginWithLdap() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo",
                                "credentials", "abc123", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    @Test
    @ApplyLdifFiles( "ldap_test_data.ldif" )
    public void shouldBeAbleToLoginWithLdapAndAuthorizeInternally() throws Throwable
    {
        //--------------------------
        // First login as admin and create the internal user 'neo' with role 'reader'

        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.changePassword", Collections.singletonMap( "password", "secret" ) ),
                pullAll() ) );

        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        reconnect();

        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "secret", "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.createUser( 'neo', 'invalid', false ) CALL dbms.addUserToRole( 'neo', 'reader' ) RETURN 0" ),
                pullAll() ) );

        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        //--------------------------
        // Then login user 'neo' with LDAP and test that internal authorization gives correct permission
        reconnect();

        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo",
                                "credentials", "abc123", "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        assertThat( client, eventuallyRecieves( msgSuccess(), msgSuccess() ) );

        client.send( TransportTestUtil.chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        assertThat( client, eventuallyRecieves(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Write operations are not allowed for `neo` transactions." ) ) ) );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    private void reconnect() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
        this.client = cf.newInstance();
    }
}
