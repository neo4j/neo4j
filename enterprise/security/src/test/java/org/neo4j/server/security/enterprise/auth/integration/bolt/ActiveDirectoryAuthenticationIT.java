/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;

/*
 * Only run these tests when the appropriate ActiveDirectory server is in fact live.
 * The tests remain here because they are very useful when developing and testing Active Directory
 * security features. Regular automated testing of Active Directory security should also be handled
 * in the smoke tests run downstream of the main build, so the fact that these tests are not run during
 * the main build should not be of serious concern.
 *
 * Note also that most of the security code related to Active Directory is identical to the LDAP code,
 * and so the tests in LdapAuthIT, which are run during normal build, do in fact test that
 * code. Testing against a real Active Directory is not possible during a build phase, and therefor
 * we keep this disabled by default.
 */
@Ignore
public class ActiveDirectoryAuthenticationIT
{
    @Rule
    public Neo4jWithSocket server =
            new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), asSettings( getSettingsFunction() ) );

    private void restartNeo4jServerWithOverriddenSettings( Consumer<Map<Setting<?>,String>> overrideSettingsFunction )
            throws IOException
    {
        server.shutdownDatabase();
        server.ensureDatabase( asSettings( overrideSettingsFunction ) );
    }

    private Consumer<Map<String,String>> asSettings( Consumer<Map<Setting<?>,String>> overrideSettingsFunction )
    {
        return settings ->
        {
            Map<Setting<?>,String> o = new LinkedHashMap<>();
            overrideSettingsFunction.accept( o );
            for ( Setting key : o.keySet() )
            {
                settings.put( key.name(), o.get( key ) );
            }
        };
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>,String>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled, "true" );
            settings.put( SecuritySettings.auth_provider, "ldap" );
            settings.put( SecuritySettings.ldap_server, "activedirectory.neohq.net" );
            settings.put( SecuritySettings.ldap_authentication_user_dn_template, "CN={0},CN=Users,DC=neo4j,DC=com" );
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
            settings.put( SecuritySettings.ldap_authorization_user_search_base, "cn=Users,dc=neo4j,dc=com" );
            settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(CN={0}))" );
            settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "memberOf" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                    "'CN=Neo4j Read Only,CN=Users,DC=neo4j,DC=com'=reader;" +
                    "CN=Neo4j Read-Write,CN=Users,DC=neo4j,DC=com=publisher;" +
                    "CN=Neo4j Schema Manager,CN=Users,DC=neo4j,DC=com=architect;" +
                    "CN=Neo4j Administrator,CN=Users,DC=neo4j,DC=com=admin" );
        };
    }

    private Consumer<Map<Setting<?>,String>> useSystemAccountSettings = settings ->
    {
        settings.put( SecuritySettings.ldap_authorization_use_system_account, "true" );
        settings.put( SecuritySettings.ldap_authorization_system_username, "Neo4j System" );
        settings.put( SecuritySettings.ldap_authorization_system_password, "ProudListingsMedia1" );
    };

    public Factory<TransportConnection> cf = (Factory<TransportConnection>) SecureSocketConnection::new;

    public HostnamePort address = new HostnamePort( "localhost:7687" );

    protected TransportConnection client;

    //------------------------------------------------------------------
    // Active Directory tests on EC2
    // NOTE: These rely on an external server and are not executed by automated testing
    //       They are here as a convenience for running local testing.

    @Test
    public void shouldNotBeAbleToLoginUnknownUserOnEC2() throws Throwable
    {

        assertAuthFail( "unknown", "ProudListingsMedia1" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "tank", "ProudListingsMedia1" );
        assertWriteSucceeds();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "tank", "ProudListingsMedia1" );
        assertWriteSucceeds();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "smith", "ProudListingsMedia1" );
        assertReadFails( "'smith' with no roles" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "smith", "ProudListingsMedia1" );
        assertReadFails( "'smith' with no roles" );
    }

    //------------------------------------------------------------------
    // Secure Active Directory tests on EC2
    // NOTE: These tests does not work together with EmbeddedTestCertificates used in the embedded secure LDAP tests!
    //       (This is because the embedded tests override the Java default key/trust store locations using
    //        system properties that will not be re-read)

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingLdapsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings
                .andThen( settings -> settings.put( SecuritySettings.ldap_server, "ldaps://activedirectory.neohq.net:636" ) ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingLDAPSOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.ldap_server, "ldaps://activedirectory.neohq.net:636" ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings.andThen( settings -> settings.put( SecuritySettings.ldap_use_starttls, "true" ) ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.ldap_use_starttls, "true" ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToAccessEC2ActiveDirectoryInstance() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings ->
        {
        } );

        // When
        assertAuth( "tank", "ProudListingsMedia1" );

        // Then
        assertReadSucceeds();
        assertWriteSucceeds();
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

    private void assertAuth( String username, String password ) throws Exception
    {
        assertAuth( username, password, null );
    }

    private void assertAuth( String username, String password, String realm ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( init( "TestClient/1.1", authToken( username, password, realm ) ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private Map<String,Object> authToken( String username, String password, String realm )
    {
        if ( realm != null && realm.length() > 0 )
        {
            return map( "principal", username, "credentials", password, "scheme", "basic", "realm", realm );
        }
        else
        {
            return map( "principal", username, "credentials", password, "scheme", "basic" );
        }
    }

    private void assertAuthFail( String username, String password ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", username,
                                "credentials", password, "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );
    }

    protected void assertReadSucceeds() throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertReadFails( String username ) throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Read operations are not allowed for user %s.", username ) ) ) );
    }

    protected void assertWriteSucceeds() throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertWriteFails( String username ) throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Write operations are not allowed for user %s.", username ) ) ) );
    }

}
