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
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URL;
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
import org.neo4j.server.security.enterprise.auth.SecuritySettings;
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

@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "Test",
        partitions = { @CreatePartition(
                name = "example",
                suffix = "dc=example,dc=com",
                contextEntry = @ContextEntry( entryLdif = "dn: dc=example,dc=com\n" +
                                                          "dc: example\n" +
                                                          "o: example\n" +
                                                          "objectClass: top\n" +
                                                          "objectClass: dcObject\n" +
                                                          "objectClass: organization\n\n" ) ),
        },
        loadedSchemas = {
                @LoadSchema( name = "nis", enabled = true ),
        } )
@CreateLdapServer(
        transports = { @CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" ),
                       @CreateTransport( protocol = "LDAPS", port = 10636, address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name  = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = { StartTlsHandler.class },
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( "ldap_test_data.ldif" )
public class LdapAuthenticationIT extends AbstractLdapTestUnit
{
    final String MD5_HASHED_abc123 = "{MD5}6ZoYxCjLONXyYIU2eJIuAw=="; // Hashed 'abc123' (see ldap_test_data.ldif)

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(), getSettingsFunction() );

    private void restartNeo4jServerWithOverriddenSettings( Consumer<Map<Setting<?>, String>> overrideSettingsFunction )
            throws IOException
    {
        server.restartDatabase( overrideSettingsFunction );
    }

    private void restartNeo4jServerWithSaslDigestMd5() throws IOException
    {
        server.restartDatabase( ldapOnlyAuthSettings.andThen(
                settings -> {
                    settings.put( SecuritySettings.ldap_auth_mechanism, "DIGEST-MD5" );
                    settings.put( SecuritySettings.ldap_user_dn_template, "{0}" );
                }
        ) );
    }

    private void restartNeo4jServerWithSaslCramMd5() throws IOException
    {
        server.restartDatabase( ldapOnlyAuthSettings.andThen(
                settings -> {
                    settings.put( SecuritySettings.ldap_auth_mechanism, "CRAM-MD5" );
                    settings.put( SecuritySettings.ldap_user_dn_template, "{0}" );
                }
        ) );
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
            settings.put( SecuritySettings.internal_authentication_enabled, "true" );
            settings.put( SecuritySettings.internal_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_server, "0.0.0.0:10389" );
            settings.put( SecuritySettings.ldap_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_system_username, "uid=admin,ou=system" );
            settings.put( SecuritySettings.ldap_system_password, "secret" );
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "true" );
            settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
            settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin" );
        };
    }

    public Factory<TransportConnection> cf = (Factory<TransportConnection>) SecureSocketConnection::new;

    public HostnamePort address = new HostnamePort( "localhost:7687" );

    protected TransportConnection client;

    @Test
    public void shouldLoginWithLdap() throws Throwable
    {
        assertAuth( "neo4j", "abc123" );
    }

    @Test
    public void shouldFailToLoginWithLdapIfInvalidCredentials() throws Throwable
    {
        assertAuthFail( "neo4j", "CANT_REMEMBER_MY_PASSWORDS_ANYMORE!" );
    }

    @Test
    public void shouldLoginWithLdapUsingSaslDigestMd5() throws Throwable
    {
        // When
        restartNeo4jServerWithSaslDigestMd5();

        // Then
        assertAuth( "neo4j", MD5_HASHED_abc123 );
    }

    @Test
    public void shouldFailToLoginWithLdapDigestMd5IfInvalidCredentials() throws Throwable
    {
        // When
        restartNeo4jServerWithSaslDigestMd5();

        // Then
        assertAuthFail( "neo4j", MD5_HASHED_abc123.toUpperCase() );
    }

    @Test
    public void shouldLoginWithLdapUsingSaslCramMd5() throws Throwable
    {
        // When
        restartNeo4jServerWithSaslCramMd5();

        // Then
        assertAuth( "neo4j", MD5_HASHED_abc123 );
    }

    @Test
    public void shouldFailToLoginWithLdapCramMd5IfInvalidCredentials() throws Throwable
    {
        // When
        restartNeo4jServerWithSaslCramMd5();

        // Then
        assertAuthFail( "neo4j", MD5_HASHED_abc123.toUpperCase() );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithLdapOnly() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings );

        // Then
        testAuthWithReaderUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithLdapOnly() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings );

        // Then
        testAuthWithPublisherUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithLdapOnly() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings );

        // Then
        testAuthWithNoPermissionUser( "smith" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithLdapOnlyAndNoGroupToRoleMapping() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, null );
        } ) );

        // Then
        // User 'neo' has reader role by default, but since we are not passing a group-to-role mapping
        // he should get no permissions
        testAuthWithNoPermissionUser( "neo" );
    }

    @Test

    public void shouldBeAbleToLoginAndAuthorizeWithLdapOnlyAndQuotedGroupToRoleMapping() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                    " '500'  =\t reader  ; \"501\"\t=publisher\n;502 =architect  ;  \"503\"=  \nadmin" );
        } ) );

        // Then
        testAuthWithReaderUser();
        reconnect();
        testAuthWithPublisherUser();
        reconnect();
        testAuthWithNoPermissionUser( "smith" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContext() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } ) );

        // Then
        testAuthWithReaderUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithUserLdapContext() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } ) );

        // Then
        testAuthWithPublisherUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContext() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } ) );

        // Then
        testAuthWithNoPermissionUser( "smith" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContextAndNoGroupToRoleMapping() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, null );
        } ) );

        // Then
        // User 'neo' has reader role by default, but since we are not passing a group-to-role mapping
        // he should get no permissions
        testAuthWithNoPermissionUser( "neo" );
    }

    @Test
    public void shouldBeAbleToLoginWithLdapAndAuthorizeInternally() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.internal_authentication_enabled, "false" );
            settings.put( SecuritySettings.internal_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_enabled, "false" );
        } );

        // Then
        // First login as the admin user 'neo4j' and create the internal user 'neo' with role 'reader'
        testCreateReaderUser();

        // Then login user 'neo' with LDAP and test that internal authorization gives correct permission
        reconnect();

        testAuthWithReaderUser();
    }

    //------------------------------------------------------------------
    // Embedded secure LDAP tests
    // NOTE: These can potentially mess up the environment for any subsequent tests relying on the
    //       default Java key/trust stores

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithLdapOnlyUsingLDAPS() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_server, "ldaps://localhost:10636" );
            } ) );

            // Then
            testAuthWithReaderUser();
        }
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingLDAPS() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
                settings.put( SecuritySettings.ldap_server, "ldaps://localhost:10636" );
            } ) );

            // Then
            testAuthWithReaderUser();
        }
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithLdapOnlyUsingStartTls() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_server, "localhost:10389" );
                settings.put( SecuritySettings.ldap_use_starttls, "true" );
            } ) );

            // Then
            testAuthWithReaderUser();
        }
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithLdapUserContextUsingStartTls() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
                settings.put( SecuritySettings.ldap_server, "localhost:10389" );
                settings.put( SecuritySettings.ldap_use_starttls, "true" );
            } ) );

            // Then
            testAuthWithReaderUser();
        }
    }

    //------------------------------------------------------------------
    // Active Directory tests on EC2
    // NOTE: These rely on an external server and are not executed by automated testing
    //       They are here as a convenience for running local testing.

    //@Test
    public void shouldNotBeAbleToLoginUnknownUserOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings );

        assertAuthFail( "unknown", "abc123ABC123" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2UsingSystemAccountSettings );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithUserLdapContextOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings );

        assertAuth( "tank", "abc123ABC123" );
        assertWriteSucceeds();
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizePublisherOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2UsingSystemAccountSettings );

        assertAuth( "tank", "abc123ABC123" );
        assertWriteSucceeds();
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContextOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings );

        assertAuth( "smith", "abc123ABC123" );
        assertReadFails( "smith" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2UsingSystemAccountSettings );

        assertAuth( "smith", "abc123ABC123" );
        assertReadFails( "smith" );
    }

    //------------------------------------------------------------------
    // Secure Active Directory tests on EC2
    // NOTE: These tests does not work together with EmbeddedTestCertificates used in the embedded secure LDAP tests!
    //       (This is because the embedded tests override the Java default key/trust store locations using
    //        system properties that will not be re-read)

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingLdapsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2UsingSystemAccountSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_server, "ldaps://henrik.neohq.net:636" );
        }) );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingLDAPSOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_server, "ldaps://henrik.neohq.net:636" );
        }) );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2UsingSystemAccountSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_use_starttls, "true" );
        }) );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //@Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( activeDirectoryOnEc2NotUsingSystemAccountSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_use_starttls, "true" );
        }) );

        assertAuth( "neo", "abc123ABC123" );
        assertReadSucceeds();
        assertWriteFails( "neo" );
    }

    //-------------------------------------------------------------------------

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
        getLdapServer().setConfidentialityRequired( false );
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

    private void testCreateReaderUser() throws Exception
    {
        assertAuth( "neo4j", "abc123" );

        // NOTE: The default user 'neo4j' has password change required, so we have to first change it
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.security.changeUserPassword('neo4j', '123', false) " +
                     "CALL dbms.security.createUser( 'neo', 'invalid', false ) " +
                     "CALL dbms.security.addRoleToUser( 'reader', 'neo' ) RETURN 0" ),
                pullAll() ) );

        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private void testAuthWithReaderUser() throws Exception
    {
        // When
        assertAuth( "neo", "abc123" );

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
                        String.format( "Write operations are not allowed for 'neo'." ) ) ) );
    }

    private void testAuthWithPublisherUser() throws Exception
    {
        // When
        assertAuth( "tank", "abc123" );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    private void testAuthWithNoPermissionUser( String username ) throws Exception
    {
        // When
        assertAuth( username, "abc123" );

        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Read operations are not allowed for '%s'.", username ) ) ) );
    }

    private void assertAuth( String username, String password ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", username,
                                "credentials", password, "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private void assertAuthFail( String username, String password ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", username,
                                "credentials", password, "scheme", "basic" ) ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized, "The client is unauthorized due to authentication failure." ) ) );
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
                        String.format( "Read operations are not allowed for '%s'.", username ) ) ) );
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
                        String.format( "Write operations are not allowed for '%s'.", username ) ) ) );
    }

    private Consumer<Map<Setting<?>,String>> ldapOnlyAuthSettings = settings ->
    {
        settings.put( SecuritySettings.internal_authentication_enabled, "false" );
        settings.put( SecuritySettings.internal_authorization_enabled, "false" );
        settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
        settings.put( SecuritySettings.ldap_authorization_enabled, "true" );
    };

    private Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2Settings = settings -> {
        //settings.put( SecuritySettings.ldap_server, "ec2-176-34-79-113.eu-west-1.compute.amazonaws.com:389" );
        settings.put( SecuritySettings.ldap_server, "henrik.neohq.net:389" );
        settings.put( SecuritySettings.ldap_user_dn_template, "cn={0},cn=Users,dc=neo4j,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "cn=Users,dc=neo4j,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(CN={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "memberOf" );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                "'CN=Neo4j Read Only,CN=Users,DC=neo4j,DC=com'=reader;" +
                "CN=Neo4j Read-Write,CN=Users,DC=neo4j,DC=com=publisher;" +
                "CN=Neo4j Schema Manager,CN=Users,DC=neo4j,DC=com=architect;" +
                "CN=Neo4j Administrator,CN=Users,DC=neo4j,DC=com=admin"
        );
    };

    private Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2NotUsingSystemAccountSettings =
            activeDirectoryOnEc2Settings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
            } );

    private Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2UsingSystemAccountSettings =
            activeDirectoryOnEc2Settings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_authorization_use_system_account, "true" );
                settings.put( SecuritySettings.ldap_system_username, "Petra Selmer" );
                settings.put( SecuritySettings.ldap_system_password, "S0uthAfrica" );
            } );

    //-------------------------------------------------------------------------
    // TLS helper
    private class EmbeddedTestCertificates implements AutoCloseable
    {
        private static final String KEY_STORE = "javax.net.ssl.keyStore";
        private static final String KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
        private static final String TRUST_STORE = "javax.net.ssl.trustStore";
        private static final String TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

        private final String keyStore = System.getProperty( KEY_STORE );
        private final String keyStorePassword = System.getProperty( KEY_STORE_PASSWORD );
        private final String trustStore = System.getProperty( TRUST_STORE );
        private final String trustStorePassword = System.getProperty( TRUST_STORE_PASSWORD );

        public EmbeddedTestCertificates()
        {
            File keyStoreFile = fileFromResources( "/neo4j_ldap_test_keystore.jks" );
            String keyStorePath = keyStoreFile.getAbsolutePath();

            System.setProperty( KEY_STORE, keyStorePath );
            System.setProperty( KEY_STORE_PASSWORD, "secret" );
            System.setProperty( TRUST_STORE, keyStorePath );
            System.setProperty( TRUST_STORE_PASSWORD, "secret" );
        }

        @Override
        public void close() throws Exception
        {
            resetProperty( KEY_STORE, keyStore );
            resetProperty( KEY_STORE_PASSWORD, keyStorePassword );
            resetProperty( TRUST_STORE, trustStore );
            resetProperty( TRUST_STORE_PASSWORD, trustStorePassword );
        }

        private File fileFromResources( String path )
        {
            URL url = getClass().getResource( path );
            return new File( url.getFile() );
        }

        private void resetProperty( String property, String value )
        {
            if ( property != null )
            {
                System.clearProperty( property );
            }
            else
            {
                System.setProperty( property, value );
            }
        }
    }
}
