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
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.function.Consumer;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.LdapContext;

import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
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
public class LdapAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    private final String MD5_HASHED_abc123 = "{MD5}6ZoYxCjLONXyYIU2eJIuAw=="; // Hashed 'abc123' (see ldap_test_data.ldif)

    @Before
    @Override
    public void setup()
    {
        super.setup();
        getLdapServer().setConfidentialityRequired( false );
    }

    private void restartNeo4jServerWithSaslDigestMd5() throws IOException
    {
        server.shutdownDatabase();
        server.ensureDatabase( asSettings( ldapOnlyAuthSettings.andThen(
                settings -> {
                    settings.put( SecuritySettings.ldap_auth_mechanism, "DIGEST-MD5" );
                    settings.put( SecuritySettings.ldap_user_dn_template, "{0}" );
                }
        ) ) );
    }

    private void restartNeo4jServerWithSaslCramMd5() throws IOException
    {
        server.shutdownDatabase();
        server.ensureDatabase( asSettings( ldapOnlyAuthSettings.andThen(
                settings -> {
                    settings.put( SecuritySettings.ldap_auth_mechanism, "CRAM-MD5" );
                    settings.put( SecuritySettings.ldap_user_dn_template, "{0}" );
                }
        ) ) );
    }

    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction().andThen( ldapOnlyAuthSettings ).andThen( settings -> {
            settings.put( SecuritySettings.ldap_server, "0.0.0.0:10389" );
            settings.put( SecuritySettings.ldap_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_authentication_cache_enabled, "true" );
            settings.put( SecuritySettings.ldap_system_username, "uid=admin,ou=system" );
            settings.put( SecuritySettings.ldap_system_password, "secret" );
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "true" );
            settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
            settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                    "500=reader;501=publisher;502=architect;503=admin" );
        } );
    }

    @Test
    public void shouldLoginWithLdap() throws Throwable
    {
        assertAuth( "neo4j", "abc123" );
        reconnect();
        assertAuth( "neo4j", "abc123" );
    }

    @Test
    public void shouldLoginWithLdapWithAuthenticationCacheDisabled() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authentication_cache_enabled, "false" );
        } ) );

        assertAuth( "neo4j", "abc123" );
        reconnect();
        assertAuth( "neo4j", "abc123" );
    }

    @Test
    public void shouldFailToLoginWithLdapIfInvalidCredentials() throws Throwable
    {
        assertAuthFail( "neo4j", "CANT_REMEMBER_MY_PASSWORDS_ANYMORE!" );
    }

    @Test
    public void shoulFailToLoginWithLdapIfInvalidCredentialsFollowingSuccessfulLogin() throws Throwable
    {
        assertAuth( "neo4j", "abc123" );
        reconnect();
        assertAuthFail( "neo4j", "" );
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
        testAuthWithReaderUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithLdapOnly() throws Throwable
    {
        testAuthWithPublisherUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithLdapOnly() throws Throwable
    {
        testAuthWithNoPermissionUser( "smith" );
    }

    @Test
    public void shouldShowCurrentUser() throws Throwable
    {
        // When
        assertAuth( "smith", "abc123" );
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.security.showCurrentUser()" ),
                pullAll() ) );

        // Then
        // Assuming showCurrentUser has fields username, roles, flags
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( "smith" ), equalTo( emptyList() ), equalTo( emptyList() ) ) )
            ) );
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
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } );

        // Then
        testAuthWithReaderUser();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithUserLdapContext() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } );

        // Then
        testAuthWithPublisherUser();
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithUserLdapContext() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } );

        // Then
        assertAuth( "neo4j", "abc123" );

        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.security.clearAuthCache() MATCH (n) RETURN n" ), pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(),
                msgFailure( Status.Security.AuthorizationExpired, "LDAP authorization info expired." ) ) );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContext() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        } );

        // Then
        testAuthWithNoPermissionUser( "smith" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContextAndNoGroupToRoleMapping() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, null );
        } );

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
            settings.put( SecuritySettings.active_realms,
                    SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME );
            settings.put( SecuritySettings.native_authentication_enabled, "false" );
            settings.put( SecuritySettings.native_authorization_enabled, "true" );
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

    @Test
    public void shouldBeAbleToUseProcedureAllowedAnnotationWithLdapGroupToRoleMapping() throws Throwable
    {
        // When
        restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=role1" );
        } ) );

        GraphDatabaseAPI graphDatabaseAPI = (GraphDatabaseAPI) server.graphDatabaseService();
        graphDatabaseAPI.getDependencyResolver().resolveDependency( Procedures.class )
                .registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );

        assertAuth( "neo", "abc123" );
        client.send( TransportTestUtil.chunk( run( "CALL test.allowedProcedure1()" ), pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( "foo" ) ) ),
                msgSuccess() ) );
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

    @Test
    public void shouldBeAbleToLoginWithLdapWhenSelectingRealmFromClient() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.active_realms,
                    SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME );
            settings.put( SecuritySettings.native_authentication_enabled, "true" );
            settings.put( SecuritySettings.native_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_enabled, "true" );
        } );

        //--------------------------
        // When we have a native 'tank' that is read only, and ldap 'tank' that is publisher
        testCreateReaderUser("tank");

        //--------------------------
        // Then native "tank" is reader
        reconnect();
        testAuthWithReaderUser("tank", "native");

        //--------------------------
        // And ldap "tank" is publisher
        reconnect();
        testAuthWithPublisherUser("tank", "ldap");
    }

    @Test
    public void shouldClearAuthenticationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_server, "ldaps://localhost:10636" );
            } ) );

            // Then
            assertAuth( "tank", "abc123", "ldap" );
            changeLDAPPassword( "tank", "abc123", "123abc" );

            // When logging in without clearing cache
            reconnect();

            // Then
            assertAuthFail( "tank", "123abc" );
            reconnect();
            assertAuth( "tank", "abc123", "ldap" );

            // When clearing cache and logging in
            reconnect();
            testClearAuthCache();
            reconnect();

            // Then
            assertAuthFail( "tank", "abc123" );
            reconnect();
            assertAuth( "tank", "123abc", "ldap" );
        }
    }

    @Test
    public void shouldClearAuthorizationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartNeo4jServerWithOverriddenSettings( ldapOnlyAuthSettings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_server, "ldaps://localhost:10636" );
            } ) );

            // Then
            assertAuth( "tank", "abc123", "ldap" );
            assertReadSucceeds();
            assertWriteSucceeds();

            // When
            changeLDAPGroup( "tank", "abc123", "reader" );

            // When logging in without clearing cache
            reconnect();
            assertAuth( "tank", "abc123", "ldap" );

            // Then
            assertReadSucceeds();
            assertWriteSucceeds();

            // When clearing cache and logging in
            reconnect();
            testClearAuthCache();
            reconnect();

            // Then
            assertAuth( "tank", "abc123", "ldap" );
            assertReadSucceeds();
            assertWriteFails( "tank" );
        }
    }

    protected void assertReadSucceeds() throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk( run( "MATCH (n) RETURN count(n)" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( greaterThanOrEqualTo( 0L ) ) ),
                msgSuccess() ) );
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

    private void testClearAuthCache() throws Exception
    {
        assertAuth( "neo4j", "abc123" );

        client.send( TransportTestUtil.chunk( run( "CALL dbms.security.clearAuthCache()" ), pullAll() ) );

        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private void modifyLDAPAttribute( String username, Object credentials, String attribute, Object value )
            throws Throwable
    {
        String principal = String.format( "cn=%s,ou=users,dc=example,dc=com", username );
        String principal1 = String.format( "cn=%s,ou=users,dc=example,dc=com", username );
        JndiLdapContextFactory contextFactory = new JndiLdapContextFactory();
        contextFactory.setUrl( "ldaps://localhost:10636" );
        LdapContext ctx = contextFactory.getLdapContext( principal1, credentials );

        ModificationItem[] mods = new ModificationItem[1];
        mods[0] = new ModificationItem( DirContext.REPLACE_ATTRIBUTE, new BasicAttribute( attribute, value ) );

        // Perform the update
        ctx.modifyAttributes( principal, mods );
        ctx.close();
    }

    private void changeLDAPPassword( String username, Object credentials, Object newCredentials ) throws Throwable
    {
        modifyLDAPAttribute( username, credentials, "userpassword", newCredentials );
    }

    private void changeLDAPGroup( String username, Object credentials, String group ) throws Throwable
    {
        String gid;
        switch ( group )
        {
        case "reader":
            gid = "500";
            break;
        case "publisher":
            gid = "501";
            break;
        case "architect":
            gid = "502";
            break;
        case "admin":
            gid = "503";
            break;
        case "none":
            gid = "504";
            break;
        default:
            throw new IllegalArgumentException( "Invalid group name '" + group +
                    "', expected one of none, reader, publisher, architect, or admin" );
        }
        modifyLDAPAttribute( username, credentials, "gidnumber", gid );
    }

    private static Consumer<Map<Setting<?>,String>> ldapOnlyAuthSettings = settings ->
    {
        settings.put( SecuritySettings.active_realm, SecuritySettings.LDAP_REALM_NAME );
        settings.put( SecuritySettings.native_authentication_enabled, "false" );
        settings.put( SecuritySettings.native_authorization_enabled, "false" );
        settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
        settings.put( SecuritySettings.ldap_authorization_enabled, "true" );
    };

    private static Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2Settings = settings -> {
        settings.put( SecuritySettings.active_realm, SecuritySettings.LDAP_REALM_NAME );
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

    private static Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2NotUsingSystemAccountSettings =
            activeDirectoryOnEc2Settings.andThen( settings -> {
                settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
            } );

    private static Consumer<Map<Setting<?>,String>> activeDirectoryOnEc2UsingSystemAccountSettings =
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
