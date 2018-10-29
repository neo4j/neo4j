/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapOperationErrorException;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.api.filtering.EntryFilteringCursor;
import org.apache.directory.server.core.api.interceptor.BaseInterceptor;
import org.apache.directory.server.core.api.interceptor.Interceptor;
import org.apache.directory.server.core.api.interceptor.context.SearchOperationContext;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.LdapContext;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.server.security.enterprise.auth.LdapRealm;
import org.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.fail;

interface TimeoutTests
{ /* Category marker */
}

@SuppressWarnings( "deprecation" )
@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "Test",
        partitions = {@CreatePartition(
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
                @LoadSchema( name = "nis" ),
        } )
@CreateLdapServer(
        transports = {@CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", port = 10636, address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = {StartTlsHandler.class},
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( "ldap_test_data.ldif" )
public class LdapAuthIT extends EnterpriseAuthenticationTestBase
{
    private static final String LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS = "LDAP: error code 49 - INVALID_CREDENTIALS";
    private static final String REFUSED_IP = "127.0.0.1"; // "0.6.6.6";

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();
        getLdapServer().setConfidentialityRequired( false );
    }

    @Override
    protected Map<Setting<?>,String> getSettings()
    {
        Map<Setting<?>,String> settings = new HashMap<>();
        settings.put( SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME );
        settings.put( SecuritySettings.ldap_server, "0.0.0.0:10389" );
        settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authentication_cache_enabled, "true" );
        settings.put( SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system" );
        settings.put( SecuritySettings.ldap_authorization_system_password, "secret" );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin" );
        settings.put( SecuritySettings.procedure_roles, "test.staticReadProcedure:role1" );
        settings.put( SecuritySettings.ldap_read_timeout, "1s" );
        settings.put( SecuritySettings.ldap_authorization_use_system_account, "false" );
        return settings;
    }

    @Test
    public void shouldShowCurrentUser()
    {
        try ( Driver driver = connectDriver( "smith", "abc123" ); Session session = driver.session() )
        {
            // when
            Record record = session.run( "CALL dbms.showCurrentUser()" ).single();

            // then
            // Assuming showCurrentUser has fields username, roles, flags
            assertThat( record.get( 0 ).asString(), equalTo( "smith" ) );
            assertThat( record.get( 1 ).asList(), equalTo( Collections.emptyList() ) );
            assertThat( record.get( 2 ).asList(), equalTo( Collections.emptyList() ) );
        }
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithLdapOnlyAndNoGroupToRoleMapping() throws IOException
    {
        restartServerWithOverriddenSettings( SecuritySettings.ldap_authorization_group_to_role_mapping.name(), null );
        // Then
        // User 'neo' has reader role by default, but since we are not passing a group-to-role mapping
        // he should get no permissions
        assertReadFails( "neo", "abc123" );
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithserLdapContext()
    {
        // Given
        try ( Driver driver = connectDriver( "neo4j", "abc123" ) )
        {
            assertReadSucceeds( driver );

            try ( Session session = driver.session() )
            {
                session.run( "CALL dbms.security.clearAuthCache()" );
            }

            try
            {
                assertReadFails( driver );
                fail( "should have failed due to authorization expired" );
            }
            catch ( ServiceUnavailableException e )
            {
                // TODO Bolt should handle the AuthorizationExpiredException better
            }
        }
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithUserLdapContext()
    {
        // Given
        try ( Driver driver = connectDriver( "neo4j", "abc123" ) )
        {
            assertReadSucceeds( driver );

            try ( Session session = driver.session() )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CALL dbms.security.clearAuthCache()" );
                    assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) );
                    tx.success();
                }
            }
        }
    }

    @Test
    public void shouldKeepAuthorizationForLifetimeOfTransaction() throws Throwable
    {
        assertKeepAuthorizationForLifetimeOfTransaction( "neo",
                tx -> assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) ) );
    }

    @Test
    public void shouldKeepAuthorizationForLifetimeOfTransactionWithProcedureAllowed() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.ldap_authorization_group_to_role_mapping.name(), "503=admin;504=role1" );
        dbRule.resolveDependency( Procedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );
        assertKeepAuthorizationForLifetimeOfTransaction( "smith",
                tx -> assertThat( tx.run( "CALL test.staticReadProcedure()" ).single().get( 0 ).asString(), equalTo( "static" ) ) );
    }

    private void assertKeepAuthorizationForLifetimeOfTransaction( String username, Consumer<Transaction> assertion ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        final Throwable[] threadFail = {null};

        Thread readerThread = new Thread( () ->
        {
            try
            {
                try ( Driver driver = connectDriver( username, "abc123" );
                        Session session = driver.session();
                        Transaction tx = session.beginTransaction() )
                {
                    assertion.accept( tx );
                    latch.startAndWaitForAllToStart();
                    latch.finishAndWaitForAllToFinish();
                    assertion.accept( tx );
                    tx.success();
                }
            }
            catch ( Throwable t )
            {
                threadFail[0] = t;
                // Always release the latch so we get the failure in the main thread
                latch.start();
                latch.finish();
            }
        } );

        readerThread.start();
        latch.startAndWaitForAllToStart();

        clearAuthCacheFromDifferentConnection();

        latch.finishAndWaitForAllToFinish();

        readerThread.join();
        if ( threadFail[0] != null )
        {
            throw threadFail[0];
        }
    }

    @Test
    public void shouldFailIfInvalidLdapServer() throws IOException
    {
        // When
        restartServerWithOverriddenSettings( SecuritySettings.ldap_server.name(), "ldap://127.0.0.1" );
        try
        {
            connectDriver( "neo", "abc123" );
            fail( "should have refused connection" );
        }
        catch ( TransientException e )
        {
            assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_CONNECTION_REFUSED_CLIENT_MESSAGE ) );
        }
    }

    @Test
    @Category( TimeoutTests.class )
    public void shouldTimeoutIfLdapServerDoesNotRespond() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            restartServerWithOverriddenSettings(
                    SecuritySettings.ldap_read_timeout.name(), "1s",
                    SecuritySettings.ldap_authorization_connection_pooling.name(), "true",
                    SecuritySettings.ldap_authorization_use_system_account.name(), "true"
            );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @Category( TimeoutTests.class )
    public void shouldTimeoutIfLdapServerDoesNotRespondWithoutConnectionPooling() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            restartServerWithOverriddenSettings(
                    // NOTE: Pooled connections from previous test runs will not be affected by this read timeout setting
                    SecuritySettings.ldap_read_timeout.name(), "1s",
                    SecuritySettings.ldap_authorization_connection_pooling.name(), "false",
                    SecuritySettings.ldap_authorization_use_system_account.name(), "true"
            );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @Category( TimeoutTests.class )
    public void shouldFailIfLdapSearchFails() throws IOException
    {
        try ( DirectoryServiceFailOnSearch ignore = new DirectoryServiceFailOnSearch() )
        {
            restartServerWithOverriddenSettings(
                    SecuritySettings.ldap_read_timeout.name(), "1s",
                    SecuritySettings.ldap_authorization_use_system_account.name(), "true"
            );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @Category( TimeoutTests.class )
    public void shouldTimeoutIfLdapServerDoesNotRespondWithLdapUserContext() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            // When
            restartServerWithOverriddenSettings( SecuritySettings.ldap_read_timeout.name(), "1s" );

            try
            {
                connectDriver( "neo", "abc123" );
                fail( "should have timed out" );
            }
            catch ( TransientException e )
            {
                assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_READ_TIMEOUT_CLIENT_MESSAGE ) );
            }
        }
    }

    @Test
    public void shouldGetCombinedAuthorization() throws Throwable
    {
        restartServerWithOverriddenSettings(
                SecuritySettings.auth_providers.name(), SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME,
                SecuritySettings.native_authentication_enabled.name(), "true",
                SecuritySettings.native_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authentication_enabled.name(), "true",
                SecuritySettings.ldap_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authorization_use_system_account.name(), "true"
        );

        // Given
        // we have a native 'tank' that is read only, and ldap 'tank' that is publisher
        createNativeUser( "tank", "localpassword", PredefinedRoles.READER );

        // Then
        // the created "tank" can log in and gets roles from both providers
        // because the system account is used to authorize over the ldap provider
        try ( Driver driver = connectDriver( "tank", "localpassword", "native" ) )
        {
            assertRoles( driver, PredefinedRoles.READER, PredefinedRoles.PUBLISHER );
        }

        // the ldap "tank" can also log in and gets roles from both providers
        try ( Driver driver = connectDriver( "tank", "abc123", "ldap" ) )
        {
            assertRoles( driver, PredefinedRoles.READER, PredefinedRoles.PUBLISHER );
        }
    }

    // ===== Logging tests =====

    @Test
    public void shouldNotLogErrorsFromLdapRealmWhenLoginSuccessfulInNativeRealmNativeFirst() throws IOException, InvalidArgumentsException
    {
        restartServerWithOverriddenSettings(
                SecuritySettings.auth_providers.name(), SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME,
                SecuritySettings.native_authentication_enabled.name(), "true",
                SecuritySettings.native_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authentication_enabled.name(), "true",
                SecuritySettings.ldap_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authorization_use_system_account.name(), "true"
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // Then
        // the created "foo" can log in
        assertAuth( "foo", "bar" );

        // We should not get errors spammed in the security log
        assertSecurityLogDoesNotContain( "ERROR" );
    }

    @Test
    public void shouldNotLogErrorsFromLdapRealmWhenLoginSuccessfulInNativeRealmLdapFirst() throws IOException, InvalidArgumentsException
    {
        restartServerWithOverriddenSettings(
                SecuritySettings.auth_providers.name(), SecuritySettings.LDAP_REALM_NAME + "," + SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.native_authentication_enabled.name(), "true",
                SecuritySettings.native_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authentication_enabled.name(), "true",
                SecuritySettings.ldap_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authorization_use_system_account.name(), "true"
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // Then
        // the created "foo" can log in
        assertAuth( "foo", "bar" );

        // We should not get errors spammed in the security log
        assertSecurityLogDoesNotContain( "ERROR" );
    }

    @Test
    public void shouldLogInvalidCredentialErrorFromLdapRealm() throws Throwable
    {
        // When
        assertAuthFail( "neo", "wrong-password" );

        // Then
        assertSecurityLogContains( LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS );
    }

    @Test
    public void shouldLogInvalidCredentialErrorFromLdapRealmWhenAllProvidersFail() throws Throwable
    {
        restartServerWithOverriddenSettings(
                SecuritySettings.auth_providers.name(), SecuritySettings.NATIVE_REALM_NAME + ", " + SecuritySettings.LDAP_REALM_NAME,
                SecuritySettings.native_authentication_enabled.name(), "true",
                SecuritySettings.native_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authentication_enabled.name(), "true",
                SecuritySettings.ldap_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authorization_use_system_account.name(), "true"
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // When
        assertAuthFail( "foo", "wrong-password" );

        // Then
        assertSecurityLogContains( LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS );
    }

    @Test
    public void shouldLogConnectionRefusedFromLdapRealm() throws Throwable
    {
        // When
        restartServerWithOverriddenSettings( SecuritySettings.ldap_server.name(), "ldap://" + REFUSED_IP );

        try
        {
            connectDriver( "neo", "abc123" );
            fail( "Expected connection refused" );
        }
        catch ( TransientException e )
        {
            assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_CONNECTION_REFUSED_CLIENT_MESSAGE ) );
        }

        assertSecurityLogContains( "ERROR" );
        assertSecurityLogContains( "auth server connection refused" );
        assertSecurityLogContains( REFUSED_IP );
    }

    @Test
    public void shouldLogConnectionRefusedFromLdapRealmWithMultipleRealms() throws Throwable
    {
        restartServerWithOverriddenSettings(
            SecuritySettings.auth_providers.name(), SecuritySettings.NATIVE_REALM_NAME + ", " + SecuritySettings.LDAP_REALM_NAME,
            SecuritySettings.native_authentication_enabled.name(), "true",
            SecuritySettings.native_authorization_enabled.name(), "true",
            SecuritySettings.ldap_authentication_enabled.name(), "true",
            SecuritySettings.ldap_authorization_enabled.name(), "true",
            SecuritySettings.ldap_authorization_use_system_account.name(), "true",
            SecuritySettings.ldap_server.name(), "ldap://" + REFUSED_IP
        );

        assertAuthFail( "neo", "abc123" );

        assertSecurityLogContains( "ERROR" );
        assertSecurityLogContains( "LDAP connection refused" );
        assertSecurityLogContains( REFUSED_IP );
    }

    @Test
    public void shouldClearAuthenticationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartServerWithOverriddenSettings( SecuritySettings.ldap_server.name(), "ldaps://localhost:10636" );

            // Then
            assertAuth( "tank", "abc123" );
            changeLDAPPassword( "tank", "abc123", "123abc" );

            // When logging in without clearing cache

            // Then
            assertAuthFail( "tank", "123abc" );
            assertAuth( "tank", "abc123" );

            // When clearing cache and logging in
            clearAuthCacheFromDifferentConnection();

            // Then
            assertAuthFail( "tank", "abc123" );
            assertAuth( "tank", "123abc" );
        }
    }

    @Test
    public void shouldClearAuthorizationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartServerWithOverriddenSettings( SecuritySettings.ldap_server.name(), "ldaps://localhost:10636" );

            // Then
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                assertReadSucceeds( driver );
                assertWriteSucceeds( driver );
            }

            changeLDAPGroup( "tank", "abc123", "reader" );

            // When logging in without clearing cache
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                // Then
                assertReadSucceeds( driver );
                assertWriteSucceeds( driver );
            }

            // When clearing cache and logging in
            clearAuthCacheFromDifferentConnection();

            // Then
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                assertReadSucceeds( driver );
                assertWriteFails( driver );
            }
        }
    }

    // ===== Helpers =====

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

    @SuppressWarnings( "SameParameterValue" )
    private void changeLDAPPassword( String username, Object credentials, Object newCredentials ) throws Throwable
    {
        modifyLDAPAttribute( username, credentials, "userpassword", newCredentials );
    }

    @SuppressWarnings( "SameParameterValue" )
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

    private class DirectoryServiceWaitOnSearch implements AutoCloseable
    {
        private final Interceptor waitOnSearchInterceptor;

        DirectoryServiceWaitOnSearch( long waitingTimeMillis )
        {
            waitOnSearchInterceptor = new BaseInterceptor()
            {
                @Override
                public String getName()
                {
                    return getClass().getName();
                }

                @Override
                public EntryFilteringCursor search( SearchOperationContext searchContext ) throws LdapException
                {
                    try
                    {
                        Thread.sleep( waitingTimeMillis );
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    return super.search( searchContext );
                }
            };

            try
            {
                getService().addFirst( waitOnSearchInterceptor );
            }
            catch ( LdapException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close()
        {
            getService().remove( waitOnSearchInterceptor.getName() );
        }
    }

    private class DirectoryServiceFailOnSearch implements AutoCloseable
    {
        private final Interceptor failOnSearchInterceptor;

        DirectoryServiceFailOnSearch()
        {
            failOnSearchInterceptor = new BaseInterceptor()
            {
                @Override
                public String getName()
                {
                    return getClass().getName();
                }

                @Override
                public EntryFilteringCursor search( SearchOperationContext searchContext ) throws LdapException
                {
                    throw new LdapOperationErrorException();
                }
            };

            try
            {
                getService().addFirst( failOnSearchInterceptor );
            }
            catch ( LdapException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close()
        {
            getService().remove( failOnSearchInterceptor.getName() );
        }
    }
}
