package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;

@SuppressWarnings( "deprecation" )
@RunWith( Parameterized.class )
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
public class AuthIT
{
    private static final Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
    private static EmbeddedTestCertificates embeddedTestCertificates;
    private static final String NONE_USER = "smith";
    private static final String READ_USER = "neo";
    private static final String WRITE_USER = "tank";
    private static final String PROC_USER = "jane";

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public DatabaseRule dbRule = new EnterpriseDatabaseRule()
            .withSetting( GraphDatabaseSettings.auth_enabled, "true" )
            .withSetting( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" )
            .withSetting( SecuritySettings.ldap_authentication_cache_enabled, "true" )
            .withSetting( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" )
            .withSetting( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" )
            .withSetting( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" )
            .withSetting( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin;505=role1" )
            .withSetting( SecuritySettings.procedure_roles, "test.staticReadProcedure:role1" )
            .withSetting( SecuritySettings.ldap_read_timeout, "1s" )
            .withSetting( GraphDatabaseSettings.auth_enabled, "true" )
            .withSetting( new BoltConnector( "bolt" ).type, "BOLT" )
            .withSetting( new BoltConnector( "bolt" ).enabled, "true" )
            .withSetting( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() )
            .withSetting( new BoltConnector( "bolt" ).listen_address, "localhost:0" )
            .startLazily();

    @ClassRule
    public static CreateLdapServerRule ldapServerRule = new CreateLdapServerRule();

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> configurations()
    {
        return Arrays.asList( new Object[][]{
                {"Ldap", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldaps", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldaps://0.0.0.0:10636",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"StartTLS", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://localhost:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_use_starttls, "true"
                        )
                },
                {"LdapSystemAccount", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldaps SystemAccount", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldaps://0.0.0.0:10636",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"StartTLS SystemAccount", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://localhost:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system",
                                SecuritySettings.ldap_use_starttls, "true"
                        )
                },
                {"Ldap authn cache disabled", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_cache_enabled, "false"
                        )
                },
                {"Ldap Digest MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "DIGEST-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap Cram MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "CRAM-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap authn Native authz", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "false",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap with Native authn", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap with Native authz", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap and Native", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Native with unresponsive ldap", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://127.0.0.1:10389",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Native", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true"
                        )
                }
        } );
    }

    private final String password;
    private final Map<Setting<?>,String> configMap;
    private final boolean confidentialityRequired;
    private ConnectorPortRegister connectorRegister;

    @SuppressWarnings( "unused" )
    public AuthIT( String suiteName, String password, boolean confidentialityRequired, List<Object> settings )
    {
        this.password = password;
        this.confidentialityRequired = confidentialityRequired;
        this.configMap = new HashMap<>();
        for ( int i = 0; i < settings.size() - 1; i++ )
        {
            Setting setting = (Setting) settings.get( i );
            String value = (String) settings.get( ++i );
            configMap.put( setting, value );
        }
    }

    @BeforeClass
    public static void classSetup()
    {
        embeddedTestCertificates = new EmbeddedTestCertificates();
    }

    @Before
    public void setup() throws IOException, InvalidArgumentsException, KernelException
    {
        ldapServerRule.getLdapServer().setConfidentialityRequired( confidentialityRequired );

        dbRule.withSettings( configMap );
        dbRule.ensureStarted();
        connectorRegister = dbRule.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );

        DependencyResolver dependencyResolver = dbRule.getDependencyResolver();
        EnterpriseAuthAndUserManager authManager = dependencyResolver.resolveDependency( EnterpriseAuthAndUserManager.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        if ( userManager != null )
        {
            userManager.newUser( NONE_USER, password.getBytes(), false );
            userManager.newUser( PROC_USER, password.getBytes(), false );
            userManager.newUser( READ_USER, password.getBytes(), false );
            userManager.newUser( WRITE_USER, password.getBytes(), false );
            userManager.addRoleToUser( PredefinedRoles.READER, READ_USER );
            userManager.addRoleToUser( PredefinedRoles.PUBLISHER, WRITE_USER );
            userManager.newRole( "role1", PROC_USER );
        }
        dependencyResolver.resolveDependency( Procedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );
    }

    @AfterClass
    public static void classTeardown()
    {
        if ( embeddedTestCertificates != null )
        {
            embeddedTestCertificates.close();
        }
    }

    @Test
    public void shouldLoginWithCorrectInformation()
    {
        assertAuth( READ_USER, password );
        assertAuth( READ_USER, password );
    }

    @Test
    public void shouldFailLoginWithIncorrectCredentials()
    {
        assertAuthFail( READ_USER, "WRONG" );
        assertAuthFail( READ_USER, "ALSO WRONG" );
    }

    @Test
    public void shouldFailLoginWithInvalidCredentialsFollowingSuccessfulLogin()
    {
        assertAuth( READ_USER, password );
        assertAuthFail( READ_USER, "WRONG" );
    }

    @Test
    public void shouldGetCorrectAuthorizationNoPermission()
    {
        Driver driver = connectDriver( NONE_USER, password );
        assertReadFails( driver );
        assertWriteFails( driver );
        driver.close();
    }

    @Test
    public void shouldGetCorrectAuthorizationReaderUser()
    {
        Driver driver = connectDriver( READ_USER, password );
        assertReadSucceeds( driver );
        assertWriteFails( driver );
        driver.close();
    }

    @Test
    public void shouldGetCorrectAuthorizationWriteUser()
    {
        Driver driver = connectDriver( WRITE_USER, password );
        assertReadSucceeds( driver );
        assertWriteSucceeds( driver );
        driver.close();
    }

    @Test
    public void shouldGetCorrectAuthorizationAllowedProcedure()
    {
        Driver driver = connectDriver( PROC_USER, password );
        assertProcSucceeds( driver );
        assertWriteFails( driver );
        driver.close();
    }

    // ======= Helpers

    @SuppressWarnings( "SameParameterValue" )
    private void assertAuth( String username, String password )
    {
        try ( Driver driver = connectDriver( username, password );
                Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private void assertAuthFail( String username, String password )
    {
        try ( Driver ignored = connectDriver( username, password ) )
        {
            fail( "Should not have authenticated" );
        }
        catch ( AuthenticationException e )
        {
            assertThat( e.code(), equalTo( "Neo.ClientError.Security.Unauthorized" ) );
        }
    }

    private void assertReadSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            assertThat( single.asLong(), greaterThanOrEqualTo( 0L ) );
        }
    }

    private void assertReadFails( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            fail( "Should not be allowed read operation" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "Read operations are not allowed for user " ) );
        }
    }

    private void assertWriteSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            StatementResult result = session.run( "CREATE ()" );
            assertThat( result.summary().counters().nodesCreated(), equalTo( 1 ) );
        }
    }

    private void assertWriteFails( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE ()" ).consume();
            fail( "Should not be allowed write operation" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "Write operations are not allowed for user " ) );
        }
    }

    private void assertProcSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "CALL test.staticReadProcedure()" ).single().get( 0 );
            assertThat( single.asString(), equalTo( "static" ) );
        }
    }

    private Driver connectDriver( String username, String password )
    {
        return GraphDatabase.driver( "bolt://" + connectorRegister.getLocalAddress( "bolt" ).toString(), AuthTokens.basic( username, password ), config );
    }

    //-------------------------------------------------------------------------
    // TLS helper
    private static class EmbeddedTestCertificates implements AutoCloseable
    {
        private static final String KEY_STORE = "javax.net.ssl.keyStore";
        private static final String KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
        private static final String TRUST_STORE = "javax.net.ssl.trustStore";
        private static final String TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

        private final String keyStore = System.getProperty( KEY_STORE );
        private final String keyStorePassword = System.getProperty( KEY_STORE_PASSWORD );
        private final String trustStore = System.getProperty( TRUST_STORE );
        private final String trustStorePassword = System.getProperty( TRUST_STORE_PASSWORD );

        EmbeddedTestCertificates()
        {
            URL url = getClass().getResource( "/neo4j_ldap_test_keystore.jks" );
            File keyStoreFile = new File( url.getFile() );
            String keyStorePath = keyStoreFile.getAbsolutePath();

            System.setProperty( KEY_STORE, keyStorePath );
            System.setProperty( KEY_STORE_PASSWORD, "secret" );
            System.setProperty( TRUST_STORE, keyStorePath );
            System.setProperty( TRUST_STORE_PASSWORD, "secret" );
        }

        @Override
        public void close()
        {
            resetProperty( KEY_STORE, keyStore );
            resetProperty( KEY_STORE_PASSWORD, keyStorePassword );
            resetProperty( TRUST_STORE, trustStore );
            resetProperty( TRUST_STORE_PASSWORD, trustStorePassword );
        }

        private void resetProperty( String property, String value )
        {
            if ( value == null )
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
