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

import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.Map;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;

public abstract class EnterpriseAuthenticationTestBase extends AbstractLdapTestUnit
{
    private static final Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();

    private TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    protected DatabaseRule dbRule = getDatabaseTestRule( testDirectory );

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    @Before
    public void setup() throws Exception
    {
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, "true" )
              .withSetting( new BoltConnector( "bolt" ).type, "BOLT" )
              .withSetting( new BoltConnector( "bolt" ).enabled, "true" )
              .withSetting( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() )
              .withSetting( new BoltConnector( "bolt" ).listen_address, "localhost:0" );
        dbRule.withSettings( getSettings() );
        dbRule.ensureStarted();
        dbRule.resolveDependency( Procedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );
    }

    protected abstract Map<Setting<?>,String> getSettings();

    protected DatabaseRule getDatabaseTestRule( TestDirectory testDirectory )
    {
        return new EnterpriseDatabaseRule( testDirectory ).startLazily();
    }

    void restartServerWithOverriddenSettings( String... configChanges ) throws IOException
    {
        dbRule.restartDatabase( configChanges );
    }

    void assertAuth( String username, String password )
    {
        assertAuth( username, password, null );
    }

    void assertAuth( String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( username, password, realm );
                Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), CoreMatchers.equalTo( 1L ) );
        }
    }

    void assertAuth( AuthToken authToken )
    {
        try ( Driver driver = connectDriver( authToken );
                Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), CoreMatchers.equalTo( 1L ) );
        }
    }

    void assertAuthFail( String username, String password )
    {
        assertAuthFail( username, password, null );
    }

    void assertAuthFail( String username, String password, String realm )
    {
        try ( Driver ignored = connectDriver( username, password, realm ) )
        {
            fail( "Should not have authenticated" );
        }
        catch ( AuthenticationException e )
        {
            assertThat( e.code(), CoreMatchers.equalTo( "Neo.ClientError.Security.Unauthorized" ) );
        }
    }

    void assertReadSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            assertThat( single.asLong(), Matchers.greaterThanOrEqualTo( 0L ) );
        }
    }

    void assertReadFails( String username, String password )
    {
        try ( Driver driver = connectDriver( username, password ) )
        {
            assertReadFails( driver );
        }
    }

    void assertReadFails( Driver driver )
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

    void assertWriteSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            StatementResult result = session.run( "CREATE ()" );
            assertThat( result.summary().counters().nodesCreated(), CoreMatchers.equalTo( 1 ) );
        }
    }

    void assertWriteFails( Driver driver )
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

    void assertProcSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "CALL test.staticReadProcedure()" ).single().get( 0 );
            assertThat( single.asString(), CoreMatchers.equalTo( "static" ) );
        }
    }

    void assertAuthorizationExpired( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "MATCH (n) RETURN n" ).single();
            fail( "should have gotten authorization expired exception" );
        }
        catch ( ServiceUnavailableException e )
        {
            // TODO Bolt should handle the AuthorizationExpiredException better
            //assertThat( e.getMessage(), equalTo( "Plugin 'plugin-TestCombinedAuthPlugin' authorization info expired: " +
            //        "authorization_expired_user needs to re-authenticate." ) );
        }
    }

    void clearAuthCacheFromDifferentConnection()
    {
        clearAuthCacheFromDifferentConnection( "neo4j", "abc123", null );
    }

    void clearAuthCacheFromDifferentConnection( String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( username, password, realm );
                Session session = driver.session() )
        {
            session.run( "CALL dbms.security.clearAuthCache()" );
        }
    }

    Driver connectDriver( String username, String password )
    {
        return connectDriver( username, password, null );
    }

    Driver connectDriver( String username, String password, String realm )
    {
        AuthToken token;
        if ( realm == null || realm.isEmpty() )
        {
            token = AuthTokens.basic( username, password );
        }
        else
        {
            token = AuthTokens.basic( username, password, realm );
        }
        return connectDriver( token );
    }

    private Driver connectDriver( AuthToken token )
    {
        return GraphDatabase.driver( "bolt://" + dbRule.resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" ).toString(), token, config );
    }

    void assertRoles( Driver driver, String... roles )
    {
        try ( Session session = driver.session() )
        {
            Record record = session.run( "CALL dbms.showCurrentUser() YIELD roles" ).single();
            assertThat( record.get( "roles" ).asList(), containsInAnyOrder( roles ) );
        }
    }

    void assertSecurityLogContains( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        File workingDirectory = testDirectory.directory();
        File logFile = new File( workingDirectory, "logs/security.log" );

        Reader reader = fileSystem.openAsReader( logFile, UTF_8 );
        BufferedReader bufferedReader = new BufferedReader( reader );
        String line;
        boolean foundError = false;

        while ( (line = bufferedReader.readLine()) != null )
        {
            if ( line.contains( message ) )
            {
                foundError = true;
            }
        }
        bufferedReader.close();
        reader.close();

        assertThat( "Security log should contain message '" + message + "'", foundError );
    }

    void assertSecurityLogDoesNotContain( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        File workingDirectory = testDirectory.directory();
        File logFile = new File( workingDirectory, "logs/security.log" );

        Reader reader = fileSystem.openAsReader( logFile, UTF_8 );
        BufferedReader bufferedReader = new BufferedReader( reader );
        String line;

        while ( (line = bufferedReader.readLine()) != null )
        {
            assertThat( "Security log should not contain message '" + message + "'",
                    !line.contains( message ) );
        }
        bufferedReader.close();
        reader.close();
    }

    void createNativeUser( String username, String password, String... roles ) throws IOException, InvalidArgumentsException
    {
        EnterpriseAuthAndUserManager authManager =
                dbRule.resolveDependency( EnterpriseAuthAndUserManager.class );

        authManager.getUserManager( AuthSubject.AUTH_DISABLED, true )
                .newUser( username, password( password ), false );

        for ( String role : roles )
        {
            authManager.getUserManager( AuthSubject.AUTH_DISABLED, true )
                    .addRoleToUser( role, username );
        }
    }

    //-------------------------------------------------------------------------
    // TLS helper
    static class EmbeddedTestCertificates implements AutoCloseable
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
