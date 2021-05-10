/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.security;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.ComponentVersion.SECURITY_USER_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

@TestDirectoryExtension
@TestInstance( PER_CLASS )
class UserSecurityGraphComponentTest
{
    @Inject
    @SuppressWarnings( "unused" )
    private static TestDirectory directory;

    private static DatabaseManagementService dbms;
    private static GraphDatabaseFacade system;
    private static SystemGraphComponents systemGraphComponents;
    private static UserSecurityGraphComponent userSecurityGraphComponent;
    private static AuthManager authManager;

    @BeforeAll
    static void setup() throws IOException, InvalidArgumentsException
    {
        Config cfg = Config.newBuilder()
                           .set( auth_enabled, TRUE )
                           .set( allow_single_automatic_upgrade, FALSE )
                           .build();

        dbms = new TestDatabaseManagementServiceBuilder( directory.homePath() )
                .impermanent()
                .setConfig( cfg )
                .noOpSystemGraphInitializer()
                .build();
        system = (GraphDatabaseFacade) dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver resolver = system.getDependencyResolver();
        systemGraphComponents = resolver.resolveDependency( SystemGraphComponents.class );
        authManager = resolver.resolveDependency( AuthManager.class );

        // Insert a custom SecurityUserComponent instead of the default one,
        // in order to have a handle on it and to migrate a 3.5 user
        systemGraphComponents.deregister( SECURITY_USER_COMPONENT );
        UserRepository oldUsers = new InMemoryUserRepository();
        User oldUser = new User.Builder( "alice", credentialFor( "secret" ) )
                .withRequiredPasswordChange( false )
                .build();

        oldUsers.create( oldUser );
        UserRepository initialPassword = new InMemoryUserRepository();
        userSecurityGraphComponent =
                new UserSecurityGraphComponent( CommunitySecurityLog.NULL_LOG, oldUsers, initialPassword, Config.defaults() );
        systemGraphComponents.register( userSecurityGraphComponent );

        // remove DBMS runtime component as it is not a subject of this test
        systemGraphComponents.deregister( DBMS_RUNTIME_COMPONENT );
    }

    @BeforeEach
    void clear() throws Exception
    {
        inTx( tx -> tx.getAllNodes().stream().forEach( n ->
        {
            n.getRelationships().forEach( Relationship::delete );
            n.delete();
        } ) );
    }

    @AfterAll
    static void tearDown()
    {
        dbms.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldAuthenticate( UserSecurityGraphComponentVersion version ) throws Exception
    {
        initializeLatestSystem();
        initUserSecurityComponent( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ),  EMBEDDED_CONNECTION);
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldInitializeDefaultVersion() throws Exception
    {
        systemGraphComponents.initializeSystemGraph( system );

        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.componentName(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( CURRENT ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( CURRENT ) );
    }

    @ParameterizedTest
    @MethodSource( "versionAndStatusProvider" )
    void shouldInitializeAndUpgradeSystemGraph( UserSecurityGraphComponentVersion version, SystemGraphComponent.Status initialStatus ) throws Exception
    {
        initializeLatestSystem();
        initUserSecurityComponent( version );
        assertCanUpgradeThisVersionAndThenUpgradeIt( initialStatus );
    }

    static Stream<Arguments> versionAndStatusProvider()
    {
        return Stream.of(
                Arguments.arguments( UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_35, UNSUPPORTED_BUT_CAN_UPGRADE ),
                Arguments.arguments( UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_40, REQUIRES_UPGRADE ),
                Arguments.arguments( UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_41, REQUIRES_UPGRADE ),
                Arguments.arguments( UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_43D4, CURRENT )
        );
    }

    @Test
    void shouldAddUserOnUpgradeFrom3_5() throws Exception
    {
        // Given
        initializeLatestSystem();
        initUserSecurityComponent( UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_35 );

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent( system );

        // Then
        HashMap<String, Object> usernameAndIds = getUserNamesAndIds();
        assertThat( usernameAndIds.get( "alice" ), notNullValue() );
    }

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldAddUserIdsOnUpgradeFromOlderSystemDb( UserSecurityGraphComponentVersion version ) throws Exception
    {
        // Given
        initializeLatestSystem();
        initUserSecurityComponent( version );

        createUser( version, "alice" );

        // Then
        HashMap<String, Object> usernameAndIdsBeforeUpgrade = getUserNamesAndIds();
        assertThat( usernameAndIdsBeforeUpgrade.get( "neo4j" ), equalTo(null) );
        assertThat( usernameAndIdsBeforeUpgrade.get( "alice" ), equalTo(null) );

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent( system );

        // Then
        HashMap<String, Object> usernameAndIdsAfterUpgrade = getUserNamesAndIds();

        assertThat( usernameAndIdsAfterUpgrade.get( "neo4j" ), notNullValue() );
        assertThat( usernameAndIdsAfterUpgrade.get( "alice" ), notNullValue() );
    }

    private static Stream<Arguments> supportedPreviousVersions()
    {
        return Arrays.stream( UserSecurityGraphComponentVersion.values() )
                  .filter( version -> version.runtimeSupported() &&
                                      version.getVersion() < UserSecurityGraphComponentVersion.LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION )
                .map( Arguments::of );
    }

    private static void assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status initialState ) throws Exception
    {
        var systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );
        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", initialState,
                "dbms-status", initialState
        ) );

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent( system );

        // Then when looking at component statuses
        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", CURRENT,
                "dbms-status", CURRENT
        ) );
    }

    private static void assertStatus( Map<String,SystemGraphComponent.Status> expected ) throws Exception
    {
        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.componentName(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        for ( var entry : expected.entrySet() )
        {
            assertThat( entry.getKey(), statuses.get( entry.getKey() ), is( entry.getValue() ) );
        }
    }

    private HashMap<String,Object> getUserNamesAndIds()
    {
        HashMap<String, Object> usernameAndIds = new HashMap<>();

        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
             ResourceIterator<Node> nodes = tx.findNodes( USER_LABEL );
             while ( nodes.hasNext() )
             {
                 Node userNode = nodes.next();
                 String username = userNode.getProperty( "name" ).toString();
                 Object userId;
                 try
                 {
                    userId = userNode.getProperty( "id" );
                 }
                 catch ( NotFoundException e )
                 {
                     userId = null;
                 }
                 usernameAndIds.put( username, userId );
             }
        }
        return usernameAndIds;
    }

    private void createUser( UserSecurityGraphComponentVersion version, String name )
    {
        KnownCommunitySecurityComponentVersion builder = userSecurityGraphComponent.findSecurityGraphComponentVersion( version );
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            builder.addUser(tx, name, credentialFor( "abc123" ), false, false  );
            tx.commit();
        }
    }

    private static void initializeLatestSystem() throws Exception
    {
        var systemGraphComponent = new DefaultSystemGraphComponent( Config.defaults() );
        systemGraphComponent.initializeSystemGraph( system, true );
    }

    private static void initUserSecurityComponent( UserSecurityGraphComponentVersion version ) throws Exception
    {
        KnownCommunitySecurityComponentVersion builder = userSecurityGraphComponent.findSecurityGraphComponentVersion( version );
        inTx( tx -> userSecurityGraphComponent.initializeSystemGraphConstraints( tx ) );

        switch ( version )
        {
        case COMMUNITY_SECURITY_40:
            inTx( builder::setupUsers );
            break;
        case COMMUNITY_SECURITY_41:
        case COMMUNITY_SECURITY_43D4:
            inTx( builder::setupUsers );
            inTx( tx -> builder.setVersionProperty( tx, version.getVersion() ) );
            break;
        default:
            break;
        }
    }

    private static void inTx( ThrowingConsumer<Transaction,Exception> consumer ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            consumer.accept( tx );
            tx.commit();
        }
    }
}
