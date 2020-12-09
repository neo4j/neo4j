/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLog;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_35;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_40;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_41;
import static org.neo4j.dbms.database.ComponentVersion.SECURITY_USER_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

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

    @BeforeAll
    static void setup() throws IOException, InvalidArgumentsException
    {
        dbms = new TestDatabaseManagementServiceBuilder( directory.homePath() ).impermanent().noOpSystemGraphInitializer().build();
        system = (GraphDatabaseFacade) dbms.database( SYSTEM_DATABASE_NAME );
        systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );

        // Insert a custom SecurityUserComponent instead of the default one,
        // in order to have a handle on it and to migrate a 3.5 user
        systemGraphComponents.deregister( SECURITY_USER_COMPONENT );
        UserRepository oldUsers = new InMemoryUserRepository();
        User oldUser = new User.Builder( "alice", credentialFor( "secret" ) )
                .withRequiredPasswordChange( false )
                .build();

        oldUsers.create( oldUser );
        UserRepository initialPassword = new InMemoryUserRepository();
        userSecurityGraphComponent = new UserSecurityGraphComponent( NullLog.getInstance(), oldUsers, initialPassword, Config.defaults() );
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
        assertThat( "Expecting four components", statuses.size(), is( 3 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( CURRENT ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( CURRENT ) );
    }

    @ParameterizedTest
    @MethodSource( "versionAndStatusProvider" )
    void shouldInitializeAndUpgradeSystemGraph( String version, SystemGraphComponent.Status initialStatus ) throws Exception
    {
        initializeLatestSystem();
        initUserSecurityComponent( version );
        assertCanUpgradeThisVersionAndThenUpgradeIt( initialStatus );
    }

    static Stream<Arguments> versionAndStatusProvider()
    {
        return Stream.of(
                Arguments.arguments( VERSION_35, UNSUPPORTED_BUT_CAN_UPGRADE ),
                Arguments.arguments( VERSION_40, REQUIRES_UPGRADE ),
                Arguments.arguments( VERSION_41, CURRENT )
        );
    }

    private void assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status initialState ) throws Exception
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

    private void assertStatus( Map<String,SystemGraphComponent.Status> expected ) throws Exception
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

    private void initializeLatestSystem() throws Exception
    {
        var systemGraphComponent = new DefaultSystemGraphComponent( Config.defaults() );
        systemGraphComponent.initializeSystemGraph( system, true );
    }

    private void initUserSecurityComponent( String version ) throws Exception
    {
        KnownCommunitySecurityComponentVersion builder = userSecurityGraphComponent.findSecurityGraphComponentVersion( version );
        inTx( tx -> userSecurityGraphComponent.initializeSystemGraphConstraints( tx ) );

        switch ( version )
        {
        case VERSION_40:
            inTx( builder::setupUsers );
            break;
        case VERSION_41:
            inTx( builder::setupUsers );
            inTx( tx -> builder.setVersionProperty( tx, 2 ) );
            break;
        default:
            break;
        }
    }

    private void inTx( ThrowingConsumer<Transaction,Exception> consumer ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            consumer.accept( tx );
            tx.commit();
        }
    }
}
