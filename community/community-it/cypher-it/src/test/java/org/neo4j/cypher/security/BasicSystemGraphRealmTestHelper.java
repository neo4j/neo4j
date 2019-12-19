/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.security;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public class BasicSystemGraphRealmTestHelper
{
    public static class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager<StandaloneDatabaseContext>
    {
        protected GraphDatabaseFacade testSystemDb;
        protected final DatabaseManagementService managementService;
        private final DatabaseIdRepository.Caching databaseIdRepository = new TestDatabaseIdRepository();

        protected TestDatabaseManager( TestDirectory testDir )
        {
            managementService = createManagementService( testDir );
            testSystemDb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        }

        protected DatabaseManagementService createManagementService( TestDirectory testDir )
        {
            return new TestDatabaseManagementServiceBuilder( testDir.homeDir() ).impermanent()
                    .setConfig( GraphDatabaseSettings.auth_enabled, false ).build();
        }

        public DatabaseManagementService getManagementService()
        {
            return managementService;
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( NamedDatabaseId namedDatabaseId )
        {
            if ( namedDatabaseId.isSystemDatabase() )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new StandaloneDatabaseContext( database ) );
            }
            return Optional.empty();
        }

        @Override
        public StandaloneDatabaseContext createDatabase( NamedDatabaseId namedDatabaseId )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void dropDatabase( NamedDatabaseId namedDatabaseId )
        {
        }

        @Override
        public void stopDatabase( NamedDatabaseId namedDatabaseId )
        {
        }

        @Override
        public void startDatabase( NamedDatabaseId namedDatabaseId )
        {
        }

        @Override
        public DatabaseIdRepository.Caching databaseIdRepository()
        {
            return databaseIdRepository;
        }

        @Override
        public SortedMap<NamedDatabaseId,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }

    private static Map<String,Object> testAuthenticationToken( String username, String password )
    {
        Map<String,Object> authToken = new TreeMap<>();
        authToken.put( AuthToken.PRINCIPAL, username );
        authToken.put( AuthToken.CREDENTIALS, UTF8.encode( password ) );
        authToken.put( AuthToken.SCHEME_KEY, AuthToken.BASIC_SCHEME );
        return authToken;
    }

    public static void assertAuthenticationSucceeds( SystemGraphRealmHelper realmHelper, String username, String password ) throws Exception
    {
        assertAuthenticationSucceeds( realmHelper, username, password, false );
    }

    public static void assertAuthenticationSucceeds( SystemGraphRealmHelper realmHelper, String username, String password, boolean changeRequired )
            throws Exception
    {
        var user = realmHelper.getUser( username );
        assertTrue( user.credentials().matchesPassword( password( password ) ) );
        assertEquals( changeRequired, user.passwordChangeRequired() );
    }

    public static void assertAuthenticationFails( SystemGraphRealmHelper realmHelper, String username, String password ) throws Exception
    {
        var user = realmHelper.getUser( username );
        assertFalse( user.credentials().matchesPassword( password( password ) ) );
    }

    static void assertAuthenticationFailsWithTooManyAttempts( BasicSystemGraphRealm realm, String username, String badPassword, int attempts )
            throws InvalidAuthTokenException
    {
        for ( int i = 0; i < attempts; i++ )
        {
            LoginContext login = realm.login( testAuthenticationToken( username, badPassword ) );
            if ( AuthenticationResult.SUCCESS.equals( login.subject().getAuthenticationResult() ) )
            {
                fail( "Unexpectedly succeeded in logging in" );
            }
            else if ( AuthenticationResult.TOO_MANY_ATTEMPTS.equals( login.subject().getAuthenticationResult() ) )
            {
                // this is what we wanted
                return;
            }
        }
        fail( "Did not get an ExcessiveAttemptsException after " + attempts + " attempts." );
    }

    public static User createUser( String userName, String password, boolean pwdChangeRequired )
    {
        return new User.Builder( userName, credentialFor( password ) )
                .withRequiredPasswordChange( pwdChangeRequired )
                .build();
    }
}
