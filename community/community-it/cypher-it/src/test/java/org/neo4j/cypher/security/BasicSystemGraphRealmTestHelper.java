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

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExcessiveAttemptsException;

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
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.ShiroAuthToken;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class BasicSystemGraphRealmTestHelper
{
    public static class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager<StandaloneDatabaseContext>
    {
        GraphDatabaseFacade testSystemDb;
        private final DatabaseManagementService managementService;
        private final DatabaseIdRepository.Caching databaseIdRepository = new TestDatabaseIdRepository();

        TestDatabaseManager( TestDirectory testDir )
        {
            managementService = new TestDatabaseManagementServiceBuilder( testDir.databaseDir() ).impermanent()
                    .setConfig( GraphDatabaseSettings.auth_enabled, false ).build();
            testSystemDb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        }

        DatabaseManagementService getManagementService()
        {
            return managementService;
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( DatabaseId databaseId )
        {
            if ( databaseId.isSystemDatabase() )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new StandaloneDatabaseContext( database ) );
            }
            return Optional.empty();
        }

        @Override
        public StandaloneDatabaseContext createDatabase( DatabaseId databaseId )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void dropDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public void stopDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public void startDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public DatabaseIdRepository.Caching databaseIdRepository()
        {
            return databaseIdRepository;
        }

        @Override
        public SortedMap<DatabaseId,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }

    public static ShiroAuthToken testAuthenticationToken( String username, String password )
    {
        Map<String,Object> authToken = new TreeMap<>();
        authToken.put( AuthToken.PRINCIPAL, username );
        authToken.put( AuthToken.CREDENTIALS, UTF8.encode( password ) );
        return new ShiroAuthToken( authToken );
    }

    // For simplified testing where username equals password
    public static void assertAuthenticationSucceeds( BasicSystemGraphRealm realm, String username )
    {
        // NOTE: Password is the same as username
        assertAuthenticationSucceeds( realm, username, username );
    }

    public static void assertAuthenticationSucceeds( BasicSystemGraphRealm realm, String username, String password )
    {
        // Try twice to rule out differences if authentication info has been cached or not
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, password ) ) );
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, password ) ) );

        // Also test the non-cached result explicitly
        assertNotNull( realm.doGetAuthenticationInfo( testAuthenticationToken( username, password ) ) );
    }

    static void assertAuthenticationFailsWithTooManyAttempts( BasicSystemGraphRealm realm, String username, int attempts )
    {
        // NOTE: Password is the same as username
        for ( int i = 0; i < attempts; i++ )
        {
            try
            {
                assertNull( realm.getAuthenticationInfo( testAuthenticationToken( username, "wrong_password" ) ) );
            }
            catch ( ExcessiveAttemptsException e )
            {
                // This is what we were really looking for
                return;
            }
            catch ( AuthenticationException e )
            {
                // This is expected
            }
        }
        fail( "Did not get an ExcessiveAttemptsException after " + attempts + " attempts." );
    }
}
