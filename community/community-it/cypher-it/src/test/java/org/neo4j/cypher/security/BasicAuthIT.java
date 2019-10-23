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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BasicAuthIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    private DbmsRule dbRule = new EmbeddedDbmsRule().startLazily().withSetting( GraphDatabaseSettings.auth_enabled, true );

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    @Test
    public void shouldCreateUserWithAuthDisabled() throws Exception
    {
        // GIVEN
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, false );
        dbRule.ensureStarted();

        GraphDatabaseService database = dbRule.getManagementService().database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            // WHEN
            tx.execute( "CREATE USER foo SET PASSWORD 'bar'" ).close();
            tx.commit();
        }
        dbRule.restartDatabase( Map.of( GraphDatabaseSettings.auth_enabled, true ) );

        // THEN
        AuthManager authManager = dbRule.resolveDependency( AuthManager.class );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "foo", "wrong" ) );
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
        loginContext = authManager.login( AuthToken.newBasicAuthToken( "foo", "bar" ) );
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }
}
