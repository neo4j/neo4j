/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.commandline.admin.security;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.commandline.admin.CommandFailed;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DeleteCommandTest extends UsersCommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailToDeleteMissingUser() throws Throwable
    {
        // Given - no existing user

        // When - trying to delete a user
        try
        {
            UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out, fileSystem );
            usersCommand.execute( new String[]{"delete", "another"} );
            fail( "Should not have succeeded without exception" );
        }
        catch ( CommandFailed e )
        {
            // Expect failure message
            assertThat( e.getMessage(), containsString( "User 'another' does not exist" ) );
        }
    }

    @Test
    public void shouldDeleteExistingUser() throws Throwable
    {
        // Given - existing user
        createTestUser( "another", "abc" );

        // When - creating new user
        UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out, fileSystem );
        usersCommand.execute( new String[]{"delete", "another"} );

        // Then - the specified user is found
        verify( out ).stdOutLine( "Deleted user 'another'" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );
    }

}
