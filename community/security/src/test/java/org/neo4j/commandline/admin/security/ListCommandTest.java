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

import java.io.File;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ListCommandTest extends CommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailWithoutSubcommand() throws Exception
    {
        UsersCommand usersCommand = new UsersCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( OutsideWorld.class ) );

        String[] arguments = {};
        try
        {
            usersCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "password" ) );
            assertThat( e.getMessage(), containsString( "list" ) );
            assertThat( e.getMessage(), containsString( "create" ) );
            assertThat( e.getMessage(), containsString( "delete" ) );
        }
    }

    @Test
    public void shouldFailWithUnknownSubcommand() throws Exception
    {
        UsersCommand usersCommand = new UsersCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( OutsideWorld.class ) );

        String[] arguments = {"make-love-not-war"};
        try
        {
            usersCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Unknown users command" ) );
        }
    }

    @Test
    public void shouldRunListWithExistingUser() throws Throwable
    {
        // Given - new user that requires password change
        createTestUser( "another", "abc" );

        // When - listing users
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        OutsideWorld out = mock( OutsideWorld.class );
        UsersCommand usersCommand =
                new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
        usersCommand.execute( new String[]{"list"} );

        // Then - the default user and new user are found
        verify( out ).stdOutLine( "another" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );
    }

    @Test
    public void shouldRunListWithExistingUserAndSubstringFilter() throws Throwable
    {
        // Given - new user that requires password change
        createTestUser( "one", "abc" );
        createTestUser( "another", "abc" );

        // When - listing users
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        OutsideWorld out = mock( OutsideWorld.class );
        UsersCommand usersCommand =
                new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
        usersCommand.execute( new String[]{"list", "other"} );

        // Then - the specified user is found
        verify( out ).stdOutLine( "another" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );
    }

}
