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

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.test.rule.TargetDirectory;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SetPasswordCommandTest
{
    private TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailWithNoArguments() throws Exception
    {
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "password" ) );
        }
    }

    @Test
    public void shouldFailOnOnlyOneArgument() throws Exception
    {
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {"neo4j"};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "password" ) );
        }
    }

    @Test
    public void shouldFailWitNonExistingUser() throws Exception
    {
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {"nosuchuser", "whatever"};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), containsString( "does not exist" ) );
        }
    }

    @Test
    public void shouldRunSetPasswordCommand() throws Throwable
    {
        // Given - new user that requires password change
        String password_change_required = "password_change_required";
        File graphDir = testDir.graphDbDir();
        File authFile = new File( new File( new File( graphDir, "data" ), "dbms" ), "auth.db" );
        FileUserRepository beforeUsers = new FileUserRepository( authFile.toPath(), NullLogProvider.getInstance() );
        User before = new User.Builder( "neo4j", Credential.forPassword( "neo4j" ) ).withRequiredPasswordChange( true )
                .build();
        beforeUsers.create( before );
        assertThat( "User should require password change", before.getFlags(), hasItem( password_change_required ) );

        // When - the admin command sets the password
        File confDir = new File( graphDir, "conf" );
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( graphDir.toPath(), confDir.toPath() );
        setPasswordCommand.execute( new String[]{"neo4j", "abc"} );

        // Then - the new user no longer requires a password change
        FileUserRepository afterUsers = new FileUserRepository( authFile.toPath(), NullLogProvider.getInstance() );
        afterUsers.start(); // load users from disk
        User after = afterUsers.getUserByName( "neo4j" );
        assertThat( "User should require password change", after.getFlags(),
                not( hasItem( password_change_required ) ) );
    }

}
