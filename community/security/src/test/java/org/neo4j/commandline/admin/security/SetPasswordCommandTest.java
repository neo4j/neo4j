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
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SetPasswordCommandTest
{
    private TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private static String password_change_required = "password_change_required";

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
    public void shouldRunSetPasswordCommandWithExistinguser() throws Throwable
    {
        // Given - new user that requires password change
        File graphDir = testDir.graphDbDir();
        createTestUser("neo4j", "neo4j");
        assertUserRequiresPasswordChange( "neo4j" );

        // When - the admin command sets the password
        File confDir = new File( graphDir, "conf" );
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( graphDir.toPath(), confDir.toPath() );
        setPasswordCommand.execute( new String[]{"neo4j", "abc"} );

        // Then - the new user no longer requires a password change
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldRunSetPasswordCommandWithoutExistingUser() throws Throwable
    {
        // Given - no user
        File graphDir = testDir.graphDbDir();

        // When - the admin command sets the password
        File confDir = new File( graphDir, "conf" );
        SetPasswordCommand setPasswordCommand = new SetPasswordCommand( graphDir.toPath(), confDir.toPath() );
        setPasswordCommand.execute( new String[]{"neo4j", "abc", "--create"} );

        // Then - the new user no longer requires a password change
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldFailToRunSetPasswordCommandWithoutExistingUser() throws Throwable
    {
        // Given - no user
        File graphDir = testDir.graphDbDir();

        // When - the admin command sets the password
        try
        {
            File confDir = new File( graphDir, "conf" );
            SetPasswordCommand setPasswordCommand = new SetPasswordCommand( graphDir.toPath(), confDir.toPath() );
            setPasswordCommand.execute( new String[]{"neo4j", "abc"} );
        }
        catch ( CommandFailed e )
        {
            // Then we get an error
            assertThat( e.getMessage(), containsString( "does not exist" ) );
        }
    }

    @Test
    public void shouldRunAdminToolWithSetPasswordCommandAndNoArgs() throws Throwable
    {
        // Given a user that requires password change
        createTestUser("neo4j", "neo4j");
        assertUserRequiresPasswordChange( "neo4j" );

        // When running the neo4j-admin tool with incorrect parameters
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        tool.execute( homeDir, configDir, "set-password" );

        // Then we get error output and user still requires password change
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).stdErrLine( "neo4j-admin set-password --create <username> <password>" );
        verify( out ).stdErrLine( "    Sets the password for the specified user and removes the password change " );
        verify( out ).stdErrLine( "    requirement" );
        verify( out ).stdErrLine( "Missing arguments: expected username and password" );
        assertUserRequiresPasswordChange( "neo4j" );
    }

    @Test
    public void shouldRunAdminToolWithSetPasswordCommandAndArgsButNoUser() throws Throwable
    {
        // Given no existing user

        // When running the neo4j-admin tool without --create parameter
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        tool.execute( homeDir, configDir, "set-password", "neo4j", "abc" );

        // Then we get error output and user still requires password change
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).stdErrLine( "command failed: Failed to set password for 'neo4j': User 'neo4j' does not exist" );
    }

    @Test
    public void shouldRunAdminToolWithSetPasswordCommandAndArgsButNoUserAndCreateFalse() throws Throwable
    {
        // Given no existing user

        // When running the neo4j-admin tool without --create parameter
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        tool.execute( homeDir, configDir, "set-password", "neo4j", "abc", "--create=false" );

        // Then we get error output and user still requires password change
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).stdErrLine( "command failed: Failed to set password for 'neo4j': User 'neo4j' does not exist" );
    }

    @Test
    public void shouldRunAdminToolWithSetPasswordCommandAndExistingUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser("neo4j", "neo4j");
        assertUserRequiresPasswordChange( "neo4j" );

        // When running the neo4j-admin tool with correct parameters
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        tool.execute( homeDir, configDir, "set-password", "neo4j", "abc" );

        // Then we get no error output and the user no longer requires password change
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out, times( 0 ) ).stdErrLine( anyString() );
        verify( out ).exit(0);
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldRunAdminToolWithSetPasswordCommandAndNoExistingUser() throws Throwable
    {
        // Given no previously existing user

        // When running the neo4j-admin tool with correct parameters
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        tool.execute( homeDir, configDir, "set-password", "--create=true", "neo4j", "abc" );

        // Then we get no error output and the user no longer requires password change
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out, times( 0 ) ).stdErrLine( anyString() );
        verify( out ).exit(0);
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    private File authFile()
    {
        return new File( new File( new File( testDir.graphDbDir(), "data" ), "dbms" ), "auth.db" );
    }

    private User createTestUser(String username, String password) throws IOException, InvalidArgumentsException
    {
        FileUserRepository users = new FileUserRepository( authFile().toPath(), NullLogProvider.getInstance() );
        User user = new User.Builder( username, Credential.forPassword( password ) ).withRequiredPasswordChange( true )
                .build();
        users.create( user );
        return user;
    }

    private User getUser(String username) throws Throwable
    {
        FileUserRepository afterUsers = new FileUserRepository( authFile().toPath(), NullLogProvider.getInstance() );
        afterUsers.start(); // load users from disk
        return afterUsers.getUserByName( username );
    }

    private void assertUserRequiresPasswordChange(String username) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should require password change", user.getFlags(), hasItem( password_change_required ) );

    }

    private void assertUserDoesNotRequirePasswordChange(String username) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should not require password change", user.getFlags(),
                not( hasItem( password_change_required ) ) );

    }
}
