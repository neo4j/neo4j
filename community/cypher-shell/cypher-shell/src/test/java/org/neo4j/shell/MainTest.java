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
package org.neo4j.shell;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.test.AssertableMain;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.Main.EXIT_SUCCESS;

class MainTest
{
    private CypherShell mockShell;
    private ShellRunner.Factory mockRunnerFactory;

    private AuthenticationException authException = new AuthenticationException( Main.NEO_CLIENT_ERROR_SECURITY_UNAUTHORIZED, "BOOM" );
    private Neo4jException passwordChangeRequiredException = new SecurityException( "Neo.ClientError.Security.CredentialsExpired", "BLAM" );

    @BeforeEach
    void setup() throws IOException
    {
        mockShell = mock( CypherShell.class );
        mockRunnerFactory = mock( ShellRunner.Factory.class );
        var runnerMock = mock( ShellRunner.class );
        when( runnerMock.runUntilEnd() ).thenReturn( EXIT_SUCCESS );
        when( mockRunnerFactory.create( any(), any(), any(), any(), any(), anyBoolean() ) ).thenReturn( runnerMock );
    }

    @Test
    void nonEndedStringFails() throws Exception
    {
        testWithMockUser( "random", "bla" )
            .userInput( "no newline" )
            .run()
            .assertFailure( "No text could be read, exiting..." )
            .assertThatOutput( equalTo( "username: no newline" ) );
    }

    @Test
    void unrelatedErrorDoesNotPrompt() throws Exception
    {
        doThrow( new RuntimeException( "bla" ) ).when( mockShell ).connect( any(), any() );

        testWithMocks().run().assertFailure( "bla" ).assertThatOutput( equalTo( "" ) );

        verify( mockShell, times( 1 ) ).connect( any(), any() );
    }

    @Test
    void promptsForUsernameAndPasswordIfNoneGivenIfInteractive() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .interactive( true, true )
            .userInputLines( "bob", "secret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: bob", "password: ******" );
    }

    @Test
    void promptsSilentlyForUsernameAndPasswordIfNoneGivenIfOutputRedirected() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .interactive( true, false )
            .userInputLines( "bob", "secret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: bob", "password: ******" );
    }

    @Test
    void doesNotPromptIfInputRedirected() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .interactive( false, true )
            .userInputLines( "bob", "secret" )
            .run()
            .assertFailure( authException.getMessage() )
            .assertOutputLines();
    }

    @Test
    void promptsForUserIfPassExistsIfInteractive() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .args( "-p secret" )
            .interactive( true, true )
            .userInputLines( "bob", "secret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: bob" );

        verify( mockShell, times( 2 ) ).connect( any(), any() );
    }

    @Test
    void promptsSilentlyForUserIfPassExistsIfOutputRedirected() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .args( "-p secret" )
            .interactive( true, false )
            .userInputLines( "bob" )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: bob" );

        verify( mockShell, times( 2 ) ).connect( any(), any() );
    }

    @Test
    void promptsForPassBeforeConnectIfUserExistsIfInteractive() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .args( "-u bob" )
            .interactive( true, true )
            .userInputLines( "secret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "password: ******" );

        verify( mockShell, times( 1 ) ).connect( any(), any() );
    }

    @Test
    void promptsSilentlyForPassIfUserExistsIfOutputRedirected() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .args( "-u bob" )
            .interactive( true, false )
            .userInputLines( "secret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "password: ******" );

        verify( mockShell, times( 1 ) ).connect( any(), any() );
    }

    @Test
    void promptsForNewPasswordIfPasswordChangeRequiredCannotBeEmpty() throws Exception
    {
        testWithMockUser( "expired_bob", "newpassword", "oldpassword" )
            .interactive( true, true )
            .userInputLines( "expired_bob", "oldpassword", "", "newpassword" )
            .run()
            .assertSuccess()
            .assertOutputLines(
                "username: expired_bob", "password: ***********", "Password change required", "new password: ",
                "new password cannot be empty", "", "new password: ***********"
            );

        verify( mockShell, times( 3 ) ).connect( any(), any() );
    }

    @Test
    void promptsHandlesBang() throws Exception
    {
        testWithMockUser( "bo!b", "sec!ret" )
            .interactive( true, true )
            .userInputLines( "bo!b", "sec!ret" )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: bo!b", "password: *******" );

        verify( mockShell, times( 2 ) ).connect( any(), any() );
    }

    @Test
    void triesOnlyOnceIfUserPassExists() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .args( "-u bob -p wrongpass" )
            .interactive( true, true )
            .run()
            .assertFailure( authException.getMessage() )
            .assertOutputLines();

        verify( mockShell, times( 1 ) ).connect( any(), any() );
    }

    @Test
    void repromptsIfUserIsNotProvidedIfInteractive() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .userInputLines( "", "bob", "secret" )
            .interactive( true, true )
            .run()
            .assertSuccess()
            .assertOutputLines( "username: ", "username cannot be empty", "", "username: bob", "password: ******" );

        verify( mockShell, times( 2 ) ).connect( any(), any() );
    }

    @Test
    void doesNotRepromptIfUserIsNotProvidedIfOutputRedirected() throws Exception
    {
        testWithMockUser( "bob", "secret" )
            .userInputLines( "", "secret" )
            .interactive( true, false )
            .run()
            .assertFailure( authException.getMessage() )
            .assertOutputLines( "username: ", "password: ******" );

        verify( mockShell, times( 2 ) ).connect( any(), any() );
    }

    @Test
    void printsVersionAndExits() throws ArgumentParserException
    {
        var result = testWithMocks().args( "--version" ).run();

        result.assertSuccess();
        assertTrue( result.getOutput().toString( UTF_8 ).matches( "Cypher-Shell \\d+\\.\\d+\\.\\d+.*\\R" ) );
    }

    @Test
    void printsDriverVersionAndExits() throws ArgumentParserException
    {
        var result = testWithMocks().args( "--driver-version" ).run();

        result.assertSuccess();
        assertTrue( result.getOutput().toString( UTF_8 ).matches( "Neo4j Driver \\d+\\.\\d+\\.\\d+.*\\R" ) );
    }

    private AssertableMain.AssertableMainBuilder testWithMocks()
    {
        return new AssertableMain.AssertableMainBuilder().interactive( true, false ).shell( mockShell ).runnerFactory( mockRunnerFactory );
    }

    private AssertableMain.AssertableMainBuilder testWithMockUser( String name, String password ) throws CommandException
    {
        return testWithMockUser( name, password, null );
    }

    private AssertableMain.AssertableMainBuilder testWithMockUser( String name, String password, String expiredPassword ) throws CommandException
    {
        when( mockShell.connect( any(), any() )).thenAnswer( invocation -> {
            var in = (ConnectionConfig) invocation.getArgument( 0 );
            if ( name.equals( in.username() ) && password.equals( in.password() ) )
            {
                return in;
            }
            else if ( expiredPassword != null && name.equals( in.username() ) && expiredPassword.equals( in.password() ) )
            {
                throw passwordChangeRequiredException;
            }
            else
            {
                throw authException;
            }
        } );
        return testWithMocks();
    }
}
