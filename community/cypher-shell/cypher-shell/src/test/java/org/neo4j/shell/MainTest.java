/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.Main.EXIT_SUCCESS;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.test.AssertableMain;

class MainTest {
    private CypherShell mockShell;
    private ShellRunner.Factory mockRunnerFactory;
    private ParameterService parameters;

    private final AuthenticationException authException =
            new AuthenticationException(Main.NEO_CLIENT_ERROR_SECURITY_UNAUTHORIZED, "BOOM");
    private final Neo4jException passwordChangeRequiredException =
            new SecurityException("Neo.ClientError.Security.CredentialsExpired", "BLAM");

    @BeforeEach
    void setup() throws IOException {
        final var connectionConfig = testConnectionConfig("neo4j://localhost:7687");
        mockShell = mock(CypherShell.class);
        when(mockShell.connectionConfig()).thenReturn(connectionConfig);
        mockRunnerFactory = mock(ShellRunner.Factory.class);
        parameters = ParameterService.create(mockShell);
        var runnerMock = mock(ShellRunner.class);
        when(runnerMock.runUntilEnd()).thenReturn(EXIT_SUCCESS);
        when(mockRunnerFactory.create(any(), any(), any(), any())).thenReturn(runnerMock);
    }

    @Test
    void nonEndedStringFails() throws Exception {
        testWithMockUser("random", "bla")
                .userInput("no newline")
                .run()
                .assertFailure("No text could be read, exiting...")
                .assertThatOutput(new Condition<>(s -> s.contains("username: no newline"), "Should contain string"));
    }

    @Test
    void unrelatedErrorDoesNotPrompt() throws Exception {
        doThrow(new RuntimeException("bla")).when(mockShell).connect(any());

        testWithMocks()
                .run()
                .assertFailure("bla")
                .assertThatOutput(new Condition<String>(s -> s.isEmpty(), "String is empty"));

        verify(mockShell, times(1)).connect(any());
    }

    @Test
    void promptsForUsernameAndPasswordIfNoneGivenIfInteractive() throws Exception {
        testWithMockUser("bob", "secret")
                .userInputLines("bob", "secret")
                .run()
                .assertSuccess()
                .assertOutputLines("username: bob", "password: ");
    }

    @Test
    void promptsSilentlyForUsernameAndPasswordIfNoneGivenIfOutputRedirected() throws Exception {
        testWithMockUser("bob", "secret")
                .outputInteractive(false)
                .userInputLines("bob", "secret")
                .run()
                .assertSuccess()
                .assertOutputLines("username: bob", "password: ");
    }

    @Test
    void doesNotPromptIfInputRedirected() throws Exception {
        testWithMockUser("bob", "secret")
                .addArgs("--non-interactive")
                .userInputLines("bob", "secret")
                .run()
                .assertFailure(authException.getMessage())
                .assertOutputLines();
    }

    @Test
    void promptsForUserIfPassExistsIfInteractive() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-p secret")
                .userInputLines("bob", "secret")
                .run()
                .assertSuccess()
                .assertOutputLines("username: bob");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void promptsSilentlyForUserIfPassExistsIfOutputRedirected() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-p secret")
                .outputInteractive(false)
                .userInputLines("bob")
                .run()
                .assertSuccess()
                .assertOutputLines("username: bob");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void promptsForPassBeforeConnectIfUserExistsIfInteractive() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-u bob")
                .userInputLines("secret")
                .run()
                .assertSuccess()
                .assertOutputLines("password: ");

        verify(mockShell, times(1)).connect(any());
    }

    @Test
    void promptsSilentlyForPassIfUserExistsIfOutputRedirected() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-u bob")
                .outputInteractive(false)
                .userInputLines("secret")
                .run()
                .assertSuccess()
                .assertOutputLines("password: ");

        verify(mockShell, times(1)).connect(any());
    }

    @Test
    void promptsForNewPasswordIfPasswordChangeRequiredCannotBeEmpty() throws Exception {
        testWithMockUser("expired_bob", "newpassword", "oldpassword")
                .userInputLines("expired_bob", "oldpassword", "", "newpassword", "newpassword")
                .run()
                .assertSuccess()
                .assertOutputLines(
                        "username: expired_bob",
                        "password: ",
                        "Password change required",
                        "new password: ",
                        "new password cannot be empty",
                        "",
                        "new password: ",
                        "confirm password: ");

        verify(mockShell, times(3)).connect(any());
    }

    @Test
    void promptsForConfirmPasswordIfPasswordChangeRequiredCanBeEmpty() throws Exception {
        testWithMockUser("expired_bob", "newpassword", "oldpassword")
                .userInputLines("expired_bob", "oldpassword", "newpassword", "")
                .run()
                .assertFailure("Passwords are not matching.")
                .assertOutputLines(
                        "username: expired_bob",
                        "password: ",
                        "Password change required",
                        "new password: ",
                        "confirm password: ");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void promptsHandlesBang() throws Exception {
        testWithMockUser("bo!b", "sec!ret")
                .userInputLines("bo!b", "sec!ret")
                .run()
                .assertSuccess()
                .assertOutputLines("username: bo!b", "password: ");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void triesOnlyOnceIfUserPassExists() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-u bob -p wrongpass")
                .run()
                .assertFailure(authException.getMessage())
                .assertOutputLines();

        verify(mockShell, times(1)).connect(any());
    }

    @Test
    void repromptsIfUserIsNotProvidedIfInteractive() throws Exception {
        testWithMockUser("bob", "secret")
                .userInputLines("", "bob", "secret")
                .run()
                .assertSuccess()
                .assertOutputLines("username: ", "username cannot be empty", "", "username: bob", "password: ");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void doesNotRepromptIfUserIsNotProvidedIfOutputRedirected() throws Exception {
        testWithMockUser("bob", "secret")
                .userInputLines("", "secret")
                .outputInteractive(false)
                .run()
                .assertFailure(authException.getMessage())
                .assertOutputLines("username: ", "password: ");

        verify(mockShell, times(2)).connect(any());
    }

    @Test
    void printsVersionAndExits() throws Exception {
        var result = testWithMocks().args("--version").run();

        result.assertSuccess();
        assertTrue(result.getOutput().toString(UTF_8).matches("Cypher-Shell \\d+\\.\\d+\\.\\d+.*\\R"));
    }

    @Test
    void printsDriverVersionAndExits() throws Exception {
        var result = testWithMocks().args("--driver-version").run();

        result.assertSuccess();
        assertTrue(result.getOutput().toString(UTF_8).matches("Neo4j Driver \\d+\\.\\d+\\.\\d+.*\\R"));
    }

    @Test
    void evaluatesLiteralParameterArgumentsOffline() throws Exception {
        testWithMockUser("bob", "secret")
                .args("-u bob -p secret")
                .addArgs("--param", "purple => 'rain'")
                .addArgs("--param", "advice => ['talk', 'less', 'smile', 'more']")
                .addArgs("--param", "when => 12")
                .run()
                .assertSuccess();

        var expectedParams = Map.of(
                "purple", Values.value("rain"),
                "advice", Values.value(List.of("talk", "less", "smile", "more")),
                "when", Values.value(12L));
        assertThat(parameters.parameters()).isEqualTo(expectedParams);
    }

    @Test
    void disconnectOnClose1() throws CommandException, ArgumentParserException, IOException {
        testWithMockUser("mr", "close").args("-u mr -p close").run(false).assertSuccess();

        verify(mockShell, times(1)).connect(any());
        verify(mockShell, times(0)).disconnect();
    }

    @Test
    void disconnectOnClose2() throws CommandException, ArgumentParserException, IOException {
        testWithMockUser("mr", "close").args("-u mr -p close").run(true).assertSuccess();

        verify(mockShell, times(1)).connect(any());
        verify(mockShell, times(1)).disconnect();
    }

    private AssertableMain.AssertableMainBuilder testWithMocks() {
        return new AssertableMain.AssertableMainBuilder()
                .outputInteractive(true)
                .shell(mockShell)
                .runnerFactory(mockRunnerFactory)
                .parameters(parameters);
    }

    private AssertableMain.AssertableMainBuilder testWithMockUser(String name, String password)
            throws CommandException {
        return testWithMockUser(name, password, null);
    }

    private AssertableMain.AssertableMainBuilder testWithMockUser(String name, String password, String expiredPassword)
            throws CommandException {
        doAnswer(invocation -> {
                    var in = (ConnectionConfig) invocation.getArgument(0);
                    if (name.equals(in.username()) && password.equals(in.password())) {
                        return null;
                    } else if (expiredPassword != null
                            && name.equals(in.username())
                            && expiredPassword.equals(in.password())) {
                        throw passwordChangeRequiredException;
                    } else {
                        throw authException;
                    }
                })
                .when(mockShell)
                .connect(any());
        return testWithMocks();
    }
}
