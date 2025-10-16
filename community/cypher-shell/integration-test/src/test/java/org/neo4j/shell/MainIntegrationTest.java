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

import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.shell.Conditions.contains;
import static org.neo4j.shell.Conditions.emptyString;
import static org.neo4j.shell.Conditions.endsWith;
import static org.neo4j.shell.Conditions.is;
import static org.neo4j.shell.Conditions.notContains;
import static org.neo4j.shell.Conditions.startsWith;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.test.AssertableMain;
import org.neo4j.shell.util.Versions;

// NOTE! Consider adding tests to integration-test-expect instead of here.
@Timeout(value = 5, unit = MINUTES)
class MainIntegrationTest extends TestHarness {
    private static final String newLine = System.lineSeparator();
    private static final String GOOD_BYE = format(":exit%n%nBye!%n");

    private final Condition<String> endsWithInteractiveExit = endsWith(format("> %s", GOOD_BYE));

    private Condition<String> returned42AndExited() {
        return allOf(contains(return42Output()), endsWithInteractiveExit);
    }

    @BeforeEach
    void cleanup() {
        runInDb(DEFAULT_DEFAULT_DB_NAME, shell -> {
            shell.execute(cypher("MATCH (n) DETACH DELETE n;"));
        });
    }

    @Test
    void promptsOnWrongAuthenticationIfInteractive() throws Exception {
        testWithUser("kate", "bushPassword", false)
                .args("--format verbose")
                .userInputLines("kate", "bushPassword", "return 42 as x;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(startsWith(format("username: kate%npassword: %n")), returned42AndExited());
    }

    @Test
    void shouldRunShowUsers() throws Exception {
        assumeAtLeastVersion("4.4.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("SHOW USERS;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("\"neo4j\", [\"admin\", \"PUBLIC\"], FALSE, FALSE, NULL"), endsWithInteractiveExit);
    }

    @Test
    void shouldGetVersionedFunction() throws Exception {
        assumeAtLeastVersion("2025.05.0"); // For testing - switch to 5.26.0, have 5.26.+ server running with version
        // featureflag
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("CYPHER 25 CALL dbms.upgradeStatus(); :exit")
                .run()
                .assertThatErrorOutput(
                        contains(
                                "42N08: syntax error or access rule violation - no such procedure. The procedure dbms.upgradeStatus() was not found. Verify that the spelling is correct."));
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("CYPHER 5 CALL dbms.upgradeStatus(); :exit")
                .run()
                .assertThatErrorOutput(
                        notContains(
                                "42N08: syntax error or access rule violation - no such procedure. The procedure dbms.upgradeStatus() was not found. Verify that the spelling is correct."));
    }

    @Test
    void promptsOnPasswordChangeRequiredSinceVersion4() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bob", "expiredPassword", true)
                .args("--format verbose")
                .userInputLines("bob", "expiredPassword", "newpassword", "newpassword", "return 42 as x;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        startsWith(
                                format(
                                        "username: bob%npassword: %nPassword change required%nnew password: %nconfirm password: %n")),
                        returned42AndExited());
    }

    @Test
    void promptsOnPasswordChangeRequiredBeforeVersion4() throws Exception {
        assumeTrue(serverVersion.major() < 4);

        testWithUser("bob", "expiredPassword", true)
                .args("--format verbose")
                .userInputLines("bob", "expiredPassword", "match (n) return count(n);", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(contains("CALL dbms.changePassword"))
                .assertThatOutput(endsWithInteractiveExit);
    }

    @Test
    void allowUserToUpdateExpiredPasswordInteractivelyWithoutBeingPrompted() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bob", "expiredPassword", true)
                .args("-u bob -p expiredPassword -d system --format verbose")
                .addArgs("ALTER CURRENT USER SET PASSWORD FROM \"expiredPassword\" TO \"shinynew\";")
                .run()
                .assertSuccess()
                .assertThatOutput(contains("0 rows"));

        assertUserCanConnectAndRunQuery("bob", "shinynew");
    }

    @Test
    void shouldFailIfNonInteractivelySettingPasswordOnNonSystemDb() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("kjell", "expiredPassword", true)
                .args("-u kjell -p expiredPassword -d neo4j --non-interactive")
                .addArgs("ALTER CURRENT USER SET PASSWORD FROM \"expiredPassword\" TO \"höglund!\";")
                .run()
                .assertFailure()
                .assertThatErrorOutput(contains("The credentials you provided were valid, but must be changed"));
    }

    @Test
    void shouldBePromptedIfRunningNonInteractiveCypherThatDoesntUpdatePassword() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("bruce", "expiredPassword", true)
                .args("-u bruce -p expiredPassword -d neo4j")
                .addArgs("match (n) return n;")
                .userInputLines("newPassword", "newPassword")
                .run()
                .assertSuccess();

        assertUserCanConnectAndRunQuery("bruce", "newPassword");
    }

    @Test
    void shouldNotBePromptedIfRunningWithExplicitNonInteractiveCypherThatDoesntUpdatePassword() throws Exception {
        assumeTrue(serverVersion.major() >= 4);

        testWithUser("nick", "expiredPassword", true)
                .args("-u nick -p expiredPassword -d neo4j --non-interactive")
                .addArgs("match (n) return n;")
                .run()
                .assertFailure()
                .assertThatErrorOutput(contains("The credentials you provided were valid, but must be changed"))
                .assertThatOutput(emptyString());
    }

    @Test
    void doesPromptOnNonInteractiveOuput() throws Exception {
        testWithUser("holy", "ghostPassword", false)
                .addArgs("return 42 as x;")
                .outputInteractive(false)
                .userInputLines("holy", "ghostPassword")
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("username: holy", "password: ", "x", "42");
    }

    @Test
    void shouldHandleEmptyLine() throws Exception {
        var expectedPrompt = format("neo4j@neo4j> %n" + "neo4j@neo4j> :exit");

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expectedPrompt), endsWithInteractiveExit);
    }

    @Test
    void wrongPortWithBolt() throws Exception {
        testWithUser("leonard", "coenPassword", false)
                .args("-u leonard -p coenPassword -a bolt://localhost:1234")
                .run()
                .assertFailure(
                        "Unable to connect to localhost:1234, ensure the database is running and that there is a working network connection to it.");
    }

    @Test
    void wrongPortWithNeo4j() throws Exception {
        testWithUser("jackie", "levenPassword", false)
                .args("-u jackie -p levenPassword -a neo4j://localhost:1234")
                .run()
                .assertFailure("Connection refused");
    }

    @Test
    void shouldAskForCredentialsWhenConnectingWithAFile() throws Exception {
        testWithUser("jacob", "collier!", false)
                .addArgs("--file", fileFromResource("single.cypher"))
                .userInputLines("jacob", "collier!")
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("username: jacob", "password: ", "result", "42");
    }

    @Test
    void shouldSupportVerboseFormatWhenReadingFile() throws Exception {
        var expectedQueryResult =
                format("+--------+%n" + "| result |%n" + "+--------+%n" + "| 42     |%n" + "+--------+");

        testWithUser("philip", "glassPassword", false)
                .args("-u philip -p glassPassword --format verbose")
                .addArgs("--file", fileFromResource("single.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expectedQueryResult));
    }

    @Test
    void shouldReadEmptyCypherStatementsFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("empty.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(emptyString());
    }

    @Test
    void shouldReadMultipleCypherStatementsFromFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("multiple.cypher"))
                .run()
                .assertSuccessAndConnected()
                .assertOutputLines("result", "42", "result", "1337", "result", "\"done\"");
    }

    @Test
    void shouldFailIfInputFileDoesntExist() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", "missing-file")
                .run()
                .assertFailure("missing-file (No such file or directory)");
    }

    @Test
    void shouldHandleInvalidCypherFromFile() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("invalid.cypher"))
                .run()
                .assertFailure()
                .errorOutputSatisfies(o -> assertThat(o).contains("Invalid input"))
                .assertOutputLines("result", "42");
    }

    @Test
    void shouldReadSingleCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("single.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains("> :source " + file + format("%nresult%n42")), endsWithInteractiveExit);
    }

    @Test
    void shouldDisconnect() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(contains("> :disconnect " + format("%nDisconnected>")), endsWith(GOOD_BYE));
    }

    @Test
    void shouldNotBeAbleToRunQueryWhenDisconnected() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", "RETURN 42 AS x;", ":exit")
                .run()
                .assertThatErrorOutput(contains("Not connected to Neo4j"))
                .assertThatOutput(contains("> :disconnect " + format("%nDisconnected>")), endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndHelp() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":help", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :help")),
                        contains(format("%nAvailable commands:")),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndHistory() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", "in-memory")
                .userInputLines(":disconnect ", ":history", ":exit")
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :history")),
                        contains("1  :disconnect"),
                        contains("2  :history"),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndSource() throws Exception {
        var file = fileFromResource("exit.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":source " + file)
                .run()
                .assertSuccessAndDisconnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :source %s", file)),
                        endsWith(format("Bye!%n")));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernamePasswordAndDatabase() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ", format(":connect -u %s -p %s -d %s", USER, PASSWORD, SYSTEM_DB_NAME), ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect "
                                + format("%nDisconnected> :connect -u %s -p %s -d %s", USER, PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernamePasswordAndDatabaseWithFullArguments() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ",
                        format(":connect --username %s --password %s --database %s", USER, PASSWORD, SYSTEM_DB_NAME),
                        ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect "
                                + format(
                                        "%nDisconnected> :connect --username %s --password %s --database %s",
                                        USER, PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldFailIfConnectingWithInvalidPassword() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ", format(":connect -u %s -p %s -d %s", USER, "wut!", SYSTEM_DB_NAME), ":exit")
                .run()
                .assertSuccessAndDisconnected(false)
                .errorOutputSatisfies(e -> assertThat(e)
                        .containsAnyOf(
                                "42NFF: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                "The client is unauthorized due to authentication failure"))
                .assertThatOutput(
                        contains("> :disconnect "
                                + format("%nDisconnected> :connect -u %s -p %s -d %s", USER, "wut!", SYSTEM_DB_NAME)),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldFailIfConnectingWithInvalidUser() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect ",
                        format(":connect -u %s -p %s -d %s", "PaulWesterberg", PASSWORD, SYSTEM_DB_NAME),
                        ":exit")
                .run()
                .assertSuccessAndDisconnected(false)
                .errorOutputSatisfies(e -> assertThat(e)
                        .containsAnyOf(
                                "42NFF: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                "The client is unauthorized due to authentication failure"))
                .assertThatOutput(
                        contains("> :disconnect "
                                + format(
                                        "%nDisconnected> :connect -u %s -p %s -d %s",
                                        "PaulWesterberg", PASSWORD, SYSTEM_DB_NAME)),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldDisconnectAndConnectWithUsernameAndPassword() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -u %s -p %s", USER, PASSWORD), ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :connect -u %s -p %s", USER, PASSWORD)),
                        endsWith(format("%s@%s> %s", USER, DEFAULT_DEFAULT_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForUsernameAndPasswordIfOnlyDBProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -d %s", SYSTEM_DB_NAME), USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :connect -d %s", SYSTEM_DB_NAME)),
                        contains(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForPasswordIfOnlyUserProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", format(":connect -d %s", SYSTEM_DB_NAME), USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :connect -d %s", SYSTEM_DB_NAME)),
                        contains(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(format("%s@%s> %s", USER, SYSTEM_DB_NAME, GOOD_BYE)));
    }

    @Test
    void shouldPromptForUsernameAndPasswordIfNoArgumentsProvided() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":disconnect ", ":connect", USER, PASSWORD, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :disconnect " + format("%nDisconnected> :connect")),
                        contains(format("%nusername: %s", USER) + format("%npassword: ***")),
                        endsWith(GOOD_BYE));
    }

    @Test
    void shouldReadMultipleCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("multiple.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("> :source " + file + format("%nresult%n42%nresult%n1337%nresult%n\"done\"")),
                        endsWithInteractiveExit);
    }

    @Test
    void shouldReadEmptyCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("empty.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains("> :source " + file + newLine + USER + "@"), endsWithInteractiveExit);
    }

    @Test
    void shouldHandleInvalidCypherStatementsFromFileInteractively() throws Exception {
        var file = fileFromResource("invalid.cypher");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .errorOutputSatisfies(o -> assertThat(o).contains("Invalid input"))
                .assertThatOutput(
                        contains("> :source " + file + format("%nresult%n42%n") + USER + "@"), endsWithInteractiveExit);
    }

    @Test
    void shouldFailIfInputFileDoesntExistInteractively() throws Exception {
        var file = "this-is-not-a-file";
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":source " + file, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(is("Cannot find file: '" + file + "'" + newLine))
                .assertThatOutput(contains("> :source " + file + newLine + USER + "@"), endsWithInteractiveExit);
    }

    @Test
    void doesNotStartWhenDefaultDatabaseUnavailableIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .run()
                .assertFailure()
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertOutputLines());
    }

    @Test
    void startsAgainstSystemDatabaseWhenDefaultDatabaseUnavailableIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":exit")
                .run()
                .assertSuccessAndConnected());
    }

    @Test
    void switchingToUnavailableDatabaseIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":use " + DEFAULT_DEFAULT_DB_NAME, ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertThatOutput(endsWithInteractiveExit));
    }

    @Test
    void switchingToUnavailableDefaultDatabaseIfInteractive() {
        // Multiple databases are only available from 4.0
        assumeTrue(serverVersion.major() >= 4);

        withDefaultDatabaseStopped(() -> buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "-d", SYSTEM_DB_NAME)
                .userInputLines(":use", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(containsDatabaseIsUnavailable(DEFAULT_DEFAULT_DB_NAME))
                .assertThatOutput(endsWithInteractiveExit));
    }

    @Test
    void shouldChangePassword() throws Exception {
        testWithUser("kate", "bushPassword", false)
                .args("--change-password")
                .userInputLines("kate", "bushPassword", "betterpassword", "betterpassword")
                .run()
                .assertSuccess()
                .assertOutputLines("username: kate", "password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("kate", "betterpassword");
    }

    @Test
    void shouldChangePasswordWhenRequired() throws Exception {
        testWithUser("paul", "simonPassword", true)
                .args("--change-password")
                .userInputLines("paul", "simonPassword", "newpassword", "newpassword")
                .run()
                .assertSuccess()
                .assertOutputLines("username: paul", "password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("paul", "newpassword");
    }

    @Test
    void shouldChangePasswordWithUser() throws Exception {
        testWithUser("mike", "oldfield", false)
                .args("-u mike --change-password")
                .userInputLines("oldfield", "newfield", "newfield")
                .run()
                .assertSuccess()
                .assertOutputLines("password: ", "new password: ", "confirm password: ");

        assertUserCanConnectAndRunQuery("mike", "newfield");
    }

    @Test
    void shouldFailToChangePassword() throws Exception {
        testWithUser("led", "zeppelin", false)
                .args("-u led --change-password")
                .userInputLines("FORGOT MY PASSWORD", "robert", "robert")
                .run()
                .assertFailure()
                .assertThatErrorOutput(startsWith("Failed to change password"))
                .assertOutputLines("password: ", "new password: ", "confirm password: ");
    }

    @Test
    void shouldHandleMultiLineHistory() throws Exception {
        final var history = Files.createTempFile("temp-cypher-shell-history", null);
        var expected =
                """
                > :history
                 1  return
                    'hej' as greeting;
                 2  return
                    1
                    as
                    x
                    ;
                 3  :history
                """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", history.toString())
                .userInputLines("return", "'hej' as greeting;", "return", "1", "as", "x", ";", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected), endsWithInteractiveExit);
    }

    @Test
    void createHistoryFileIfNotExists() throws Exception {
        final var historyDirectory = Files.createTempDirectory("temp-cypher-shell-history");
        final var history = historyDirectory.resolve("dir").resolve("dir").resolve("the-history");
        assertFalse(Files.exists(history));

        var expected =
                """
                > :history
                 1  return 1;
                 2  :history
                """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", history.toString())
                .userInputLines("return 1;", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected), endsWithInteractiveExit);

        assertTrue(Files.exists(history));
    }

    @Test
    void historyFromEnvironment() throws Exception {
        final var history = Files.createTempFile("temp-cypher-shell-history", null);

        var expected1 =
                """
                > :history
                 1  return 1;
                 2  :history
                """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .addEnvVariable("NEO4J_CYPHER_SHELL_HISTORY", history.toString())
                .userInputLines("return 1;", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected1), endsWithInteractiveExit);
        assertTrue(Files.exists(history));

        var expected2 =
                """
                > :history
                 1  return 1;
                 2  :history
                 3  :exit
                 4  return 2;
                 5  :history
                """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .addEnvVariable("NEO4J_CYPHER_SHELL_HISTORY", history.toString())
                .userInputLines("return 2;", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected2), endsWithInteractiveExit);

        assertTrue(Files.exists(history));
    }

    @Test
    void failIfHistoryIsDirectory() throws Exception {
        final var history = Files.createTempDirectory("temp-cypher-shell-history");

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", history.toString())
                .userInputLines(":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(
                        contains(
                                "Could not load history file, falling back to session-based history: History file cannot be a directory"));
    }

    @Test
    void clearHistory() throws ArgumentParserException, IOException {
        var history = Files.createTempFile("temp-history", null);

        // Build up some history
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", history.toString())
                .userInputLines("return 1;", "return 2;", ":exit")
                .run()
                .assertSuccessAndConnected();

        var readHistory = Files.readAllLines(history);
        assertEquals(3, readHistory.size());
        assertThat(readHistory.get(0)).is(endsWith("return 1;"));
        assertThat(readHistory.get(1)).is(endsWith("return 2;"));
        assertThat(readHistory.get(2)).is(endsWith(":exit"));

        var expected1 =
                """
                > :history
                 1  return 1;
                 2  return 2;
                 3  :exit
                 4  return 3;
                 5  :history""";
        var expected2 = """
                > :history
                 1  :history

                """;

        // Build up more history and clear
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", history.toString())
                .userInputLines("return 3;", ":history", ":history clear", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected1), contains(expected2));

        var readHistoryAfterClear = Files.readAllLines(history);
        assertEquals(2, readHistoryAfterClear.size());
        assertThat(readHistoryAfterClear.get(0)).is(endsWith(":history"));
        assertThat(readHistoryAfterClear.get(1)).is(endsWith(":exit"));
    }

    @Test
    void inMemoryHistory() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", "in-memory")
                .userInputLines("return 1;", "return 2;", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> return 1;
                        1
                        1
                        neo4j@neo4j> return 2;
                        2
                        2
                        neo4j@neo4j> :history
                         1  return 1;
                         2  return 2;
                         3  :history

                        neo4j@neo4j> :exit
                                """));

        // Running again, previous history should be lost
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--history", "in-memory")
                .userInputLines("return 3;", "return 4;", ":history", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> return 3;
                        3
                        3
                        neo4j@neo4j> return 4;
                        4
                        4
                        neo4j@neo4j> :history
                         1  return 3;
                         2  return 4;
                         3  :history
                                """));
    }

    @Test
    public void failGracefullyOnUnknownCommands() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":non-existing-command")
                .run()
                .assertThatErrorOutput(
                        is("Could not find command :non-existing-command, use :help to see available commands\n"));
    }

    @Test
    void shouldDisconnectAndReconnectAsOtherUser() throws Exception {
        assumeAtLeastVersion("4.2.0");

        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect", ":connect -u new_user -p new_password -d neo4j", "show current user yield user;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("show current user yield user;\n" + "user\n" + "\"new_user\"\n" + "new_user@neo4j>"));
    }

    @Test
    void shouldDisconnectAndFailToReconnect() throws Exception {
        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect",
                        ":connect -u new_user -p " + PASSWORD + " -d neo4j", // Wrong password
                        "show current user yield user;")
                .run()
                .assertThatOutput(contains("neo4j@neo4j> :disconnect\n" + "Disconnected> :connect -u new_user -p "
                        + PASSWORD + " -d neo4j\n" + "Disconnected> show current user yield user;\n"
                        + "Disconnected>"))
                .errorOutputSatisfies(e -> assertThat(e)
                        .contains("Not connected")
                        .containsAnyOf(
                                "42NFF: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                "The client is unauthorized due to authentication failure"));
    }

    @Test
    void shouldDisconnectAndFailToReconnectInteractively() throws Exception {
        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":disconnect",
                        ":connect -u new_user -d neo4j",
                        PASSWORD, // Password prompt with WRONG password
                        "show current user yield user;")
                .run()
                .assertThatOutput(
                        contains("neo4j@neo4j> :disconnect\n" + "Disconnected> :connect -u new_user -d neo4j\n"
                                + "password: ***\n"
                                + "Disconnected> show current user yield user;\n"
                                + "Disconnected>"))
                .errorOutputSatisfies(e -> assertThat(e)
                        .contains("Not connected")
                        .containsAnyOf(
                                "42NFF: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                "The client is unauthorized due to authentication failure"));
    }

    @Test
    void shouldNotConnectIfAlreadyConnected() throws Exception {
        assumeAtLeastVersion("4.2.0");

        testWithUser("new_user", "new_password", false)
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":connect -u new_user -p new_password -d neo4j", // No disconnect
                        "show current user yield user;")
                .run()
                .assertThatErrorOutput(contains("Already connected"))
                .assertThatOutput(contains("neo4j@neo4j> :connect -u new_user -p new_password -d neo4j\n"
                        + "neo4j@neo4j> show current user yield user;\n"
                        + "user\n"
                        + "\"neo4j\"\n"
                        + "neo4j@neo4j> "));
    }

    @Test
    void shouldIndentLineContinuations() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("return", "1 as res", ";", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains("neo4j@neo4j> return\n" + "             1 as res\n"
                        + "             ;\n"
                        + "res\n"
                        + "1\n"
                        + "neo4j@neo4j> :exit"));
    }

    @Test
    void shouldNicelyPrintVectors() throws Exception {
        assumeAtLeastVersion("2025.10.0"); // When vectors were made GA

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        "CYPHER 25 WITH vector([1, 2, 3], 3, INTEGER) AS vector RETURN vector, valueType(vector) AS vectorType;",
                        ":exit")
                .run()
                .assertSuccessAndConnected(true)
                .assertThatOutput(
                        contains(
                                """
                        vector([1, 2, 3], 3, INTEGER NOT NULL), "VECTOR<INTEGER NOT NULL>(3) NOT NULL"
                        """));
    }

    @Test
    void parametersSupportsAllCypherTypes() throws Exception {
        final var paramExpressions = List.of(
                entry("int", "1"),
                entry("float", "1.0"),
                entry("string", "'s'"),
                entry("bool", "true"),
                entry("point", "point({x: 1, y: 2})"),
                entry("date", "date('2009-06-18')"),
                entry("time", "time('02:00')"),
                entry("localtime", "localtime('02:00')"),
                entry("datetime", "datetime('2023-03-21T11:17:15.1Z')"),
                entry("localdatetime", "localdatetime('2023-03-21T11:17:15.1')"),
                entry("duration", "duration('P1M8DT3601.1S')"),
                entry("functionCall", "toString(1+1)"),
                entry("` backticks `", "true"),
                entry("string_escape", "'\\'yes?\\''"),
                entry("string_escape2_judgement_day", "\"\\\"mjau\\\"\""),
                entry("nope", "null"));
        // Combine all parameters in a map force online evaluation of all types as well
        final var evalutateOnlineExp = paramExpressions.stream()
                .filter(e -> !e.getValue().equals("null"))
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(",", "{", "}"));
        final var allParams = Stream.concat(paramExpressions.stream(), Stream.of(entry("online", evalutateOnlineExp)))
                .toList();
        final var paramCalls = allParams.stream()
                .map(e -> format(":param {%s:%s}", e.getKey(), e.getValue()))
                .toList();
        final var verifyQuery = allParams.stream()
                .filter(e -> !e.getValue().equals("null"))
                .map(e -> format("{%s: $%s = %s}", e.getKey(), e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n", "unwind [", "] as result return result;"));

        final var userInput = Stream.concat(paramCalls.stream(), Stream.of(":params", verifyQuery))
                .toList()
                .toArray(new String[0]);
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(userInput)
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                                        > :params
                                        {
                                          ` backticks `: true,
                                          bool: true,
                                          date: date('2009-06-18'),
                                          datetime: datetime('2023-03-21T11:17:15.1Z'),
                                          duration: duration('P1M8DT1H1.1S'),
                                          float: 1.0,
                                          functionCall: '2',
                                          int: 1,
                                          localdatetime: localdatetime('2023-03-21T11:17:15.1'),
                                          localtime: localtime('02:00:00'),
                                          nope: NULL,
                                          online: {
                                            ` backticks `: true,
                                            bool: true,
                                            date: date('2009-06-18'),
                                            datetime: datetime('2023-03-21T11:17:15.1Z'),
                                            duration: duration('P1M8DT1H1.1S'),
                                            float: 1.0,
                                            functionCall: '2',
                                            int: 1,
                                            localdatetime: localdatetime('2023-03-21T11:17:15.1'),
                                            localtime: localtime('02:00:00'),
                                            point: point( {
                                              srid: 7203,
                                              x: 1.0,
                                              y: 2.0
                                            }),
                                            string: 's',
                                            string_escape: '\\'yes?\\'',
                                            string_escape2_judgement_day: '\\"mjau\\"',
                                            time: time('02:00:00Z')
                                          },
                                          point: point( {
                                            srid: 7203,
                                            x: 1.0,
                                            y: 2.0
                                          }),
                                          string: 's',
                                          string_escape: '\\'yes?\\'',
                                          string_escape2_judgement_day: '\\"mjau\\"',
                                          time: time('02:00:00Z')
                                        }"""),
                        contains(
                                """
                                        result
                                        {int: TRUE}
                                        {float: TRUE}
                                        {string: TRUE}
                                        {bool: TRUE}
                                        {point: TRUE}
                                        {date: TRUE}
                                        {time: TRUE}
                                        {localtime: TRUE}
                                        {datetime: TRUE}
                                        {localdatetime: TRUE}
                                        {duration: TRUE}
                                        {functionCall: TRUE}
                                        {` backticks `: TRUE}
                                        {string_escape: TRUE}
                                        {string_escape2_judgement_day: TRUE}
                                        {online: TRUE}
                                        neo4j@neo4j>"""));
    }

    @Test
    void evaluatesParameterArguments() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .addArgs("--param", "purple => 'rain'")
                .addArgs("--param", "advice => ['talk', 'less', 'smile', 'more']")
                .addArgs("--param", "when => date('2021-01-12')")
                .addArgs("--param", "repeatAfterMe => 'A' + 'B' + 'C'")
                .addArgs("--param", "easyAs => 1 + 2 + 3")
                .addArgs("--param", "{a: 1, b: duration({seconds:1}), c:'hi'}")
                .addArgs("--param", "{a: 2*2, d: toString(3)}")
                .userInputLines(
                        ":params",
                        """
                        unwind [$purple,$advice,$when,$repeatAfterMe,$easyAs,$a,$b,$c,$d] as param
                        return param;
                        """)
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                                 > :params
                                 {
                                   a: 4,
                                   advice: ['talk', 'less', 'smile', 'more'],
                                   b: duration('PT1S'),
                                   c: 'hi',
                                   d: '3',
                                   easyAs: 6,
                                   purple: 'rain',
                                   repeatAfterMe: 'ABC',
                                   when: date('2021-01-12')
                                 }"""),
                        contains(
                                """
                                 neo4j@neo4j> unwind [$purple,$advice,$when,$repeatAfterMe,$easyAs,$a,$b,$c,$d] as param
                                              return param;
                                 param
                                 "rain"
                                 ["talk", "less", "smile", "more"]
                                 2021-01-12
                                 "ABC"
                                 6
                                 4
                                 PT1S
                                 "hi"
                                 "3"
                                 """));
    }

    @Test
    void evaluatesParameterArgumentsWithSemicolon() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .addArgs("--param", "purple => 'rain';")
                .addArgs("--param", "white =>  \"space\"  ;  ")
                .addArgs("--param", "advice => ['talk', 'less', 'smile', 'more'];")
                .addArgs("--param", "when => date('2021-01-12');")
                .addArgs("--param", "repeatAfterMe => 'A' + 'B' + 'C';")
                .addArgs("--param", "easyAs => 1 + 2 + 3;")
                .addArgs(
                        "--param",
                        "dt => datetime.truncate('day', datetime({ year: 2023, month: 2, day: 6, hour: 2,  timezone: 'America/Chicago' }));")
                .addArgs(
                        "--param",
                        "dt2 => datetime.truncate('day', datetime({ year: 2023, month: 2, day: 6, hour: 2,  timezone: 'America/Chicago' }));")
                .userInputLines(
                        ":params", "return $purple, $white, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                            > :params
                            {
                              advice: ['talk', 'less', 'smile', 'more'],
                              dt: datetime('2023-02-06T00:00:00-06:00[America/Chicago]'),
                              dt2: datetime('2023-02-06T00:00:00-06:00[America/Chicago]'),
                              easyAs: 6,
                              purple: 'rain',
                              repeatAfterMe: 'ABC',
                              when: date('2021-01-12'),
                              white: 'space'
                            }"""),
                        contains(
                                """
                             > return $purple, $white, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2;
                             $purple, $white, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2
                             "rain", "space", ["talk", "less", "smile", "more"], 2021-01-12, "ABC", 6, 2023-02-06T00:00-06:00[America/Chicago], 2023-02-06T00:00-06:00[America/Chicago]
                             """));
    }

    @Test
    void evaluatesArgumentsInteractive() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":param purple => 'roin'",
                        ":param purple => 'rain'",
                        ":param advice => ['talk', 'less', 'smile', 'more']",
                        ":param when => date('2021-01-12')",
                        ":param repeatAfterMe => 'A' + 'B' + 'C'",
                        ":param easyAs => 1 + 2 + 3",
                        ":param {nope: null}",
                        ":param {a: {a:[duration('PT1M'), 1*2*3]}, b: toString(23)}",
                        ":param {c: 1.1, d: true}",
                        ":params",
                        "unwind [$purple,$advice,$when,$repeatAfterMe,$easyAs,$a,$b,$c,$d,$nope] as param return param;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                        > :params
                        {
                          a: {
                            a: [duration('PT1M'), 6]
                          },
                          advice: ['talk', 'less', 'smile', 'more'],
                          b: '23',
                          c: 1.1,
                          d: true,
                          easyAs: 6,
                          nope: NULL,
                          purple: 'rain',
                          repeatAfterMe: 'ABC',
                          when: date('2021-01-12')
                        }
                        """),
                        contains(
                                """
                        > unwind [$purple,$advice,$when,$repeatAfterMe,$easyAs,$a,$b,$c,$d,$nope] as param return param;
                        param
                        "rain"
                        ["talk", "less", "smile", "more"]
                        2021-01-12
                        "ABC"
                        6
                        {a: [PT1M, 6]}
                        "23"
                        1.1
                        TRUE
                        NULL"""));
    }

    @Test
    void evaluatesArgumentsInteractiveWithSemicolon() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        ":param purple => 'rain';",
                        ":param advice => ['talk', 'less', 'smile', 'more'];",
                        ":param when => date('2021-01-12');",
                        ":param repeatAfterMe => 'A' + 'B' + 'C';",
                        ":param easyAs => 1 + 2 + 3;",
                        ":param dt => datetime.truncate('day', datetime({ year: 2023, month: 2, day: 6, hour: 2,  timezone: 'America/Chicago' }));",
                        ":param dt2 => datetime.truncate('day', datetime({ year: 2023, month: 2, day: 6, hour: 2,  timezone: 'America/Chicago' }));;",
                        ":params",
                        "return $purple, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2;")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains(
                                """
                                        > :params
                                        {
                                          advice: ['talk', 'less', 'smile', 'more'],
                                          dt: datetime('2023-02-06T00:00:00-06:00[America/Chicago]'),
                                          dt2: datetime('2023-02-06T00:00:00-06:00[America/Chicago]'),
                                          easyAs: 6,
                                          purple: 'rain',
                                          repeatAfterMe: 'ABC',
                                          when: date('2021-01-12')
                                        }"""),
                        contains(
                                """
                                        > return $purple, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2;
                                        $purple, $advice, $when, $repeatAfterMe, $easyAs, $dt, $dt2
                                        "rain", ["talk", "less", "smile", "more"], 2021-01-12, "ABC", 6, 2023-02-06T00:00-06:00[America/Chicago], 2023-02-06T00:00-06:00[America/Chicago]
                                        """));
    }

    @Test
    void illegalParameters() throws Exception {
        assumeAtLeastVersion("5.6.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        "create ();",
                        ":params { a: 1 }",
                        ":params { a: collect { match (n) return n } }",
                        ":params { b: { a: [ collect { match (n) return n } ] } }",
                        ":params",
                        ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(
                        contains(
                                "Parameter values needs to have a literal type (not nodes, relationships or paths), but found: `a`: [node"),
                        contains(
                                "Parameter values needs to have a literal type (not nodes, relationships or paths), but found: `b`: {a: [[node"))
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> create ();
                        neo4j@neo4j> :params { a: 1 }
                        neo4j@neo4j> :params { a: collect { match (n) return n } }
                        neo4j@neo4j> :params { b: { a: [ collect { match (n) return n } ] } }
                        neo4j@neo4j> :params
                        {
                          a: 1
                        }
                        neo4j@neo4j>"""))
                .assertThatOutput(contains(GOOD_BYE));
    }

    @Test
    void complexParameters() throws Exception {
        assumeAtLeastVersion("5.6.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(
                        "create ( { p: 'x' });",
                        ":params { a: 1 }",
                        ":params { a: collect { match (n) return n.p } }",
                        ":params { b: { a: [ collect { match (n) return n.p } ] } }",
                        ":params",
                        ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> create ( { p: 'x' });
                        neo4j@neo4j> :params { a: 1 }
                        neo4j@neo4j> :params { a: collect { match (n) return n.p } }
                        neo4j@neo4j> :params { b: { a: [ collect { match (n) return n.p } ] } }
                        neo4j@neo4j> :params
                        {
                          a: ['x'],
                          b: {
                            a: [['x']]
                          }
                        }
                        neo4j@neo4j>"""))
                .assertThatOutput(contains(GOOD_BYE));
    }

    @Test
    void shouldShowPlanDescription() throws Exception {
        assumeAtLeastVersion("4.4.0");

        var expected = serverVersion.major() >= 5
                ? """
                                +-----------------+----+---------+----------------+---------------------+
                                | Operator        | Id | Details | Estimated Rows | Pipeline            |
                                +-----------------+----+---------+----------------+---------------------+
                                | +ProduceResults |  0 | n       |             10 |                     |
                                | |               +----+---------+----------------+                     |
                                | +AllNodesScan   |  1 | n       |             10 | Fused in Pipeline 0 |
                                +-----------------+----+---------+----------------+---------------------+
                                """
                : """
                               +-----------------------+---------+----------------+---------------------+
                               | Operator              | Details | Estimated Rows | Other               |
                               +-----------------------+---------+----------------+---------------------+
                               | +ProduceResults@neo4j | n       |             10 | Fused in Pipeline 0 |
                               | |                     +---------+----------------+---------------------+
                               | +AllNodesScan@neo4j   | n       |             10 | Fused in Pipeline 0 |
                               +-----------------------+---------+----------------+---------------------+
                               """;

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose")
                .userInputLines("EXPLAIN MATCH (n) RETURN n;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains(expected));
    }

    @Test
    void shouldImpersonate() throws Exception {
        assumeAtLeastVersion("4.4.0");

        // Setup impersonated user and role
        runInSystemDb(shell -> {
            createOrReplaceUser(shell, "impersonate_me", "12345678", false);
            shell.execute(cypher("CREATE OR REPLACE ROLE restricted AS COPY OF reader;"));
            shell.execute(cypher("DENY READ {secretProp} ON GRAPHS * TO restricted;"));
            shell.execute(cypher("GRANT ROLE restricted TO impersonate_me;"));
        });

        // Setup data
        runInDb(DEFAULT_DEFAULT_DB_NAME, shell -> {
            shell.execute(cypher("MERGE (n:ImpersonationTest { secretProp: 'hello', otherProp: 'hi' });"));
        });

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--impersonate", "impersonate_me", "--format", "verbose")
                .userInputLines(
                        ":impersonate",
                        ":impersonate impersonate_me",
                        ":impersonate " + USER,
                        ":impersonate impersonate_me",
                        "MATCH (n:ImpersonationTest) RETURN n;",
                        ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(
                        contains("as user neo4j impersonating impersonate_me"),
                        contains(
                                """
                                neo4j(impersonate_me)@neo4j> :impersonate
                                neo4j@neo4j> :impersonate impersonate_me
                                neo4j(impersonate_me)@neo4j> :impersonate neo4j
                                neo4j@neo4j> :impersonate impersonate_me
                                neo4j(impersonate_me)@neo4j> MATCH (n:ImpersonationTest) RETURN n;
                                +----------------------------------------+
                                | n                                      |
                                +----------------------------------------+
                                | (:ImpersonationTest {otherProp: "hi"}) |
                                +----------------------------------------+
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void shouldDenyImpersonate() throws Exception {
        assumeAtLeastVersion("4.4.0");

        // Setup impersonated user and role
        runInSystemDb(shell -> {
            createOrReplaceUser(shell, "impersonate_me", "12345678", false);
            createOrReplaceUser(shell, "alice", "abcdefgh", false);
            shell.execute(cypher("CREATE OR REPLACE ROLE restricted AS COPY OF reader;"));
            shell.execute(cypher("DENY READ {secretProp} ON GRAPHS * TO restricted;"));
            shell.execute(cypher("GRANT ROLE restricted TO impersonate_me;"));
            shell.execute(cypher("DENY IMPERSONATE (alice) ON DBMS TO restricted;"));
        });

        // Setup data
        runInDb(DEFAULT_DEFAULT_DB_NAME, shell -> {
            shell.execute(cypher("MERGE (n:ImpersonationTest { secretProp: 'hello', otherProp: 'hi' });"));
        });

        buildTest()
                .args("-u alice -p abcdefgh --format verbose")
                .userInputLines(":impersonate impersonate_me", "MATCH (n:ImpersonationTest) RETURN n;", ":exit")
                .run()
                .assertSuccess(false)
                .errorOutputSatisfies(e -> assertThat(e)
                        .containsAnyOf(
                                "42NFF: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                "Cannot impersonate user 'impersonate_me'."))
                .assertThatOutput(
                        contains(
                                """
                                alice@neo4j> :impersonate impersonate_me
                                Disconnected> MATCH (n:ImpersonationTest) RETURN n;
                                Disconnected> :exit
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void connectWithCredentialsInAddress() throws Exception {
        assumeAtLeastVersion("4.3");
        testWithUser("the_undertaker", "password", false)
                .args("-a neo4j://the_undertaker:password@localhost:7687 --format plain")
                .userInputLines("SHOW CURRENT USER YIELD user;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        contains(
                                """
                                user
                                "the_undertaker"
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void connectWithEnvironmentalAddress() throws Exception {
        assumeAtLeastVersion("4.3");
        testWithUser("hulk_hogan", "password", false)
                .args("--format plain")
                .addEnvVariable("NEO4J_ADDRESS", "neo4j://hulk_hogan:password@localhost:7687")
                .userInputLines("SHOW CURRENT USER YIELD user;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(
                        contains(
                                """
                                user
                                "hulk_hogan"
                                """),
                        endsWithInteractiveExit);
    }

    @Test
    void disconnectOnClose() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--file", fileFromResource("empty.cypher"))
                .run(true)
                .assertSuccessAndDisconnected()
                .assertThatOutput(emptyString());
    }

    @Test
    void logToFile() throws Exception {
        final var tempFile = Files.createTempFile("temp-log", null);
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--log", tempFile.toString())
                .userInputLines("return 1;", ":exit")
                .run()
                .assertSuccess();

        assertFileContains(tempFile, "Executing cypher: return 1");
        assertFileContains(tempFile, "org.neo4j.driver.internal.logging");

        Files.delete(tempFile);
    }

    @Test
    void license() throws ArgumentParserException, IOException {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines("return 1;", ":exit")
                .run()
                .assertSuccess()
                .assertThatOutput(not(contains("time limited trial")))
                .assertThatErrorOutput(not(contains("time limited trial")));
    }

    /*
     * :sysinfo is hard to test, but here is a pretty crappy smoke test.
     */
    @Test
    void sysInfo() throws Exception {
        assumeAtLeastVersion("4.4.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":sysinfo", ":exit")
                .run()
                .assertSuccessAndConnected()
                .outputSatisfies(o -> assertThat(o)
                        .containsOnlyOnce("\"neo4j\"")
                        .containsOnlyOnce("\"system\"")
                        .containsOnlyOnce("\"Node ID\"")
                        .containsOnlyOnce("\"Relationship ID\"")
                        .containsOnlyOnce("\"Relationship Type ID\"")
                        .containsOnlyOnce("\"Total\"")
                        .containsOnlyOnce("\"Database\"")
                        .containsOnlyOnce("\"Hits\"")
                        .containsOnlyOnce("\"Hit Ratio\"")
                        .containsOnlyOnce("\"Usage Ratio\"")
                        .containsOnlyOnce("\"Page Faults\"")
                        .containsOnlyOnce("\"Last Tx Id\"")
                        .containsOnlyOnce("\"Current Read\"")
                        .containsOnlyOnce("\"Current Write\"")
                        .containsOnlyOnce("\"Peak Transactions\"")
                        .containsOnlyOnce("\"Committed Read\"")
                        .containsOnlyOnce("\"Committed Write\""));
    }

    @Test
    void sysInfoDisconnected() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":disconnect", ":sysinfo", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(contains("Connect to a database to use :sysinfo"))
                .assertThatOutput(
                        contains(
                                """
                                Disconnected> :sysinfo
                                Disconnected> :exit
                                """));
    }

    @Test
    void sysInfoNotSupported() throws Exception {
        assumeVersionBefore("4.4.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":sysinfo", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(contains(":sysinfo is only supported since 4.4.0"))
                .assertThatOutput(
                        contains(
                                """
                                neo4j@neo4j> :sysinfo
                                neo4j@neo4j> :exit
                                """));
    }

    @Test
    void sysInfoOnSystem() throws Exception {
        assumeAtLeastVersion("4.4.0");
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(":use system", ":sysinfo", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatErrorOutput(
                        is("The :sysinfo command is not supported while using the system or a composite database.\n"))
                .assertThatOutput(contains("> :sysinfo\n" + USER + "@system> :exit"));
    }

    @Test
    void showNotificationsIfEnabled() throws Exception {
        final String expected;
        if (protocolVersion.compareTo(Versions.version("5.6")) >= 0) {
            expected =
                    "info: cartesian product. The disconnected pattern '(a:A), (b:B)' builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing. (03N90)";
        } else if (serverVersion.compareTo(Versions.version("5.0.0")) >= 0) {
            expected =
                    "info: If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: (b)) (Neo.ClientNotification.Statement.CartesianProduct)";
        } else {
            expected =
                    "warn: If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: (b)) (Neo.ClientNotification.Statement.CartesianProductWarning)";
        }

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose", "--notifications")
                .userInputLines("match (a:A), (b:B) return *;", ":exit")
                .run()
                .assertSuccessAndConnected(true)
                .assertThatOutput(contains(String.format("\n%s\n", expected)));
    }

    @Test
    void showNotificationsIfEnabledWarn() throws Exception {
        assumeAtLeastVersion("5.2");

        final String expected;

        if (protocolVersion.compareTo(Versions.version("5.6")) >= 0) {
            expected =
                    "warn: feature deprecated with replacement. id is deprecated. It is replaced by elementId or consider using an application-generated id. (01N01)";
        } else {
            expected =
                    "warn: The query used a deprecated function. ('id' has been replaced by 'elementId or consider using an application-generated id') (Neo.ClientNotification.Statement.FeatureDeprecationWarning)";
        }

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose", "--notifications")
                .userInputLines("match (n) return id(n);", ":exit")
                .run()
                .assertSuccessAndConnected(true)
                .assertThatOutput(contains(String.format("\n%s\n", expected)));
    }

    @Test
    void hideNotificationsIfNonInteractive() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose", "--notifications", "--non-interactive")
                .addArgs("match (a:A), (b:B) return *;")
                .run()
                .assertSuccessAndConnected(true)
                .assertThatOutput(notContains("info:"));
    }

    @Test
    void hideNotificationsByDefault() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "verbose")
                .userInputLines("match (a:A), (b:B) return *;", ":exit")
                .run()
                .assertSuccessAndConnected(true)
                .assertThatOutput(notContains("info:"));
    }

    @Test
    void accessModes() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines("create ();", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "write")
                .userInputLines("create ();", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "read")
                .userInputLines("create ();", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(contains("Writing in read access mode not allowed"))
                .assertThatOutput(endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "write")
                .userInputLines(":access-mode read", "create ();", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(contains("Writing in read access mode not allowed"))
                .assertThatOutput(endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "read")
                .userInputLines(":access-mode write", "create ();", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "write")
                .userInputLines("match (n) return n limit 1;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains("\nn\n("), endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain", "--access-mode", "read")
                .userInputLines("match (n) return n limit 1;", ":exit")
                .run()
                .assertSuccessAndConnected()
                .assertThatOutput(contains("\nn\n("), endsWithInteractiveExit);

        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--format", "plain")
                .userInputLines(":access-mode sudo", ":exit")
                .run()
                .assertSuccessAndConnected(false)
                .assertThatErrorOutput(contains("Unknown access mode sudo, available modes are READ, WRITE"))
                .assertThatOutput(endsWithInteractiveExit);
    }

    @Test
    void errorFormatGql() throws Exception {
        final String expected;
        if (isAtLeastVersion("5.27.0")) {
            expected =
                    """
                    42N08: syntax error or access rule violation - no such procedure. The procedure dibs() was not found. Verify that the spelling is correct.
                      42001: syntax error or access rule violation - invalid syntax

                    """;
        } else {
            expected =
                    """
                    There is no procedure with the name `dibs` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.
                    """;
        }
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--error-format", "gql")
                .userInputLines("call dibs;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> call dibs;
                        neo4j@neo4j> :exit"""))
                .errorOutputSatisfies(err ->
                        assertThat(err).as("serverVersion=%s", serverVersion).isEqualTo(expected));
    }

    @Test
    void errorFormatGqlWithPosition() throws Exception {
        final String expected;
        if (isAtLeastVersion("5.27.0")) {
            expected =
                    """
                    42N62: syntax error or access rule violation - variable not defined. Variable `m` not defined. (line 2, column 9 (offset: 19))
                    " RETURN m"
                             ^
                      42001: syntax error or access rule violation - invalid syntax

                    """;
        } else {
            expected =
                    """
                    Variable `m` not defined (line 2, column 9 (offset: 19))
                    " RETURN m"
                             ^
                    """;
        }
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--error-format", "gql")
                .userInputLines("MATCH (n) \n RETURN m;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatOutput(
                        contains(
                                """
                                neo4j@neo4j> MATCH (n)\s
                                              RETURN m;
                                neo4j@neo4j> :exit"""))
                .errorOutputSatisfies(err ->
                        assertThat(err).as("serverVersion=%s", serverVersion).isEqualTo(expected));
    }

    @Test
    void errorFormatLegacy() throws Exception {
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--error-format", "legacy")
                .userInputLines("call dibs;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> call dibs;
                        neo4j@neo4j> :exit"""))
                .errorOutputSatisfies(
                        err -> assertThat(err)
                                .isEqualTo(
                                        """
                        There is no procedure with the name `dibs` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.
                        """));
    }

    @Test
    void errorFormatDefault() throws Exception {
        final String expected;
        if (isAtLeastVersion("5.27.0")) {
            expected =
                    """
                    42N08: syntax error or access rule violation - no such procedure. The procedure dibs() was not found. Verify that the spelling is correct.
                      42001: syntax error or access rule violation - invalid syntax

                    """;
        } else {
            expected =
                    """
                    There is no procedure with the name `dibs` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.
                    """;
        }
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines("call dibs;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> call dibs;
                        neo4j@neo4j> :exit"""))
                .errorOutputSatisfies(err ->
                        assertThat(err).as("serverVersion=%s", serverVersion).isEqualTo(expected));
    }

    @Test
    void errorFormatStacktrace() throws Exception {
        final List<String> expected;
        if (isAtLeastVersion("5.27.0")) {
            expected = List.of(
                    "org.neo4j.driver.exceptions.ClientException: There is no procedure with the name `dibs`",
                    "Suppressed: org.neo4j.driver.internal.util.ErrorUtil$InternalExceptionCause",
                    "Caused by: org.neo4j.driver.exceptions.Neo4jException: 42N08: The procedure dibs() was not found.");
        } else {
            expected = List.of(
                    "org.neo4j.driver.exceptions.ClientException: There is no procedure with the name `dibs` registered for this database instance",
                    "Suppressed: org.neo4j.driver.internal.util.ErrorUtil$InternalExceptionCause");
        }
        buildTest()
                .addArgs("-u", USER, "-p", PASSWORD, "--error-format", "stacktrace")
                .userInputLines("call dibs;", ":exit")
                .run()
                .assertSuccess(false)
                .assertThatOutput(
                        contains(
                                """
                        neo4j@neo4j> call dibs;
                        neo4j@neo4j> :exit"""))
                .errorOutputSatisfies(err -> assertThat(err).contains(expected));
    }

    private static CypherStatement cypher(String cypher) {
        return CypherStatement.complete(cypher);
    }

    private void assertUserCanConnectAndRunQuery(String user, String password) throws Exception {
        buildTest()
                .addArgs("-u", user, "-p", password, "--format", "plain", "return 42 as x;")
                .run()
                .assertSuccess();
    }

    private AssertableMain.AssertableMainBuilder testWithUser(
            String name, String password, boolean requirePasswordChange) {
        runInSystemDb(shell -> createOrReplaceUser(shell, name, password, requirePasswordChange));
        return buildTest();
    }

    private static void createOrReplaceUser(
            CypherShell shell, String name, String password, boolean requirePasswordChange) throws CommandException {
        if (versionOrThrow(shell.getServerVersion()).major() >= 4) {
            var changeString = requirePasswordChange ? "" : " CHANGE NOT REQUIRED";
            shell.execute(CypherStatement.complete(
                    "CREATE OR REPLACE USER " + name + " SET PASSWORD '" + password + "'" + changeString + ";"));
            shell.execute(CypherStatement.complete("GRANT ROLE reader TO " + name + ";"));
        } else {
            try {
                shell.execute(CypherStatement.complete("CALL dbms.security.createUser('" + name + "', '" + password
                        + "', " + requirePasswordChange + ")"));
            } catch (ClientException e) {
                if (e.code().equalsIgnoreCase("Neo.ClientError.General.InvalidArguments")
                        && e.getMessage().contains("already exists")) {
                    shell.execute(CypherStatement.complete("CALL dbms.security.deleteUser('" + name + "')"));
                    var createUser = "CALL dbms.security.createUser('" + name + "', '" + password + "', "
                            + requirePasswordChange + ")";
                    shell.execute(CypherStatement.complete(createUser));
                }
            }
        }
    }

    private static org.neo4j.shell.util.Version versionOrThrow(String version) {
        try {
            return Versions.version(version);
        } catch (Versions.FailedToParseException e) {
            throw new RuntimeException(e);
        }
    }

    private String return42Output() {
        return format("> return 42 as x;%n" + return42VerboseTable());
    }

    private String return42VerboseTable() {
        return format("+----+%n" + "| x  |%n" + "+----+%n" + "| 42 |%n" + "+----+%n" + "%n" + "1 row");
    }

    private String fileFromResource(String filename) {
        return requireNonNull(getClass().getClassLoader().getResource(filename)).getFile();
    }

    private void withDefaultDatabaseStopped(ThrowingAction<Exception> test) {
        final var useWait = serverVersion.compareTo(versionOrThrow("4.4.0")) >= 0;
        final var stop = "STOP DATABASE " + DEFAULT_DEFAULT_DB_NAME + (useWait ? " WAIT;" : ";");
        final var start = "START DATABASE " + DEFAULT_DEFAULT_DB_NAME + (useWait ? " WAIT;" : ";");
        try {
            runInSystemDb(shell -> shell.execute(CypherStatement.complete(stop)));
            test.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            runInSystemDb(shell -> shell.execute(CypherStatement.complete(start)));
        }
    }

    private static void assertFileContains(Path file, String find) throws IOException {
        try (var reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(find)) {
                    return;
                }
            }
        }
        fail("Could not find '" + find + "' in file " + file);
    }

    private Condition<String> containsDatabaseIsUnavailable(String name) {
        return anyOf(
                contains("database is unavailable"),
                contains("Database '" + name + "' is unavailable"),
                contains("database is not currently available"));
    }
}
