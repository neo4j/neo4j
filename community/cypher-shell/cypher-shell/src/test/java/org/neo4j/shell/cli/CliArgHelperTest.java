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
package org.neo4j.shell.cli;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.shell.test.Util.asArray;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.Environment;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.RawParameters;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.shell.test.LocaleDependentTestBase;

class CliArgHelperTest extends LocaleDependentTestBase {
    private CliArgHelper parser;
    private Map<String, String> env;

    private CliArgs parse(String... args) {
        var parsed = parser.parse(args);
        if (parsed == null) {
            fail("Failed to parse arguments: " + Arrays.toString(args));
        }
        return parsed;
    }

    private String parseAndFail(String... args) {
        final var originalErr = System.err;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            System.setErr(new PrintStream(bout));

            final var parsed = parser.parse(args);
            assertNull(parsed);

            return bout.toString();
        } finally {
            System.setErr(originalErr);
        }
    }

    @BeforeEach
    void setup() {
        env = new HashMap<>();
        parser = new CliArgHelper(new Environment(env));
    }

    // The library is fuzzy sometimes, for example it can parse '--idle' as '--idle-timeout' if it feels like it.
    // Because we're using two parsers (one for hidden), that can be problematic.
    @Test
    void parseAll() throws IOException {
        final var tempFile = Files.createTempFile("test-log", ".log");
        final var args = parse(
                "--fail-at-end",
                "--format",
                "plain",
                "--param",
                "{p:1}",
                "--non-interactive",
                "--sample-rows",
                "123",
                "--wrap",
                "false",
                "--driver-version",
                "--change-password",
                "--log",
                tempFile.toString(),
                "--history",
                "myhistfile",
                "--notifications",
                "--idle-timeout",
                "3s",
                "--hidden-idle-timeout",
                "4s",
                "--address",
                "myneoaddress",
                "--username",
                "myuser",
                "--impersonate",
                "impuser",
                "--password",
                "mypass",
                "--encryption",
                "true",
                "--database",
                "mydb",
                "--access-mode",
                "read");
        assertThat(args.getFailBehavior()).isEqualTo(FailBehavior.FAIL_AT_END);
        assertThat(args.getFormat()).isEqualTo(Format.PLAIN);
        assertThat(args.getParameters()).isEqualTo(List.of(new ParameterService.RawParameters("{p:1}")));
        assertThat(args.getNonInteractive()).isEqualTo(true);
        assertThat(args.getNumSampleRows()).isEqualTo(123);
        assertThat(args.getWrap()).isEqualTo(false);
        assertThat(args.getDriverVersion()).isEqualTo(true);
        assertThat(args.getChangePassword()).isEqualTo(true);
        assertThat(args.logHandler()).containsInstanceOf(FileHandler.class);
        assertThat(args.getHistoryBehaviour()).isEqualTo(new CypherShellTerminal.FileHistory(Path.of("myhistfile")));
        assertThat(args.getNotificationsEnabled()).isEqualTo(true);
        assertThat(args.getIdleTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(args.getIdleTimeoutDelay()).isEqualTo(Duration.ofSeconds(4));
        assertThat(args.getUri()).isEqualTo(URI.create("neo4j://myneoaddress:7687"));
        assertThat(args.getUsername()).isEqualTo("myuser");
        assertThat(args.getImpersonatedUser()).contains("impuser");
        assertThat(args.getEncryption()).isEqualTo(Encryption.TRUE);
        assertThat(args.getDatabase()).isEqualTo("mydb");
        assertThat(args.getAccessMode()).isEqualTo(AccessMode.READ);
    }

    @Test
    void testForceNonInteractiveIsNotDefault() {
        assertFalse(parse(asArray()).getNonInteractive(), "Force non-interactive should not be the default mode");
    }

    @Test
    void testForceNonInteractiveIsParsed() {
        assertTrue(
                parse(asArray("--non-interactive")).getNonInteractive(),
                "Force non-interactive should have been parsed to true");
    }

    @Test
    void testNumSampleRows() {
        assertEquals(200, parse("--sample-rows 200".split(" ")).getNumSampleRows(), "sample-rows 200");
        assertNull(parser.parse("--sample-rows 0".split(" ")), "invalid sample-rows");
        assertNull(parser.parse("--sample-rows -1".split(" ")), "invalid sample-rows");
        assertNull(parser.parse("--sample-rows foo".split(" ")), "invalid sample-rows");
    }

    @Test
    void testWrap() {
        assertTrue(parse("--wrap true".split(" ")).getWrap(), "wrap true");
        assertFalse(parse("--wrap false".split(" ")).getWrap(), "wrap false");
        assertTrue(parse().getWrap(), "default wrap");
        assertNull(parser.parse("--wrap foo".split(" ")), "invalid wrap");
    }

    @Test
    void testDefaultScheme() {
        CliArgs arguments = parser.parse();
        assertNotNull(arguments);
        assertEquals("neo4j", arguments.getUri().getScheme());
    }

    @Test
    void testVersionIsParsed() {
        assertTrue(parse(asArray("--version")).getVersion(), "Version should have been parsed to true");
    }

    @Test
    void testDriverVersionIsParsed() {
        assertTrue(
                parse(asArray("--driver-version")).getDriverVersion(),
                "Driver version should have been parsed to true");
    }

    @Test
    void testFailFastIsDefault() {
        assertEquals(FailBehavior.FAIL_FAST, parse(asArray()).getFailBehavior(), "Unexpected fail-behavior");
    }

    @Test
    void testFailFastIsParsed() {
        assertEquals(
                FailBehavior.FAIL_FAST, parse(asArray("--fail-fast")).getFailBehavior(), "Unexpected fail-behavior");
    }

    @Test
    void testFailAtEndIsParsed() {
        assertEquals(
                FailBehavior.FAIL_AT_END,
                parse(asArray("--fail-at-end")).getFailBehavior(),
                "Unexpected fail-behavior");
    }

    @Test
    void singlePositionalArgumentIsFine() {
        String text = "Single string";
        assertEquals(Optional.of(text), parse(asArray(text)).getCypher(), "Did not parse cypher string");
    }

    @Test
    void parseArgumentsAndQuery() {
        String query = "\"match (n) return n\"";
        ArrayList<String> strings = new ArrayList<>(asList("-a 192.168.1.1 -p 123 --format plain".split(" ")));
        strings.add(query);
        assertEquals(Optional.of(query), parse(strings.toArray(new String[0])).getCypher());
    }

    @Test
    void parseFormat() {
        assertEquals(Format.PLAIN, parse("--format", "plain").getFormat());
        assertEquals(Format.VERBOSE, parse("--format", "verbose").getFormat());
    }

    @Test
    void parsePassword() {
        assertEquals("foo", parse("--password", "foo").getPassword());
        assertEquals("", parse().getPassword());
    }

    @Test
    void parsePasswordWithFallback() {
        env.put("NEO4J_PASSWORD", "foo");
        assertEquals("foo", parse().getPassword());
        assertEquals("notfoo", parse("--password", "notfoo").getPassword());
    }

    @Test
    void parseUserName() {
        assertEquals("foo", parse("--username", "foo").getUsername());
        assertEquals("", parse().getUsername());
    }

    @Test
    void parseUserWithFallback() {
        env.put("NEO4J_USERNAME", "foo");
        assertEquals("foo", parse().getUsername());
        assertEquals("notfoo", parse("--username", "notfoo").getUsername());
    }

    @Test
    void parseFullAddress() {
        CliArgs cliArgs = parse("--address", "bolt+routing://alice:foo@bar:69");
        assertEquals("alice", cliArgs.getUsername());
        assertEquals("foo", cliArgs.getPassword());
        assertEquals("bolt+routing", cliArgs.getUri().getScheme());
        assertEquals("bar", cliArgs.getUri().getHost());
        assertEquals(69, cliArgs.getUri().getPort());
    }

    @Test
    void parseFullAddress2() {
        CliArgs cliArgs = parse("--uri", "bolt+routing://alice:foo@bar:69");
        assertEquals("alice", cliArgs.getUsername());
        assertEquals("foo", cliArgs.getPassword());
        assertEquals("bolt+routing", cliArgs.getUri().getScheme());
        assertEquals("bar", cliArgs.getUri().getHost());
        assertEquals(69, cliArgs.getUri().getPort());
    }

    @Test
    void parseFullAddress3() {
        String failure =
                parseAndFail("--uri", "bolt+routing://alice:foo@bar:69", "--address", "bolt+routing://bob:foo@bar:69");
        assertThat(failure).contains("usage: cypher-shell", "cypher-shell: error: Specify one of -a/--address/--uri");
    }

    @Test
    void parseFullAddressWithFallback() {
        env.put("NEO4J_ADDRESS", "bolt+routing://alice:foo@bar:69");
        assertEquals(URI.create("bolt+routing://alice:foo@bar:69"), parse().getUri());
        assertEquals(
                URI.create("bolt://bob:log@mjau:123"),
                parse("--address", "bolt://bob:log@mjau:123").getUri());
    }

    @Test
    void parseFullAddressWithFallback2() {
        env.put("NEO4J_URI", "bolt+routing://alice:foo@bar:69");
        assertEquals(URI.create("bolt+routing://alice:foo@bar:69"), parse().getUri());
        assertEquals(
                URI.create("bolt://bob:log@mjau:123"),
                parse("--address", "bolt://bob:log@mjau:123").getUri());
    }

    @Test
    void parseFullAddressWithFallback3() {
        env.put("NEO4J_URI", "bolt+routing://alice:foo@bar:69");
        env.put("NEO4J_ADDRESS", "bolt+routing://bob:foo@bar:69");

        assertThatThrownBy(this::parse)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Specify one or none of environment variables NEO4J_ADDRESS and NEO4J_URI");
    }

    @Test
    void defaultAddress() {
        assertEquals(URI.create("neo4j://localhost:7687"), parse().getUri());
    }

    @Test
    void parseDatabase() {
        assertEquals("db2", parse("--database", "db2").getDatabase());
        assertEquals("db1", parse("-d", "db1").getDatabase());
        assertEquals("", parse().getDatabase());
    }

    @Test
    void parseDatabaseWithFallback() {
        env.put("NEO4J_DATABASE", "secrets");
        assertEquals("secrets", parse().getDatabase());
        assertEquals("other", parse("--database", "other").getDatabase());
    }

    @Test
    void parseWithoutProtocol() {
        assertEquals(
                URI.create("neo4j://localhost:10000"),
                parse("--address", "localhost:10000").getUri());
    }

    @Test
    void parseAddressWithRoutingContext() {
        CliArgs cliArgs = parser.parse("--address", "neo4j://localhost:7697?policy=one");
        assertNotNull(cliArgs);
        assertEquals("neo4j", cliArgs.getUri().getScheme());
        assertEquals("localhost", cliArgs.getUri().getHost());
        assertEquals(7697, cliArgs.getUri().getPort());
    }

    @Test
    void nonsenseArgsGiveError() {
        String failure = parseAndFail("-notreally");

        assertTrue(failure.contains("cypher-shell [-h]"));
        assertTrue(failure.contains("cypher-shell: error: unrecognized arguments: '-notreally'"));
    }

    @Test
    void nonsenseUrlGivesError() {
        String failure = parseAndFail("--address", "host port");

        assertTrue(failure.contains("cypher-shell [-h]"));
        assertTrue(failure.contains("cypher-shell: error: Failed to parse address"));
        assertTrue(failure.contains("\nAddress should be of the form:"));
    }

    @Test
    void defaultsEncryptionToDefault() {
        assertEquals(Encryption.DEFAULT, parse().getEncryption());
    }

    @Test
    void allowsEncryptionToBeTurnedOnOrOff() {
        assertEquals(Encryption.TRUE, parse("--encryption", "true").getEncryption());
        assertEquals(Encryption.FALSE, parse("--encryption", "false").getEncryption());
    }

    @Test
    void shouldNotAcceptInvalidEncryption() {
        assertThatThrownBy(() -> parser.parseAndThrow("--encryption", "bugaluga"))
                .isInstanceOf(ArgumentParserException.class)
                .hasMessageContaining(
                        "argument --encryption: invalid choice: 'bugaluga' (choose from {true,false,default})");
    }

    @Test
    void shouldParseSingleStringArg() {
        CliArgs cliArgs = parser.parse("-P", "foo=>'nanana'");
        assertNotNull(cliArgs);
        assertEquals(List.of(new RawParameters("{foo: 'nanana'}")), cliArgs.getParameters());
    }

    @Test
    void shouldParseTwoArgs() {
        CliArgs cliArgs = parser.parse("-P", "foo=>'nanana'", "-P", "bar=>35");
        assertThat(cliArgs).isNotNull();
        var expected = List.of(new RawParameters("{foo: 'nanana'}"), new RawParameters("{bar: 35}"));
        assertThat(cliArgs.getParameters()).isEqualTo(expected);
    }

    @Test
    void shouldFailForInvalidSyntaxForArg() {
        assertThatThrownBy(() -> parser.parseAndThrow("-P", "foo: => 'nanana"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Incorrect usage", "usage: --param  'name => value'");
    }

    @Test
    void testDefaultInputFileName() {
        CliArgs arguments = parser.parse();
        assertNotNull(arguments);
        assertNull(arguments.getInputFilename());
    }

    @Test
    void testSetInputFileName() {
        CliArgs arguments = parser.parse("--file", "foo");
        assertNotNull(arguments);
        assertEquals("foo", arguments.getInputFilename());
    }

    @Test
    void helpfulIfUsingWrongFile() {
        assertThatThrownBy(() -> parser.parseAndThrow("-file", "foo"))
                .isInstanceOf(ArgumentParserException.class)
                .hasMessageContaining("Unrecognized argument '-file', did you mean --file?");
    }

    @Test
    void impersonation() {
        CliArgs arguments = parser.parse("--impersonate", "some-user");
        assertNotNull(arguments);
        assertEquals(Optional.of("some-user"), arguments.connectionConfig().impersonatedUser());
    }

    @Test
    void defaultLogHandler() {
        CliArgs arguments = parse();
        assertEquals(Optional.empty(), arguments.logHandler());
    }

    @Test
    void defaultEmptyLogHandler() {
        CliArgs arguments = parse("--log");
        assertThat(arguments.logHandler()).containsInstanceOf(ConsoleHandler.class);
    }

    @Test
    void fileLogHandler() throws IOException {
        final var dir = Files.createTempDirectory("temp-dir");
        final var file = new File(dir.toFile(), "shell.log");
        CliArgs arguments = parse("--log", file.getAbsolutePath());
        assertThat(arguments.logHandler()).containsInstanceOf(FileHandler.class);
        file.delete();
    }

    @Test
    void history() {
        assertThat(parse().getHistoryBehaviour()).isInstanceOf(CypherShellTerminal.DefaultHistory.class);
        assertThat(parse("--history", "in-memory").getHistoryBehaviour())
                .isInstanceOf(CypherShellTerminal.InMemoryHistory.class);
        assertThat(parse("--history", "/some/path/file.history").getHistoryBehaviour())
                .isEqualTo(new CypherShellTerminal.FileHistory(Path.of("/some/path/file.history")));
    }

    @Test
    void notificationsEnabled() {
        CliArgs arguments = parser.parse("--notifications");
        assertNotNull(arguments);
        assertEquals(true, arguments.getNotificationsEnabled());
    }

    @Test
    void notificationsDefault() {
        CliArgs arguments = parser.parse();
        assertNotNull(arguments);
        assertEquals(false, arguments.getNotificationsEnabled());
    }

    @Test
    void accessModes() {
        assertEquals(AccessMode.READ, parser.parse("--access-mode", "read").getAccessMode());
        assertEquals(AccessMode.READ, parser.parse("--access-mode", "ReAd").getAccessMode());
        assertEquals(AccessMode.WRITE, parser.parse("--access-mode", "write").getAccessMode());
        assertEquals(AccessMode.WRITE, parser.parse("--access-mode", "wRiTe").getAccessMode());
        assertEquals(AccessMode.WRITE, parser.parse().getAccessMode());

        assertThatThrownBy(() -> parser.parseAndThrow("--access-mode", "godmode"))
                .isInstanceOf(ArgumentParserException.class)
                .hasMessageContaining("argument --access-mode: could not convert 'godmode' (choose from {read,write})");
    }

    @Test
    void idleTimeout() {
        final var defaultTimeout = Duration.ofSeconds(-1);
        final var defaultDelay = Duration.ofMinutes(5);
        assertTimeout(defaultTimeout, defaultDelay);
        assertTimeout(defaultTimeout, defaultDelay, "--idle-timeout", "disable");
        assertTimeout(Duration.ofSeconds(1), defaultDelay, "--idle-timeout", "1s");
        assertTimeout(Duration.ofMinutes(1), defaultDelay, "--idle-timeout", "1m");
        assertTimeout(Duration.ofHours(1), defaultDelay, "--idle-timeout", "1h");
        assertTimeout(Duration.ofHours(2).plusMinutes(3).plusSeconds(4), defaultDelay, "--idle-timeout", "2h3m4s");

        assertTimeout(defaultTimeout, defaultDelay, "--hidden-idle-timeout-delay", "disable");
        assertTimeout(defaultTimeout, Duration.ofSeconds(1), "--hidden-idle-timeout-delay", "1s");
        assertTimeout(defaultTimeout, Duration.ofMinutes(1), "--hidden-idle-timeout-delay", "1m");
        assertTimeout(defaultTimeout, Duration.ofHours(1), "--hidden-idle-timeout-delay", "1h");
        assertTimeout(
                defaultTimeout,
                Duration.ofHours(2).plusMinutes(3).plusSeconds(4),
                "--hidden-idle-timeout-delay",
                "2h3m4s");

        assertTimeout(
                Duration.ofHours(1).plusMinutes(2).plusSeconds(3),
                Duration.ofHours(4).plusMinutes(5).plusSeconds(6),
                "--idle-timeout",
                "1h2m3s",
                "--hidden-idle-timeout-delay",
                "4h5m6s");
    }

    private void assertTimeout(Duration timeout, Duration delay, String... params) {
        final var args = parse(params);
        assertEquals(timeout, args.getIdleTimeout());
        assertEquals(delay, args.getIdleTimeoutDelay());
    }

    @Test
    void rememberToUpdateDocs() {
        final var defaultOut = System.out;
        final String helpText;
        try {
            final var out = new ByteArrayOutputStream();
            final var printStream = new PrintStream(out);
            System.setOut(printStream);
            parser.parse("--help");
            helpText = out.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(defaultOut);
        }

        assertThat(helpText)
                .describedAs("\n⚠️️️️⚠️⚠️ Help has changed. Remember to update docs!! ⚠️⚠️⚠️\n")
                .isEqualTo(
                        """
                                usage: cypher-shell [-h] [-a ADDRESS] [-u USERNAME] [--impersonate IMPERSONATE] [-p PASSWORD]
                                                    [--encryption {true,false,default}] [-d DATABASE] [--access-mode {read,write}]
                                                    [--format {auto,verbose,plain}] [-P PARAM] [--non-interactive]
                                                    [--sample-rows SAMPLE-ROWS] [--wrap {true,false}] [-v] [--driver-version]
                                                    [-f FILE] [--change-password] [--log [LOG-FILE]] [--history HISTORY-BEHAVIOUR]
                                                    [--notifications] [--idle-timeout IDLE-TIMEOUT] [--fail-fast | --fail-at-end]
                                                    [cypher]

                                Cypher Shell is a command-line tool used to  run  queries and perform administrative tasks against a
                                Neo4j instance. By default, the shell  is  interactive,  but  you  can  also use it for scripting by
                                passing Cypher directly on the command line  or  by  piping  a file with Cypher statements (requires
                                Powershell on Windows). It communicates via the Bolt protocol.

                                Example of piping a file:
                                  cat some-cypher.txt | cypher-shell

                                positional arguments:
                                  cypher                 An optional string of Cypher to execute and then exit.

                                named arguments:
                                  -h, --help             show this help message and exit
                                  --fail-fast            Exit and report failure on the first  error  when reading from a file (this
                                                         is the default behavior).
                                  --fail-at-end          Exit and report failures at the end of the input when reading from a file.
                                  --format {auto,verbose,plain}
                                                         Desired output format. Displays the  results  in  tabular format if you use
                                                         the shell interactively and  with  minimal  formatting  if  you  use it for
                                                         scripting.
                                                         `verbose` displays results in tabular format and prints statistics.
                                                         `plain` displays data with minimal formatting. (default: auto)
                                  -P PARAM, --param PARAM
                                                         Add a parameter to this session.  Example:  `-P  {a:  1}`  or `-P {a: 1, b:
                                                         duration({seconds: 1})}`. This argument  can  be  specified multiple times.
                                                         (default: [])
                                  --non-interactive      Force non-interactive mode. Only useful  when auto-detection fails (like on
                                                         Windows). (default: false)
                                  --sample-rows SAMPLE-ROWS
                                                         Number of rows sampled to  compute  table widths (only for format=VERBOSE).
                                                         (default: 1000)
                                  --wrap {true,false}    Wrap  table  column   values   if   column   is   too   narrow   (only  for
                                                         format=VERBOSE). (default: true)
                                  -v, --version          Print Cypher Shell version and exit. (default: false)
                                  --driver-version       Print Neo4j Driver version and exit. (default: false)
                                  -f FILE, --file FILE   Pass a file with  Cypher  statements  to  be  executed. After executing all
                                                         statements, Cypher Shell shuts down.
                                  --change-password      Change the neo4j user password and exit. (default: false)
                                  --log [LOG-FILE]       Enable logging to the specified  file,  or  standard  error  if the file is
                                                         omitted.
                                  --history HISTORY-BEHAVIOUR
                                                         File path of a query  and  a  command  history  file or `in-memory` for in-
                                                         memory history. Defaults  to  <user home>/.neo4j/.cypher_shell_history. Can
                                                         also be set using the environment variable NEO4J_CYPHER_SHELL_HISTORY.
                                  --notifications        Enable notifications in interactive mode. (default: false)
                                  --idle-timeout IDLE-TIMEOUT
                                                         Closes  the  application  after  the  specified  amount  of  idle  time  in
                                                         interactive  mode.  You  can   specify   the   duration  using  the  format
                                                         `<hours>h<minutes>m<seconds>s`, for example `1h` (1  hour), `1h30m` (1 hour
                                                         30 minutes), or `30m` (30 minutes).

                                connection arguments:
                                  -a ADDRESS, --address ADDRESS, --uri ADDRESS
                                                         Address and port to  connect  to.  Defaults  to neo4j://localhost:7687. Can
                                                         also  be  specified  using   the   environment  variable  NEO4J_ADDRESS  or
                                                         NEO4J_URI.
                                  -u USERNAME, --username USERNAME
                                                         Username to  connect  as.  Can  also  be  specified  using  the environment
                                                         variable NEO4J_USERNAME.
                                  --impersonate IMPERSONATE
                                                         User to impersonate.
                                  -p PASSWORD, --password PASSWORD
                                                         Password to connect  with.  Can  also  be  specified  using the environment
                                                         variable NEO4J_PASSWORD.
                                  --encryption {true,false,default}
                                                         Whether  the  connection  to  Neo4j  should  be  encrypted.  This  must  be
                                                         consistent with  the  Neo4j's  configuration.  If  choosing  'default', the
                                                         encryption setting is deduced from the  specified address. For example, the
                                                         'neo4j+ssc' protocol uses encryption. (default: default)
                                  -d DATABASE, --database DATABASE
                                                         Database to  connect  to.  Can  also  be  specified  using  the environment
                                                         variable NEO4J_DATABASE.
                                  --access-mode {read,write}
                                                         Access mode. Defaults to WRITE. (default: write)
                                """);
    }
}
