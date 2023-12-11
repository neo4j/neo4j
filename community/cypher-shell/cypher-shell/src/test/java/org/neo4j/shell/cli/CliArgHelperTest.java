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
import java.nio.file.Files;
import java.nio.file.Path;
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
}
