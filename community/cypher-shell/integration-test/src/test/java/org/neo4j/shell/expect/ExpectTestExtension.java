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
package org.neo4j.shell.expect;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.neo4j.shell.expect.ExpectTestExtension.CYPHER_SHELL_PATH;
import static org.neo4j.shell.expect.ExpectTestExtension.DEBUG;
import static org.neo4j.shell.expect.InteractionAssertion.assertEqualInteraction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Neo4jLabsPlugin;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Extension to run tests using expect. Will start docker containers with neo4j and expect. Use runTestCase to run expect scenarios.
 */
public class ExpectTestExtension implements BeforeAllCallback, AfterAllCallback {
    static final boolean DEBUG = false;
    static String CYPHER_SHELL_PATH = "/cypher-shell/bin/cypher-shell";
    private final String neo4jDockerTag;
    private NeoContainer neo4jContainer;
    private GenericContainer<?> expectContainer;

    public ExpectTestExtension(String neo4jDockerTag) {
        this.neo4jDockerTag = neo4jDockerTag;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        createAndStartContainers();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        this.neo4jContainer.close();
        this.expectContainer.close();
    }

    public void runTestCase(Path expectResourcePath) throws IOException, InterruptedException {
        final var expectedPath = expectResourcePath
                .getParent()
                .resolve(expectResourcePath.getFileName().toString() + ".expected");
        if (getClass().getClassLoader().getResource(expectedPath.toString()) == null) {
            throw new RuntimeException("Missing expected output file: " + expectedPath);
        }
        runTestCase(expectResourcePath, expectedPath);
    }

    public void runTestCase(Path expectResourcePath, Path expectedResourcePath)
            throws IOException, InterruptedException {
        final var expectedPathString = expectedResourcePath.toString();
        final var expected =
                IOUtils.resourceToString(expectedPathString, UTF_8, getClass().getClassLoader());
        final var expectScriptFilename = expectResourcePath.getFileName().toString();
        final var execution = expectContainer.execInContainer("expect", expectScriptFilename);
        final var actual = execution.getStdout();
        assertEqualInteraction(actual, expected);
        assertEquals(0, execution.getExitCode());
        assertEquals("", execution.getStderr());
    }

    public static Stream<Path> findAllExpectResources() throws IOException {
        final var directoryPath = Path.of("expect/tests");
        final var stream = ExpectTestExtension.class.getClassLoader().getResourceAsStream(directoryPath.toString());
        final var files = IOUtils.readLines(stream, "UTF-8").stream()
                .filter(f -> f.endsWith(".expect") && !f.equals("common.expect"))
                .toList();
        if (files.size() < 2) {
            throw new RuntimeException("Could not find test cases in " + directoryPath);
        }
        return files.stream().map(directoryPath::resolve);
    }

    private void createAndStartContainers() throws IOException {
        final var neo4jUser = "neo4j";
        final var neo4jPassword = "123techno";
        final var neo4jAddress = "neo4j";

        final var network = Network.newNetwork();

        // Create a docker container that will be used to run Cypher Shell with expect
        final var expectImage = new ImageFromDockerfile()
                .withFileFromClasspath("Dockerfile", "/expect/docker/Dockerfile")
                .withFileFromClasspath("expect", "/expect/tests")
                .withFileFromPath("cypher-shell.zip", cypherShellZip());
        final var expectContainer = new GenericContainer(expectImage)
                .withNetwork(network)
                .withEnv("NEO4J_USER", neo4jUser)
                .withEnv("NEO4J_PASSWORD", neo4jPassword)
                .withEnv("NEO4J_ADDRESS", neo4jAddress)
                .withEnv("CYPHER_SHELL_PATH", CYPHER_SHELL_PATH);

        // Create container that runs neo4j
        final var neo4jContainer = new NeoContainer("neo4j:" + neo4jDockerTag)
                .withAdminPassword(neo4jPassword)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withNetwork(network)
                .withNetworkAliases(neo4jAddress)
                .withLabsPlugins(Neo4jLabsPlugin.APOC);

        neo4jContainer.start();
        expectContainer.start();

        this.neo4jContainer = neo4jContainer;
        this.expectContainer = expectContainer;
    }

    private static Path cypherShellZip() throws IOException {
        final var target = Path.of("../cypher-shell/target");
        final var zipFiles = Files.list(target)
                .filter(f -> f.getFileName().toString().endsWith(".zip"))
                .toList();
        if (zipFiles.size() == 1) {
            return zipFiles.get(0);
        } else {
            throw new RuntimeException("Did not find cypher shell zip distribution in " + target);
        }
    }

    private static class NeoContainer extends Neo4jContainer<NeoContainer> {
        NeoContainer(String name) {
            super(name);
        }
    }
}

class InteractionAssertion {
    private static final String SPAWN_CYPHER_SHELL_START = "spawn " + CYPHER_SHELL_PATH;
    private static final String REPLACEMENT = "<removed>";
    private static final Pattern BOLT_VERSION_PATTERN =
            Pattern.compile("^Connected to Neo4j using Bolt protocol version ([0-9.]+) at");
    private static final Pattern CONSUME_PATTERN = Pattern.compile(
            "^ready to start consuming query after ([0-9]+) ms, results consumed after another ([0-9]+) ms");

    static void assertEqualInteraction(String actual, String expected) {
        final var cleanedActual = cleanActual(actual).collect(Collectors.joining(System.lineSeparator()));
        final var cleanedExpected = cleanExpected(expected).collect(Collectors.joining(System.lineSeparator()));
        if (!cleanedActual.equals(cleanedExpected)) {
            if (DEBUG) {
                debugPrint(actual, "Actual Interaction");
                debugPrint(expected, "Expected Interaction");
                debugPrint(cleanedActual, "Actual Interaction Cleaned");
                debugPrint(cleanedExpected, "Expected Interaction Cleaned");
            }
            assertEquals(cleanedExpected, cleanedActual);
        }
    }

    private static Stream<String> cleanActual(String input) {
        return cleanExpected(input)
                .filter(l -> !l.startsWith(SPAWN_CYPHER_SHELL_START))
                .map(InteractionAssertion::removeAnsiCodes);
    }

    private static Stream<String> cleanExpected(String input) {
        return input.lines().map(l -> removeFirst(BOLT_VERSION_PATTERN, l)).map(l -> removeFirst(CONSUME_PATTERN, l));
    }

    // Removes matching groups from input.
    private static String removeFirst(Pattern pattern, String input) {
        final var matcher = pattern.matcher(input);
        if (matcher.find()) {
            final var builder = new StringBuilder(input);
            int offset = 0;
            for (int i = 1; i <= matcher.groupCount(); ++i) {
                final int start = matcher.start(i);
                final int end = matcher.end(i);
                builder.replace(start + offset, end + offset, REPLACEMENT);
                offset = offset - (end - start) + REPLACEMENT.length();
            }
            return builder.toString();
        } else {
            return input;
        }
    }

    private static String removeAnsiCodes(String input) {
        // Note, there are some unexpected (to me), ansi codes in the interactions.
        return input.replaceAll("\u001B\\[[?]?[;\\d]*[mhlCD]", "").trim();
    }

    private static void debugPrint(String content, String heading) {
        int size = (80 - heading.length() - 2) / 2;
        System.out.println("=".repeat(size) + " " + heading + " " + "=".repeat(size));
        System.out.println(content);
        final var end = "End of " + heading;
        System.out.println("=".repeat(size - 3) + " " + end + " " + "=".repeat(size - 2));
    }
}
