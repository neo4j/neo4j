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
package org.neo4j.shell.expect;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.io.IOUtils.resourceToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.shell.expect.ExpectTestExtension.CYPHER_SHELL_PATH;
import static org.neo4j.shell.expect.InteractionAssertion.assertEqualInteraction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
    private static final boolean DEBUG = false;
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
                resourceToString(expectedPathString, UTF_8, getClass().getClassLoader());
        final var expectScriptFilename = expectResourcePath.getFileName().toString();
        final var args = DEBUG
                ? new String[] {"expect", "-d", expectScriptFilename}
                : new String[] {"expect", expectScriptFilename};
        final var execution = expectContainer.execInContainer(args);
        if (execution.getExitCode() != 0 || !execution.getStderr().isEmpty()) {
            final var message =
                    """
                    Exit Code: %d
                    ==================================== stderr ====================================
                    %s
                    ==================================== stdout ====================================
                    %s
                    """
                            .formatted(execution.getExitCode(), execution.getStderr(), execution.getStdout());
            fail(message);
        }
        assertEqualInteraction(execution.getStdout(), expected);
    }

    public static Stream<Path> findAllExpectResources() throws IOException {
        final var directoryPath = Path.of("expect/tests");
        try (final var stream =
                ExpectTestExtension.class.getClassLoader().getResourceAsStream(directoryPath.toString())) {
            if (stream == null) {
                throw new FileNotFoundException("Resource `" + directoryPath + "` not found");
            }
            final var files = IOUtils.readLines(stream, UTF_8).stream()
                    .filter(f -> f.endsWith(".expect") && !f.equals("common.expect"))
                    .toList();
            if (files.size() < 2) {
                throw new RuntimeException("Could not find test cases in " + directoryPath);
            }
            return files.stream().map(directoryPath::resolve);
        }
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
        try (Stream<Path> pathStream = Files.list(target)) {
            var zipFiles = pathStream
                    .filter(f -> f.getFileName().toString().endsWith(".zip"))
                    .toList();
            if (zipFiles.size() == 1) {
                return zipFiles.get(0);
            } else {
                throw new RuntimeException("Did not find cypher shell zip distribution in " + target);
            }
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
    private static final List<Pattern> REPLACEMENT_PATTERNS = List.of(
            compile("^Connected to Neo4j using Bolt protocol version ([0-9.]+) at"),
            compile("^ready to start consuming query after ([0-9]+) ms, results consumed after another ([0-9]+) ms"));
    private static final Pattern ANSI_CODE_PATTERN = Pattern.compile("\u001B\\[[?]?[;\\d]*[mhlCDA]");

    static void assertEqualInteraction(String actual, String expected) {
        final var cleanedActual = cleanActual(actual).collect(Collectors.joining(System.lineSeparator()));
        final var cleanedExpected = cleanExpected(expected).collect(Collectors.joining(System.lineSeparator()));
        if (!cleanedExpected.equals(cleanedActual)) {
            String message =
                    "Actual interaction was not equal to expected. Hint: expect has debug options you might want to consider"
                            + System.lineSeparator()
                            + debugString(cleanedActual, "Actual Interaction Cleaned")
                            + debugString(cleanedExpected, "Expected Interaction Cleaned")
                            + debugString(actual, "Actual Interaction")
                            + debugString(expected, "Expected Interaction");
            assertEquals(message, cleanedExpected, cleanedActual);
        }
    }

    private static Stream<String> cleanActual(String input) {
        return cleanExpected(input)
                .map(InteractionAssertion::removeAnsiCodes)
                .filter(l -> !l.startsWith(SPAWN_CYPHER_SHELL_START))
                .map(String::trim);
    }

    private static Stream<StringBuilder> cleanExpected(String input) {
        return input.lines().map(InteractionAssertion::replacementPatterns);
    }

    private static StringBuilder replacementPatterns(String input) {
        StringBuilder result = new StringBuilder(input);
        for (Pattern pattern : REPLACEMENT_PATTERNS) {
            removeFirstMatchGroups(pattern, result);
        }
        return result;
    }

    // Removes matching groups from first match of input.
    private static void removeFirstMatchGroups(Pattern pattern, StringBuilder input) {
        final var matcher = pattern.matcher(input);
        if (matcher.find()) {
            int offset = 0;
            for (int i = 1; i <= matcher.groupCount(); ++i) {
                final int start = matcher.start(i);
                final int end = matcher.end(i);
                input.replace(start + offset, end + offset, REPLACEMENT);
                offset = offset - (end - start) + REPLACEMENT.length();
            }
        }
    }

    /*
     * Clean ansi codes.
     *
     * We could assert on ansi codes, but it's no fun building those assertions.
     * Also, there are some ansi codes in the output that feels unnecessary,
     * that would make it even less fun.
     */
    private static String removeAnsiCodes(StringBuilder input) {
        return ANSI_CODE_PATTERN.matcher(input).replaceAll("");
    }

    private static CharSequence debugString(String content, String heading) {
        int size = (80 - heading.length() - 2) / 2;
        final var nl = System.lineSeparator();
        return new StringBuilder(content.length() + 80 * 2 + 12)
                .append("=".repeat(size))
                .append(" ")
                .append(heading)
                .append(" ")
                .append("=".repeat(size))
                .append(nl)
                .append(content)
                .append(nl)
                .append("=".repeat(size - 3))
                .append(" ")
                .append("End of ")
                .append(heading)
                .append(" ")
                .append("=".repeat(size - 2))
                .append(nl);
    }
}
