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
package org.neo4j.server.startup;

import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.internal.helpers.ProcessUtils;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.server.CommunityEntryPoint;
import org.neo4j.test.jar.JarBuilder;
import picocli.CommandLine;

class CustomExtensionTest extends BootloaderCommandTestBase {

    private static final String DEFAULT_CLASS_PATH = ProcessUtils.getClassPath();

    @BeforeEach
    void restoreClasspath() {
        ProcessUtils.setClassPath(DEFAULT_CLASS_PATH);
    }

    @Test
    void canStartWithJarContainingExtension() {
        assertWithExtension(
                "Database can start",
                this::getDebugLogLines,
                (s) -> s.contains(CustomExtension.MESSAGE) && s.contains("is ready."),
                120,
                TimeUnit.SECONDS);
    }

    @Test
    void canRegisterProcedureFromJarContainingExtension() {
        assertWithExtension(
                "Can find registered procedure",
                this::queryProcedures,
                (entries) -> entries.contains(CustomProcedures.PROCEDURE_NAME),
                120,
                TimeUnit.SECONDS);
    }

    private Set<String> queryProcedures() {
        var addr = config.get(BoltConnector.listen_address);
        try (var driver = org.neo4j.driver.GraphDatabase.driver(
                "bolt://" + addr.getHostname() + ":" + addr.getPort(),
                // We customize the driver to add a sensible timeout, to allow the tests to actually
                // fail properly when the assumptions fail.
                org.neo4j.driver.Config.builder()
                        .withConnectionAcquisitionTimeout(1, TimeUnit.SECONDS)
                        .build())) {
            var query = driver.executableQuery("SHOW PROCEDURES");
            var result = query.execute();
            return result.records().stream().map(r -> r.get("name").asString()).collect(Collectors.toSet());
        }
    }

    private <T> void assertWithExtension(
            String message, Callable<T> actual, Predicate<? super T> predicate, long timeout, TimeUnit timeUnit) {
        Process handle;
        try {
            /*
            Implementers note:
                To avoid that other tests pick up the `ExtensionFactory` when performing service
                loading, we declare them package protected. If the extension is loaded, it causes a
                deadlock in the reconciler due to how LazyProcedures is initialized. However, in
                the JAR, the classes must be public in order for the service loading to work.

                We achieve this feat by redefining the class using ByteBuddy and modify the access
                attributes.

                To ensure that the extension is not already on the classpath we create a fresh JVM.
                The selected entry point, does, however, not quite set up the classpath the same way
                that the DBMS sets it up, viz. it does not add the JARs in /plugins directory to the
                classpath, and thus we have to perform this step manually.
            */

            // Create extension and add it to the classpath
            var extension = home.resolve("plugins").resolve("extension.jar");
            createPluginJar(extension);
            ProcessUtils.amendClassPath(extension.toAbsolutePath().toString());

            handle = ProcessUtils.start(
                    // Enable to be able to attach debugger
                    //                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005",
                    CommunityEntryPoint.class.getName(),
                    // Configuration the home dir is required by the entrypoint
                    "--home-dir=%s".formatted(home.toAbsolutePath().toString()),
                    // Disable authentication to allow for driver connections
                    "-cdbms.security.auth_enabled=false");
        } catch (IOException exc) {
            throw new UncheckedIOException(exc);
        }

        try {
            assertEventually(message, actual, predicate, timeout, timeUnit);
        } finally {
            handle.destroyForcibly();
        }
    }

    private <T> DynamicType.Unloaded<T> redefineToPublic(Class<T> cls) {
        return new ByteBuddy()
                .redefine(cls)
                .name(cls.getName() + "Public")
                .modifiers(Modifier.PUBLIC)
                .make();
    }

    protected void createPluginJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(path))) {
            var redefinedExtension = redefineToPublic(CustomExtensionFactory.class);
            writeClass(jarOut, redefinedExtension);
            writeClass(jarOut, CustomProcedures.class);
            addService(
                    jarOut,
                    ExtensionFactory.class.getCanonicalName(),
                    redefinedExtension.getTypeDescription().getCanonicalName());
        }
    }

    private void writeClass(JarOutputStream stream, DynamicType.Unloaded<?> redefined) throws IOException {
        stream.putNextEntry(
                new ZipEntry(toFilename(redefined.getTypeDescription().getCanonicalName())));
        stream.write(redefined.getBytes());
        stream.closeEntry();
    }

    private void writeClass(JarOutputStream stream, Class<?> cls) throws IOException {
        var filename = toFilename(cls.getCanonicalName());
        stream.putNextEntry(new ZipEntry(filename));
        stream.write(JarBuilder.classCompiledBytes(filename));
        stream.closeEntry();
    }

    private String toFilename(String canonicalName) {
        return canonicalName.replace('.', '/') + ".class";
    }

    private void addService(JarOutputStream stream, String iface, String cls) throws IOException {
        stream.putNextEntry(new ZipEntry("META-INF/services/" + iface));
        stream.write(cls.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected CommandLine createCommand(
            PrintStream out,
            PrintStream err,
            Function<String, String> envLookup,
            Function<String, String> propLookup,
            Runtime.Version version) {
        var environment = new Environment(out, err, envLookup, propLookup, version);
        return Neo4jCommand.asCommandLine(new Neo4jCommand(environment), environment);
    }
}
