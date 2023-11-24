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
package org.neo4j.procedure.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.NullLog;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ProcedureClassLoaderTest {

    @Inject
    TestDirectory testDirectory;

    Path archive;

    @BeforeEach
    void createExtension() throws IOException {
        archive = testDirectory.file("extension.jar");
    }

    @ParameterizedTest(name = "procedureReloadEnabled {0}, withExtension {1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void whenJarIsOnClasspath(boolean procedureReloadEnabled, boolean withExtension)
            throws IOException, ProcedureException {
        // Given a JAR with an extension on the classpath
        var appClassloader = makeClassloaderWithClasspath(archive);

        if (withExtension) {
            CustomExtensionUtils.createProcedureWithExtensionJar(archive);
        } else {
            CustomExtensionUtils.createProcedureJar(archive);
        }

        // when
        var result = ProcedureClassLoader.setup(
                appClassloader, List.of(archive), NullLog.getInstance(), procedureReloadEnabled);

        // If the feature is enabled, and the JAR does not contain extensions, then  we want the classloader
        // to be the ProcedureClassLoader that makes procedures reloadable. In other cases, we expect that
        // the application classloader takes over.
        ClassLoader expectedClassLoader;
        if (procedureReloadEnabled && !withExtension) {
            expectedClassLoader = result.loader();
        } else {
            expectedClassLoader = appClassloader;
        }
        assertClassloader(result.loadedClasses(), expectedClassLoader);
    }

    @ParameterizedTest(name = "withExtension {0}")
    @ValueSource(booleans = {true, false})
    void whenJarIsNotOnClasspath(boolean withExtension) throws IOException, ProcedureException {
        // Given a JAR with an empty classpath
        var appClassloader = makeClassloaderWithClasspath();

        if (withExtension) {
            CustomExtensionUtils.createProcedureWithExtensionJar(archive);
        } else {
            CustomExtensionUtils.createProcedureJar(archive);
        }

        // when
        var result = ProcedureClassLoader.setup(
                appClassloader,
                List.of(archive),
                NullLog.getInstance(),
                true // This can only occur with reloadProceduresFromDisk = true
                );

        // then we expect that all classes use the procedure classloader
        assertClassloader(result.loadedClasses(), result.loader());
    }

    @Test
    void validateClassloaderUsesParallelCapableFromURL() throws ZipException, ProcedureException {
        var result = ProcedureClassLoader.setup(List.of(), NullLog.getInstance(), true);

        // We want our classloader to look and feel like an URLClassLoader
        var loader = new URLClassLoader(new URL[] {});
        assertThat(result.loader().isRegisteredAsParallelCapable()).isEqualTo(loader.isRegisteredAsParallelCapable());
    }

    private static ClassLoader makeClassloaderWithClasspath(Path... archives) {
        var urls = Arrays.stream(archives)
                .map(ProcedureClassLoaderTest::uncheckedToURL)
                .toArray(URL[]::new);
        return new URLClassLoader("FakeAppClassloader", urls, null);
    }

    private static URL uncheckedToURL(Path pth) {
        try {
            return pth.toUri().toURL();
        } catch (MalformedURLException exc) {
            throw new RuntimeException(exc);
        }
    }

    private void assertClassloader(List<ProcedureClassLoader.Entry> classes, ClassLoader expectedClassloader) {
        // Compare in string domain to make life easier w.r.t. difference message
        var actual = classes.stream()
                .map(entry -> entry.cls().getCanonicalName() + "->"
                        + entry.cls().getClassLoader().toString())
                .toList();
        var expected = classes.stream()
                .map(entry -> entry.cls().getCanonicalName() + "->" + expectedClassloader.toString())
                .toList();

        assertThat(actual).isEqualTo(expected);
    }
}
