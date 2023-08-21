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
package org.neo4j.codegen.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.codegen.source.ClasspathHelper.fullClasspathFor;
import static org.neo4j.codegen.source.ClasspathHelper.fullClasspathStringFor;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ClasspathHelperTest {
    @Test
    void shouldNotFailForNullClassLoader() {
        assertThat(fullClasspathFor(null)).isNotEmpty();
    }

    @Test
    void shouldWorkForClassLoaderWithNoParent() throws Exception {
        // Given
        ClassLoader loader = new URLClassLoader(urls("file:///file1", "file:///file2"), null);

        // When
        Set<String> elements = fullClasspathFor(loader);

        // Then
        assertThat(elements).contains(pathTo("file1"), pathTo("file2"));
    }

    @Test
    void shouldWorkForClassLoaderWithSingleParent() throws Exception {
        // Given
        ClassLoader parent = new URLClassLoader(urls("file:///file1", "file:///file2"), null);
        ClassLoader child = new URLClassLoader(urls("file:///file3"), parent);

        // When
        Set<String> elements = fullClasspathFor(child);

        // Then
        assertThat(elements).contains(pathTo("file1"), pathTo("file2"), pathTo("file3"));
    }

    @Test
    void shouldWorkForClassLoaderHierarchy() throws Exception {
        // Given
        ClassLoader loader1 = new URLClassLoader(urls("file:///file1"), null);
        ClassLoader loader2 = new URLClassLoader(urls("file:///file2"), loader1);
        ClassLoader loader3 = new URLClassLoader(urls("file:///file3"), loader2);
        ClassLoader loader4 = new URLClassLoader(urls("file:///file4"), loader3);

        // When
        Set<String> elements = fullClasspathFor(loader4);

        // Then
        assertThat(elements).contains(pathTo("file1"), pathTo("file2"), pathTo("file3"), pathTo("file4"));
    }

    @Test
    void shouldReturnCorrectClasspathString() throws Exception {
        // Given
        ClassLoader parent = new URLClassLoader(urls("file:///foo"), null);
        ClassLoader child = new URLClassLoader(urls("file:///bar"), parent);

        // When
        String classpath = fullClasspathStringFor(child);

        // Then
        assertThat(classpath).contains(pathTo("bar") + File.pathSeparator + pathTo("foo"));
    }

    private static URL[] urls(String... strings) throws MalformedURLException {
        URL[] urls = new URL[strings.length];
        for (int i = 0; i < strings.length; i++) {
            urls[i] = new URL(strings[i]);
        }
        return urls;
    }

    private static String pathTo(String fileName) throws IOException {
        File currentDir = new File(".").getCanonicalFile();
        File root = currentDir.getParentFile().getCanonicalFile();
        while (root.getParentFile() != null) {
            root = root.getParentFile().getCanonicalFile();
        }
        return new File(root, fileName).getCanonicalPath();
    }
}
