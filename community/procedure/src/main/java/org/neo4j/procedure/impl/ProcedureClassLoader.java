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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This class provide child-first delegated classloading, based on a URLClassLoader.
 * <p>
 * The reason for needing this behaviour is primarily to prevent the URLClassLoader from delegating the class lookup
 * to the application classloader, when the JAR is a part of the classpath. Without this modification, JARs that
 * are on the classpath will be loaded by the application classloader at startup, and not be able to be reloaded.
 */
class ProcedureClassLoader extends URLClassLoader {

    /**
     * Return a new ProcedureClassLoader from a collection of JARs
     *
     * @param jars Paths to JAR archives
     * @return
     */
    public static ProcedureClassLoader of(Collection<Path> jars) {
        return new ProcedureClassLoader(
                jars.stream()
                        .map(ProcedureClassLoader::toURL)
                        .collect(Collectors.toSet())
                        .toArray(URL[]::new),
                ProcedureClassLoader.getSystemClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (this.getClassLoadingLock(name)) {
            // Test if we have already loaded the class.
            Class<?> klass = this.findLoadedClass(name);
            if (klass == null) {
                try {
                    // Start by attempting to find the class in the URLs
                    klass = findClass(name);
                } catch (ClassNotFoundException exc) {
                    // We could not find the class among the URLs. We now return
                    // to the original behaviour of the URLClassLoader and let it
                    // delegate to its parent when necessary.
                    klass = super.loadClass(name, resolve);
                }
            }

            if (resolve) {
                this.resolveClass(klass);
            }

            return klass;
        }
    }

    private ProcedureClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    private static URL toURL(Path f) {
        try {
            return f.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
