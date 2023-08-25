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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.neo4j.collection.AbstractPrefetchingRawIterator;
import org.neo4j.collection.RawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.logging.InternalLog;
import org.neo4j.procedure.impl.NamingRestrictions.IllegalNamingException;
import org.neo4j.string.Globbing;

/**
 * Given the location of a jarfile, reads the contents of the jar and returns compiled {@link CallableProcedure}
 * instances.
 */
class ProcedureJarLoader {

    private final ProcedureCompiler compiler;
    private final InternalLog log;

    ProcedureJarLoader(ProcedureCompiler compiler, InternalLog log) {
        this.compiler = compiler;
        this.log = log;
    }

    Callables loadProceduresFromDir(Path root) throws IOException, KernelException {
        return loadProceduresFromDir(root, Globbing.MATCH_ALL);
    }

    Callables loadProceduresFromDir(Path root, Predicate<String> methodNameFilter) throws IOException, KernelException {
        if (root == null || Files.notExists(root)) {
            return Callables.empty();
        }

        List<Path> jarFiles = new ArrayList<>();
        List<String> failedJarFiles = new ArrayList<>();
        try (DirectoryStream<Path> list = Files.newDirectoryStream(root, "*.jar")) {
            for (Path path : list) {
                if (isInvalidJarFile(path)) {
                    failedJarFiles.add(path.getFileName().toString());
                }
                jarFiles.add(path);
            }
        }

        if (!failedJarFiles.isEmpty()) {
            throw new ZipException(String.format(
                    "Some jar procedure files (%s) are invalid, see log for details.",
                    String.join(", ", failedJarFiles)));
        }

        if (jarFiles.size() == 0) {
            return Callables.empty();
        }

        URL[] jarFilesURLs = jarFiles.stream().map(this::toURL).toArray(URL[]::new);
        URLClassLoader loader = new URLClassLoader(jarFilesURLs, this.getClass().getClassLoader());

        Callables out = new Callables();
        for (Path jarFile : jarFiles) {
            loadProcedures(jarFile, loader, out, methodNameFilter);
        }
        return out;
    }

    private boolean isInvalidJarFile(Path jarFile) {
        try {
            new JarFile(jarFile.toFile(), true, ZipFile.OPEN_READ, JarFile.runtimeVersion()).close();
            return false;
        } catch (IOException e) {
            log.error(String.format("Plugin jar file: %s corrupted.", jarFile));
            return true;
        }
    }

    private void loadProcedures(Path jar, ClassLoader loader, Callables target, Predicate<String> methodNameFilter)
            throws IOException, KernelException {

        // Load all classes from JAR into classloader, to attempt to ensure that we
        // load as many dependencies as we can.
        RawIterator<Class<?>, IOException> classes = listClassesIn(jar, loader);

        while (classes.hasNext()) {
            Class<?> next = classes.next();
            try {
                final var procedures = compiler.compileProcedure(next, false, loader, methodNameFilter);
                final var functions = compiler.compileFunction(next, false, loader, methodNameFilter);
                final var aggregations = compiler.compileAggregationFunction(next, loader, methodNameFilter);

                // Add after compilation, to not taint `target` with a partial success.
                target.addAllProcedures(procedures);
                target.addAllFunctions(functions);
                target.addAllAggregationFunctions(aggregations);
            } catch (IllegalNamingException exc) {
                log.error(
                        "Failed to load procedures from class %s in %s/%s: %s",
                        next.getSimpleName(), jar.getParent().getFileName(), jar.getFileName(), exc.getMessage());
            }
        }
    }

    private URL toURL(Path f) {
        try {
            return f.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("ReturnValueIgnored")
    private RawIterator<Class<?>, IOException> listClassesIn(Path jar, ClassLoader loader) throws IOException {
        JarFile jarFile = new JarFile(jar.toFile(), true, ZipFile.OPEN_READ, JarFile.runtimeVersion());
        Iterator<JarEntry> jarEntries = jarFile.versionedStream().iterator();

        return new AbstractPrefetchingRawIterator<>() {
            @Override
            protected Class<?> fetchNextOrNull() throws IOException {
                try {
                    while (jarEntries.hasNext()) {
                        JarEntry nextEntry = jarEntries.next();

                        String name = nextEntry.getName();
                        if (name.endsWith(".class")) {
                            String className = name.substring(0, name.length() - ".class".length())
                                    .replace('/', '.');

                            try {
                                Class<?> aClass = loader.loadClass(className);
                                // We do getDeclaredMethods and getDeclaredFields to trigger NoClassDefErrors, which
                                // loadClass above does
                                // not do.
                                // This way, even if some of the classes in a jar cannot be loaded, we still check
                                // the others.
                                aClass.getDeclaredMethods();
                                aClass.getDeclaredFields();
                                return aClass;
                            } catch (LinkageError | Exception e) {
                                log.warn(
                                        "Failed to load `%s` from plugin jar `%s`: %s: %s",
                                        className, jar, e.getClass().getName(), e.getMessage());
                            }
                        }
                    }

                    jarFile.close();
                    return null;
                } catch (IOException | RuntimeException e) {
                    jarFile.close();
                    throw e;
                }
            }
        };
    }

    public static class Callables {
        private final List<CallableProcedure> procedures = new ArrayList<>();
        private final List<CallableUserFunction> functions = new ArrayList<>();
        private final List<CallableUserAggregationFunction> aggregationFunctions = new ArrayList<>();

        public void add(CallableProcedure proc) {
            procedures.add(proc);
        }

        public void add(CallableUserFunction func) {
            functions.add(func);
        }

        public List<CallableProcedure> procedures() {
            return procedures;
        }

        public List<CallableUserFunction> functions() {
            return functions;
        }

        public List<CallableUserAggregationFunction> aggregationFunctions() {
            return aggregationFunctions;
        }

        void addAllProcedures(List<CallableProcedure> callableProcedures) {
            procedures.addAll(callableProcedures);
        }

        void addAllFunctions(List<CallableUserFunction> callableFunctions) {
            functions.addAll(callableFunctions);
        }

        void addAllAggregationFunctions(List<CallableUserAggregationFunction> callableFunctions) {
            aggregationFunctions.addAll(callableFunctions);
        }

        private static final Callables EMPTY = new Callables();

        public static Callables empty() {
            return EMPTY;
        }
    }
}
