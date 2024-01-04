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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Predicate;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.IOUtils;
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
class ProcedureJarLoader implements AutoCloseable {

    private final ProcedureCompiler compiler;
    private final InternalLog log;

    private final boolean reloadProceduresFromDisk;

    private final Set<Closeable> closeables =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    ProcedureJarLoader(ProcedureCompiler compiler, InternalLog log, boolean reloadProceduresFromDisk) {
        this.compiler = compiler;
        this.log = log;
        this.reloadProceduresFromDisk = reloadProceduresFromDisk;
    }

    Callables loadProceduresFromDir(Path root) throws IOException, KernelException {
        return loadProceduresFromDir(root, Globbing.MATCH_ALL);
    }

    Callables loadProceduresFromDir(Path root, Predicate<String> methodNameFilter) throws IOException, KernelException {
        if (root == null || Files.notExists(root)) {
            return Callables.empty();
        }

        List<Path> jarFiles = findJars(root);

        if (jarFiles.isEmpty()) {
            return Callables.empty();
        }

        var result = ProcedureClassLoader.setup(jarFiles, log, reloadProceduresFromDisk);

        // On Windows, it is not possible to modify files when they are used by a process.
        // To support our test infrastructure, we want to ensure that we properly close
        // all open file handles for procedures when we shutdown. To do this, we keep
        // a weak reference to the classloader, and tidy up in a close-method.
        ProcedureClassLoader loader = result.loader();
        closeables.add(loader);

        Callables out = new Callables();
        for (var entry : result.loadedClasses()) {
            try {
                final var procedures = compiler.compileProcedure(entry.cls(), false, loader, methodNameFilter);
                final var functions = compiler.compileFunction(entry.cls(), false, loader, methodNameFilter);
                final var aggregations = compiler.compileAggregationFunction(entry.cls(), loader, methodNameFilter);

                // Add after compilation, to not taint `target` with a partial success.
                out.addAllProcedures(procedures);
                out.addAllFunctions(functions);
                out.addAllAggregationFunctions(aggregations);
            } catch (IllegalNamingException exc) {
                log.error(
                        "Failed to load procedures from class %s in %s/%s: %s",
                        entry.cls().getSimpleName(),
                        entry.jar().getParent().getFileName(),
                        entry.jar().getFileName(),
                        exc.getMessage());
            }
        }
        return out;
    }

    private static List<Path> findJars(Path root) throws IOException {
        List<Path> jarFiles = new ArrayList<>();
        try (DirectoryStream<Path> list = Files.newDirectoryStream(root, "*.jar")) {
            for (var path : list) {
                jarFiles.add(path);
            }
        }
        // sort paths to compensate for unpredictable order of directory stream
        Collections.sort(jarFiles);
        return jarFiles;
    }

    @Override
    public void close() throws Exception {
        try {
            IOUtils.closeAll(closeables);
        } finally {
            closeables.clear();
        }
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
