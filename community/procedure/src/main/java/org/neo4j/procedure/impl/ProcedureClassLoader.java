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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarFile;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.util.VisibleForTesting;

/**
 * This class provide child-first delegated classloading for JARs that do not inject state into the DBMS.
 * <p>
 * The classloader uses a simple heuristic to determine if the JARs inject state or not. It checks the archive for presence of the file
 * META-INF/services/org.neo4j.kernel.extension.ExtensionFactory. This file is required for the DBMS to be able to service load the extension.
 * If present, the JAR is assumed to store complex state that will not be reloadable. Otherwise, the JAR is assumed to be "stateless", and we
 * thus take care to resolve it in the local classloader, rather than resolving it in the parent classloader as would happen if the archive is
 * on the classpath.
 */
class ProcedureClassLoader extends URLClassLoader {

    /**
     * Return a new ProcedureClassLoader from a collection of JARs
     *
     * @param jars Paths to JAR archives
     * @return the created classloader and a list of classes it has resolved.
     */
    public static Result setup(Collection<Path> jars, InternalLog log, boolean procedureReloadEnabled)
            throws ZipException, ProcedureException {
        return setup(ProcedureClassLoader.class.getClassLoader(), jars, log, procedureReloadEnabled);
    }

    @VisibleForTesting
    static Result setup(ClassLoader parent, Collection<Path> jars, InternalLog log, boolean procedureReloadEnabled)
            throws ZipException, ProcedureException {
        var loader = new ProcedureClassLoader(
                jars.stream().map(ProcedureClassLoader::toURL).toArray(URL[]::new), parent);

        // Resolve classes from JARs with extensions using the normal classloader hierarchy.
        ClassResolver extensionResolver = loader::loadClass;

        // Optionally resolve classes from JARs that do not contain extensions directly in the
        // ProcedureClassLoader to make it possible to reload these.
        ClassResolver extensionlessResolver = (procedureReloadEnabled ? loader::preload : loader::loadClass);

        // Enumerate all classes present in the provided JARs.
        var classes = enumerateClasses(jars, log);
        var entries = resolveAll(classes, log, extensionResolver, extensionlessResolver);

        return new Result(loader, entries);
    }

    public record Result(ProcedureClassLoader loader, List<Entry> loadedClasses) {}

    private ProcedureClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    // Register the classloader as parallel capable to avoid deadlocks. We want to mimic the
    // behaviour of the URLClassLoader and implement the same behaviour.
    static {
        ClassLoader.registerAsParallelCapable();
    }

    private Class<?> preload(String name) throws ClassNotFoundException {
        // Validate that the class was not already loaded in the parent class loader.
        Class<?> klass = super.findLoadedClass(name);
        if (klass != null) {
            return klass;
        }

        // Find the class directly among the JARs of this classloader.
        // If we can't find it for some reason, fail with a ClassNotFoundException.
        klass = findClass(name);

        // Resolve it in this classloader so that we do not attempt to
        // load it in the parent in the future.
        resolveClass(klass);

        return klass;
    }

    private interface ClassResolver {
        Class<?> resolve(String name) throws ClassNotFoundException;
    }

    public record Entry(Path jar, Class<?> cls) {}

    private static List<Entry> resolveAll(
            ClassEnumeration enumeration,
            InternalLog log,
            ClassResolver extensionResolver,
            ClassResolver extensionlessResolver)
            throws ProcedureException {
        var entries = new ArrayList<Entry>();
        // Ordering here is important. Stateless classes should be loaded before stateful to
        // ensure that the classes can be classload:ed again.
        entries.addAll(resolve(enumeration.withoutExtensions, log, extensionlessResolver));
        entries.addAll(resolve(enumeration.withExtensions, log, extensionResolver));
        return entries;
    }

    private static List<Entry> resolve(Map<String, Path> classes, InternalLog log, ClassResolver method)
            throws ProcedureException {

        List<Throwable> exceptions = new ArrayList<>();
        List<Entry> entries = new ArrayList<>();
        for (var entry : classes.entrySet()) {
            var className = entry.getKey();
            var jar = entry.getValue().toAbsolutePath();

            Class<?> klass;
            try {
                klass = method.resolve(entry.getKey());
            } catch (ClassNotFoundException exc) {
                // Should not happen...
                exceptions.add(exc);
                continue;
            } catch (IllegalAccessError exc) {
                // Will likely happen when a different versions of library already has been loaded by the DBMS
                logWarning(log, className, jar, exc);
                exceptions.add(exc);
                continue;
            } catch (NoClassDefFoundError exc) {
                // This error is considered non-fatal in the current implementation,
                // and is triggered e.g. by attempting to classload "META-INF.versions.9.my.package".
                logWarning(log, className, jar, exc);
                continue;
            } catch (LinkageError exc) {
                // Not quite sure what errors trigger this case.
                logWarning(log, className, jar, exc);
                continue;
            }

            // Evaluate the declared attributes of the class, to attempt to produce a LinkageError
            // if the dependency is unmet.
            try {
                //noinspection ResultOfMethodCallIgnored
                klass.getDeclaredClasses();
                //noinspection ResultOfMethodCallIgnored
                klass.getDeclaredMethods();
                //noinspection ResultOfMethodCallIgnored
                klass.getDeclaredFields();

                entries.add(new Entry(jar, klass));
            } catch (LinkageError | Exception exc) {
                logWarning(log, className, jar, exc);
            }
        }

        // Collect exceptions and throw them as a unit
        if (!exceptions.isEmpty()) {
            var exc = new ProcedureException(
                    Status.Procedure.ProcedureRegistrationFailed,
                    "Failed to register procedures for the following reasons:");
            for (var exception : exceptions) {
                exc.addSuppressed(exception);
            }
            throw exc;
        }

        return entries;
    }

    private static void logWarning(InternalLog log, String className, Path jar, Throwable exc) {
        log.warn(
                "Failed to load `%s` from plugin jar `%s`: %s: %s",
                className, jar, exc.getClass().getName(), exc.getMessage());
    }

    private static URL toURL(Path f) {
        try {
            return f.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private record ClassEnumeration(Map<String, Path> withExtensions, Map<String, Path> withoutExtensions) {}

    private static ClassEnumeration enumerateClasses(Collection<Path> jars, InternalLog log) throws ZipException {
        var out = new ClassEnumeration(new HashMap<>(), new HashMap<>());
        var invalidFiles = new ArrayList<String>();

        for (Path pth : jars) {
            try (var jf = open(pth)) {
                var content = listClasses(jf);
                var destination = (content.hasExtension ? out.withExtensions : out.withoutExtensions);
                for (var klass : content.classes()) {
                    destination.put(klass, pth);
                }
            } catch (IOException exc) {
                log.error(String.format("Plugin jar file: %s corrupted.", pth));
                invalidFiles.add(pth.getFileName().toString());
            }
        }

        if (!invalidFiles.isEmpty()) {
            throw new ZipException(String.format(
                    "Some jar procedure files (%s) are invalid, see log for details.",
                    String.join(", ", invalidFiles)));
        }

        return out;
    }

    private record Content(List<String> classes, boolean hasExtension) {}

    private static Content listClasses(JarFile jf) {
        var classes = new ArrayList<String>();
        var it = jf.versionedStream().iterator();

        boolean hasExtension = jf.getEntry("META-INF/services/" + ExtensionFactory.class.getCanonicalName()) != null;

        while (it.hasNext()) {
            var entry = it.next();
            var name = entry.getName();

            if (name.endsWith(".class")) {
                classes.add(qualifiedName(name));
            }
        }

        return new Content(classes, hasExtension);
    }

    private static String qualifiedName(String name) {
        return name.substring(0, name.length() - ".class".length()).replace('/', '.');
    }

    private static JarFile open(Path pth) throws IOException {
        Objects.requireNonNull(pth);

        return new JarFile(pth.toFile(), true, ZipFile.OPEN_READ, JarFile.runtimeVersion());
    }
}
