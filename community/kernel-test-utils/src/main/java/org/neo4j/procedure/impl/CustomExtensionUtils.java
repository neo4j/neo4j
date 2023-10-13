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
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.test.jar.JarBuilder;

public final class CustomExtensionUtils {

    private CustomExtensionUtils() {}

    public static String LOG_MARKER() {
        return CustomExtension.MESSAGE;
    }

    public static void createProcedureJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(path))) {
            writeClass(jarOut, redefineToPublic(CustomProcedures.class));
        }
    }

    public static void createExtensionJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(path))) {
            var redefinedExtension = redefineToPublic(CustomExtensionFactory.class);
            writeClass(jarOut, redefinedExtension);
            writeClass(jarOut, redefineToPublic(CustomProcedures.class));
            addService(
                    jarOut,
                    ExtensionFactory.class.getCanonicalName(),
                    redefinedExtension.getTypeDescription().getCanonicalName());
        }
    }

    private static <T> DynamicType.Unloaded<T> redefineToPublic(Class<T> cls) {
        return new ByteBuddy()
                .redefine(cls)
                .name(cls.getName() + "Public")
                .modifiers(Modifier.PUBLIC)
                .make();
    }

    private static void writeClass(JarOutputStream stream, DynamicType.Unloaded<?> redefined) throws IOException {
        stream.putNextEntry(
                new ZipEntry(toFilename(redefined.getTypeDescription().getCanonicalName())));
        stream.write(redefined.getBytes());
        stream.closeEntry();
    }

    private static void writeClass(JarOutputStream stream, Class<?> cls) throws IOException {
        var filename = toFilename(cls.getCanonicalName());
        stream.putNextEntry(new ZipEntry(filename));
        stream.write(JarBuilder.classCompiledBytes(filename));
        stream.closeEntry();
    }

    private static String toFilename(String canonicalName) {
        return canonicalName.replace('.', '/') + ".class";
    }

    private static void addService(JarOutputStream stream, String iface, String cls) throws IOException {
        stream.putNextEntry(new ZipEntry("META-INF/services/" + iface));
        stream.write(cls.getBytes(StandardCharsets.UTF_8));
    }
}
