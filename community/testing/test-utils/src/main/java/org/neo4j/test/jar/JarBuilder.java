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
package org.neo4j.test.jar;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.attribute.MethodAttributeAppender;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * Utility to create jar files containing classes from the current classpath.
 */
public final class JarBuilder {
    public static byte[] classCompiledBytes(String fileName) throws IOException {
        try (InputStream in = JarBuilder.class.getClassLoader().getResourceAsStream(fileName)) {
            requireNonNull(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (in.available() > 0) {
                out.write(in.read());
            }

            return out.toByteArray();
        }
    }

    public static void createJarFor(Path pth, Class<?>... classes) {
        assert classes.length > 0;
        var file = pth.toFile();
        try {
            try (var cls = subclass(classes[0])) {
                cls.toJar(file);
            }
            for (int i = 1; i < classes.length; i++) {
                try (var cls = subclass(classes[i])) {
                    cls.inject(file);
                }
            }
        } catch (IOException exc) {
            throw new RuntimeException("Could not write %s to %s.".formatted(Arrays.toString(classes), pth), exc);
        }
    }

    private static DynamicType.Unloaded<?> subclass(Class<?> cls) {
        // To avoid that the classes we attempt to load are already class-loaded by the application classloader when
        // we refer to them by name, we subclass them and provide a new unloaded class with the same methods, and
        // annotations.
        return new ByteBuddy()
                .subclass(cls)
                .method(ElementMatchers.isDeclaredBy(cls))
                .intercept( // Proxy all method calls declared by the original class to the original class
                        SuperMethodCall.INSTANCE)
                .attribute( // Instrument the methods with the annotations of the original class
                        MethodAttributeAppender.ForInstrumentedMethod.INCLUDING_RECEIVER)
                .make();
    }
}
