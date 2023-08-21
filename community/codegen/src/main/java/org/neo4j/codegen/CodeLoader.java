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
package org.neo4j.codegen;

import static java.lang.invoke.MethodHandles.Lookup.ClassOption.NESTMATE;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.privateLookupIn;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassLoader which loads new classes that have been compiled in-process.
 */
class CodeLoader extends ClassLoader {
    private final Map<String /*class name*/, ByteCodes> stagedClasses = new HashMap<>();
    private static final MethodHandles.Lookup LOOKUP;

    static {
        try {
            LOOKUP = privateLookupIn(CodeLoader.class, lookup());
        } catch (IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    CodeLoader(ClassLoader parent) {
        super(parent);
    }

    /**
     * Stage compiled classes so that they are ready to load on first use.
     *
     * @param classes classes to load
     * @param visitor visitor which inspects class bytecodes
     */
    synchronized void stageForLoading(Iterable<? extends ByteCodes> classes, ByteCodeVisitor visitor) {
        for (ByteCodes clazz : classes) {
            visitor.visitByteCode(clazz.name(), clazz.bytes().duplicate());
            stagedClasses.put(clazz.name(), clazz);
        }
    }

    @Override
    protected synchronized Class<?> findClass(String name) throws ClassNotFoundException {
        ByteCodes clazz = stagedClasses.remove(name);
        if (clazz == null) {
            throw new ClassNotFoundException(name);
        }
        String packageName = name.substring(0, name.lastIndexOf('.'));
        if (getDefinedPackage(packageName) == null) {
            definePackage(packageName, "", "", "", "", "", "", null);
        }
        return defineClass(name, clazz.bytes(), null);
    }

    protected synchronized Class<?> defineHiddenClass(String name) throws Throwable {
        ByteCodes clazz = stagedClasses.remove(name);
        if (clazz == null) {
            throw new ClassNotFoundException(name);
        }
        return LOOKUP.defineHiddenClass(classBytes(clazz), true, NESTMATE).lookupClass();
    }

    private static byte[] classBytes(ByteCodes clazz) {
        ByteBuffer byteBuffer = clazz.bytes();
        byte[] bytes;
        if (byteBuffer.hasArray()) {
            bytes = byteBuffer.array();
        } else {
            // buffer not backed by an array, copy data into an array
            bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
        }
        return bytes;
    }
}
