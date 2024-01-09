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
package org.neo4j.cloud.storage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Maps;
import org.neo4j.function.Predicates;

/**
 * The base class for representing the basic path attributes of a {@link StoragePath}
 */
public abstract class StoragePathAttributes implements BasicFileAttributes {

    private static final List<Method> ATTRIBUTES = Stream.of(
                    "size",
                    "lastModifiedTime",
                    "lastAccessTime",
                    "creationTime",
                    "isRegularFile",
                    "isDirectory",
                    "isSymbolicLink",
                    "isOther",
                    "fileKey")
            .map(name -> {
                try {
                    return BasicFileAttributes.class.getMethod(name);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            })
            .toList();

    protected final StoragePath path;

    protected StoragePathAttributes(StoragePath path) {
        this.path = path;
    }

    @Override
    public FileTime lastAccessTime() {
        return creationTime();
    }

    @Override
    public FileTime lastModifiedTime() {
        return creationTime();
    }

    @Override
    public boolean isRegularFile() {
        return !path.isDirectory();
    }

    @Override
    public boolean isDirectory() {
        return path.isDirectory();
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    /**
     * @return the named path attributes of a {@link StoragePath} as a map
     */
    public Map<String, Object> asMap() {
        return asMap(Predicates.alwaysTrue());
    }

    /**
     * @param attributeFilter the attributes to include
     * @return the filtered path attributes of a {@link StoragePath} as a map
     */
    public Map<String, Object> asMap(Predicate<String> attributeFilter) {
        final var self = this;
        final var attrs = Maps.mutable.<String, Object>empty();
        ATTRIBUTES.stream()
                .filter(method -> attributeFilter.test(method.getName()))
                .forEach(method -> {
                    try {
                        attrs.put(method.getName(), method.invoke(self));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
        return attrs;
    }
}
