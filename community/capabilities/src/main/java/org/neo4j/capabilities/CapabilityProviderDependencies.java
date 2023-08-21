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
package org.neo4j.capabilities;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.neo4j.exceptions.UnsatisfiedDependencyException;

final class CapabilityProviderDependencies {
    private final Map<Class<?>, Supplier<?>> dependencies;

    CapabilityProviderDependencies() {
        this.dependencies = new HashMap<>();
    }

    <T> void register(Class<T> cls, Supplier<T> supplier) {
        var old = dependencies.putIfAbsent(cls, supplier);
        if (old != null) {
            throw new UnsupportedOperationException(String.format("'%s' is already registered.", cls.getName()));
        }
    }

    @SuppressWarnings("unchecked")
    <T> T get(Class<T> cls) {
        var supplier = this.dependencies.get(cls);
        if (supplier == null) {
            throw new UnsatisfiedDependencyException(cls);
        }

        return (T) supplier.get();
    }
}
