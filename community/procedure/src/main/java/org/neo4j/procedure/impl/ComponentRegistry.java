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

import java.util.HashMap;
import java.util.Map;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * Tracks components that can be injected into compiled procedures.
 */
public class ComponentRegistry {
    private final Map<Class<?>, ThrowingFunction<Context, ?, ProcedureException>> suppliers;

    public ComponentRegistry() {
        this(new HashMap<>());
    }

    private ComponentRegistry(Map<Class<?>, ThrowingFunction<Context, ?, ProcedureException>> suppliers) {
        this.suppliers = suppliers;
    }

    @SuppressWarnings("unchecked")
    <T> ThrowingFunction<Context, T, ProcedureException> providerFor(Class<T> type) {
        return (ThrowingFunction<Context, T, ProcedureException>) suppliers.get(type);
    }

    public <T> void register(Class<T> cls, ThrowingFunction<Context, T, ProcedureException> supplier) {
        suppliers.put(cls, supplier);
    }

    public static ComponentRegistry copyOf(ComponentRegistry reg) {
        return new ComponentRegistry(Map.copyOf(reg.suppliers));
    }
}
