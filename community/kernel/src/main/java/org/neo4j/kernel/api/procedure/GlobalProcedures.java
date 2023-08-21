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
package org.neo4j.kernel.api.procedure;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.util.VisibleForTesting;

public interface GlobalProcedures {
    void register(CallableProcedure proc) throws ProcedureException;

    void register(CallableUserFunction function) throws ProcedureException;

    void register(CallableUserAggregationFunction function) throws ProcedureException;

    void registerProcedure(Class<?> proc) throws ProcedureException;

    void registerFunction(Class<?> func) throws ProcedureException;

    void registerAggregationFunction(Class<?> func) throws ProcedureException;

    void registerType(Class<?> javaClass, Neo4jTypes.AnyType type);

    <T> void registerComponent(Class<T> cls, ThrowingFunction<Context, T, ProcedureException> provider, boolean safe);

    ProcedureView getCurrentView();

    @VisibleForTesting
    // Allow tests to unregister some procedures so far intended only for tests usages
    void unregister(QualifiedName name);
}
