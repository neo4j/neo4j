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

import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.values.AnyValue;

public interface ProcedureView {

    ProcedureHandle procedure(QualifiedName name) throws ProcedureException;

    UserFunctionHandle function(QualifiedName name);

    UserFunctionHandle aggregationFunction(QualifiedName name);

    Set<ProcedureSignature> getAllProcedures();

    Stream<UserFunctionSignature> getAllNonAggregatingFunctions();

    Stream<UserFunctionSignature> getAllAggregatingFunctions();

    RawIterator<AnyValue[], ProcedureException> callProcedure(
            Context ctx, int id, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException;

    AnyValue callFunction(Context ctx, int id, AnyValue[] input) throws ProcedureException;

    UserAggregationReducer createAggregationFunction(Context ctx, int id) throws ProcedureException;

    int[] getProcedureIds(String procedureGlobbing);

    int[] getAdminProcedureIds();

    int[] getFunctionIds(String functionGlobbing);

    int[] getAggregatingFunctionIds(String functionGlobbing);

    long signatureVersion();
}
