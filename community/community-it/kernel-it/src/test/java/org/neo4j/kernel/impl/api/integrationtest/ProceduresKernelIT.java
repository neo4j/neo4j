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
package org.neo4j.kernel.impl.api.integrationtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.longValue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

class ProceduresKernelIT extends KernelIntegrationTest {
    private static final QualifiedName PROC = new QualifiedName("example", "exampleProc");
    private final ProcedureSignature signature = procedureSignature(PROC)
            .supportedCypherScopes(CypherScope.CYPHER_5)
            .in("name", NTString)
            .out("name", NTString)
            .build();

    private final ProcedureSignature futureSignature = procedureSignature(PROC)
            .supportedCypherScopes(CypherScope.CYPHER_FUTURE)
            .in("firstName", NTString)
            .in("lastName", NTString)
            .out("name", NTString)
            .build();

    private final CallableProcedure procedure = procedure(signature);
    private final CallableProcedure futureProcedure = procedure(futureSignature);

    @Test
    void shouldGetProcedureByName() throws Throwable {
        // Given
        internalKernel().registerProcedure(procedure);

        // When
        ProcedureSignature found =
                procs().procedureGet(PROC, CypherScope.CYPHER_5).signature();

        // Then
        assertThat(found).isEqualTo(signature);
        commit();
    }

    @Test
    void shouldGetBuiltInProcedureByName() throws Throwable {
        // When
        QualifiedName qn = new QualifiedName("db", "labels");
        ProcedureSignature found =
                procs().procedureGet(qn, CypherScope.CYPHER_5).signature();

        // Then
        assertThat(found)
                .isEqualTo(procedureSignature(qn).out("label", NTString).build());
        commit();
    }

    @Test
    void shouldGetAllProcedures() throws Throwable {
        // Given
        internalKernel().registerProcedure(procedure);
        QualifiedName proc2 = new QualifiedName("example", "exampleProc2");
        QualifiedName proc3 = new QualifiedName("example", "exampleProc3");
        internalKernel()
                .registerProcedure(procedure(
                        procedureSignature(proc2).out("name", NTString).build()));
        internalKernel()
                .registerProcedure(procedure(
                        procedureSignature(proc3).out("name", NTString).build()));

        // When
        List<ProcedureSignature> signatures = newTransaction()
                .procedures()
                .proceduresGetAll(CypherScope.CYPHER_5)
                .toList();

        // Then
        assertThat(signatures)
                .contains(
                        procedure.signature(),
                        procedureSignature(proc2).out("name", NTString).build(),
                        procedureSignature(proc3).out("name", NTString).build());
        commit();
    }

    @Test
    void shouldRefuseToRegisterNonVoidProcedureWithoutOutputs() throws ProcedureException {
        var e = assertThrows(ProcedureException.class, () -> internalKernel()
                .registerProcedure(procedure(procedureSignature(new QualifiedName("example", "exampleProc2"))
                        .build())));
        assertThat(e.getMessage()).isEqualTo("Procedures with zero output fields must be declared as VOID");
    }

    @Test
    void shouldCallReadOnlyProcedure() throws Throwable {
        // Given
        internalKernel().registerProcedure(procedure);

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> found = procs.procedureCallRead(
                    procs.procedureGet(PROC, CypherScope.CYPHER_5).id(),
                    new AnyValue[] {longValue(1337)},
                    ProcedureCallContext.EMPTY);
            // Then
            assertThat(asList(found)).contains(new AnyValue[] {longValue(1337)});
        }

        commit();
    }

    @Test
    void registeredProcedureShouldGetRead() throws Throwable {
        // Given
        internalKernel().registerProcedure(new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<AnyValue[], ProcedureException> apply(
                    Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
                return RawIterator.<AnyValue[], ProcedureException>of(new AnyValue[] {
                    Values.stringValue(ctx.kernelTransaction().dataRead().toString())
                });
            }
        });

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(signature.name(), CypherScope.CYPHER_5).id(),
                    new AnyValue[] {Values.EMPTY_STRING},
                    ProcedureCallContext.EMPTY);

            // Then
            assertNotNull(asList(stream).get(0)[0]);
        }
        commit();
    }

    @Test
    void shouldGetProceduresWithRightScope() throws Throwable {
        // Given
        internalKernel().registerProcedure(procedure);
        internalKernel().registerProcedure(futureProcedure);

        // When
        List<ProcedureSignature> signatures = newTransaction()
                .procedures()
                .proceduresGetAll(CypherScope.CYPHER_5)
                .toList();

        // Then
        assertThat(signatures).contains(procedure.signature());
        commit();
    }

    private static CallableProcedure procedure(final ProcedureSignature signature) {
        return new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<AnyValue[], ProcedureException> apply(
                    Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) {
                return RawIterator.<AnyValue[], ProcedureException>of(input);
            }
        };
    }
}
