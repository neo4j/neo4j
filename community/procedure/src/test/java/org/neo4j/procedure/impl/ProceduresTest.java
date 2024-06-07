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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

class ProceduresTest {
    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();
    private final ProcedureSignature signature =
            procedureSignature("org", "myproc").out("name", NTString).build();
    private final CallableProcedure procedure = procedure(signature);
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));

    @Test
    void shouldGetRegisteredProcedure() throws Throwable {
        // When
        procs.register(procedure);

        // Then
        assertThat(procs.getCurrentView()
                        .procedure(signature.name(), CypherScope.CYPHER_5)
                        .signature())
                .isEqualTo(signature);
    }

    @Test
    void shouldGetAllRegisteredProcedures() throws Throwable {
        // When
        procs.register(procedure(
                procedureSignature("org", "myproc1").out("age", NTInteger).build()));
        procs.register(procedure(
                procedureSignature("org", "myproc2").out("age", NTInteger).build()));
        procs.register(procedure(
                procedureSignature("org", "myproc3").out("age", NTInteger).build()));
        var view = procs.getCurrentView();

        // Then
        List<ProcedureSignature> signatures =
                view.getAllProcedures(CypherScope.CYPHER_5).toList();
        assertThat(signatures)
                .contains(
                        procedureSignature("org", "myproc1")
                                .out("age", NTInteger)
                                .build(),
                        procedureSignature("org", "myproc2")
                                .out("age", NTInteger)
                                .build(),
                        procedureSignature("org", "myproc3")
                                .out("age", NTInteger)
                                .build());
    }

    @Test
    void shouldCallRegisteredProcedure() throws Throwable {
        // Given
        procs.register(procedure);
        var view = procs.getCurrentView();
        ProcedureHandle procHandle = view.procedure(signature.name(), CypherScope.CYPHER_5);

        // When
        RawIterator<AnyValue[], ProcedureException> result = view.callProcedure(
                buildContext(dependencyResolver, valueMapper).context(),
                procHandle.id(),
                new AnyValue[] {longValue(1337)},
                EMPTY_RESOURCE_TRACKER);

        // Then
        assertThat(asList(result)).contains(new AnyValue[] {longValue(1337)});
    }

    @Test
    void shouldNotAllowCallingNonExistingProcedure() {
        ProcedureException exception = assertThrows(ProcedureException.class, () -> procs.getCurrentView()
                .procedure(signature.name(), CypherScope.CYPHER_5));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "There is no procedure with the name `org.myproc` registered for this database instance. Please ensure you've spelled the "
                                + "procedure name correctly and that the procedure is properly deployed.");
    }

    @Test
    void shouldNotAllowRegisteringConflictingName() throws Throwable {
        // Given
        procs.register(procedure);

        ProcedureException exception = assertThrows(ProcedureException.class, () -> procs.register(procedure));
        assertThat(exception.getMessage())
                .isEqualTo("Unable to register procedure, because the name `org.myproc` is already in use.");
    }

    @Test
    void shouldNotAllowDuplicateFieldNamesInInput() {
        ProcedureException exception = assertThrows(
                ProcedureException.class,
                () -> procs.register(procedureWithSignature(
                        procedureSignature("asd").in("a", NTAny).in("a", NTAny).build())));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Procedure `asd(a :: ANY, a :: ANY) :: ()` cannot be registered, because it contains a duplicated input field, 'a'. "
                                + "You need to rename or remove one of the duplicate fields.");
    }

    @Test
    void shouldNotAllowDuplicateFieldNamesInOutput() {
        ProcedureException exception = assertThrows(
                ProcedureException.class,
                () -> procs.register(procedureWithSignature(procedureSignature("asd")
                        .out("a", NTAny)
                        .out("a", NTAny)
                        .build())));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Procedure `asd() :: (a :: ANY, a :: ANY)` cannot be registered, because it contains a duplicated output field, 'a'. "
                                + "You need to rename or remove one of the duplicate fields.");
    }

    @Test
    void shouldSignalNonExistingProcedure() {
        ProcedureException exception = assertThrows(ProcedureException.class, () -> procs.getCurrentView()
                .procedure(signature.name(), CypherScope.CYPHER_5));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "There is no procedure with the name `org.myproc` registered for this database instance. Please ensure you've spelled the "
                                + "procedure name correctly and that the procedure is properly deployed.");
    }

    @Test
    void shouldMakeContextAvailable() throws Throwable {
        // Given

        procs.register(new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<AnyValue[], ProcedureException> apply(
                    Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) {
                return RawIterator.<AnyValue[], ProcedureException>of(
                        new AnyValue[] {stringValue(ctx.thread().getName())});
            }
        });
        var view = procs.getCurrentView();

        Context ctx = prepareContext();
        ProcedureHandle procedureHandle = view.procedure(signature.name(), CypherScope.CYPHER_5);

        // When
        RawIterator<AnyValue[], ProcedureException> result =
                view.callProcedure(ctx, procedureHandle.id(), new AnyValue[0], EMPTY_RESOURCE_TRACKER);

        // Then
        assertThat(asList(result))
                .contains(new AnyValue[] {stringValue(Thread.currentThread().getName())});
    }

    private Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }

    private CallableProcedure.BasicProcedure procedureWithSignature(final ProcedureSignature signature) {
        return new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<AnyValue[], ProcedureException> apply(
                    Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) {
                return null;
            }
        };
    }

    private CallableProcedure procedure(ProcedureSignature signature) {
        return new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<AnyValue[], ProcedureException> apply(
                    Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) {
                return RawIterator.<AnyValue[], ProcedureException>of(input);
            }
        };
    }
}
