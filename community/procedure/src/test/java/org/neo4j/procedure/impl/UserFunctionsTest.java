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
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.numberValue;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;

class UserFunctionsTest {
    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();

    private static final QualifiedName FUNC = new QualifiedName("org", "myfunc");
    private static final QualifiedName FUNC1 = new QualifiedName("org", "myfunc1");
    private static final QualifiedName FUNC2 = new QualifiedName("org", "myfunc2");
    private static final QualifiedName FUNC3 = new QualifiedName("org", "myfunc3");
    private static final QualifiedName AGGR = new QualifiedName("org", "myaggr");
    private final UserFunctionSignature signature =
            functionSignature(FUNC).out(Neo4jTypes.NTAny).build();
    private final CallableUserFunction function = function(signature);
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));

    @Test
    void shouldGetRegisteredFunction() throws Throwable {
        // When
        procs.register(function);
        var view = procs.getCurrentView();

        // Then
        assertThat(view.function(signature.name(), CypherScope.CYPHER_5).signature())
                .isEqualTo(signature);
    }

    @Test
    void shouldGetAllRegisteredFunctions() throws Throwable {
        // When
        procs.register(function(functionSignature(FUNC1).out(Neo4jTypes.NTAny).build()));
        procs.register(function(functionSignature(FUNC2).out(Neo4jTypes.NTAny).build()));
        procs.register(function(functionSignature(FUNC3).out(Neo4jTypes.NTAny).build()));
        var view = procs.getCurrentView();

        // Then
        List<UserFunctionSignature> signatures = Iterables.asList(
                view.getAllNonAggregatingFunctions(CypherScope.CYPHER_5).collect(Collectors.toSet()));
        assertThat(signatures)
                .contains(
                        functionSignature(FUNC1).out(Neo4jTypes.NTAny).build(),
                        functionSignature(FUNC2).out(Neo4jTypes.NTAny).build(),
                        functionSignature(FUNC3).out(Neo4jTypes.NTAny).build());

        // And
        signatures = Iterables.asList(
                view.getAllAggregatingFunctions(CypherScope.CYPHER_5).collect(Collectors.toSet()));
        assertThat(signatures).isEmpty();
    }

    @Test
    void shouldGetRegisteredAggregationFunctions() throws Throwable {
        // When
        procs.register(function(functionSignature(FUNC1).out(Neo4jTypes.NTAny).build()));
        procs.register(function(functionSignature(FUNC2).out(Neo4jTypes.NTAny).build()));
        procs.register(aggregationFunction(
                functionSignature(AGGR).out(Neo4jTypes.NTAny).build()));
        var view = procs.getCurrentView();

        // Then
        List<UserFunctionSignature> signatures = Iterables.asList(
                view.getAllNonAggregatingFunctions(CypherScope.CYPHER_5).collect(Collectors.toSet()));
        assertThat(signatures)
                .contains(
                        functionSignature(FUNC1).out(Neo4jTypes.NTAny).build(),
                        functionSignature(FUNC2).out(Neo4jTypes.NTAny).build());

        // And
        signatures = Iterables.asList(
                view.getAllAggregatingFunctions(CypherScope.CYPHER_5).collect(Collectors.toSet()));
        assertThat(signatures)
                .contains(functionSignature(AGGR).out(Neo4jTypes.NTAny).build());
    }

    @Test
    void shouldCallRegisteredFunction() throws Throwable {
        // Given
        procs.register(function);
        var view = procs.getCurrentView();
        int functionId = view.function(signature.name(), CypherScope.CYPHER_5).id();

        // When
        Object result = view.callFunction(prepareContext(), functionId, new AnyValue[] {numberValue(1337)});

        // Then
        assertThat(result).isEqualTo(Values.of(1337));
    }

    @Test
    void shouldNotAllowCallingNonExistingFunction() {
        var view = procs.getCurrentView();
        UserFunctionHandle functionHandle = view.function(signature.name(), CypherScope.CYPHER_5);
        ProcedureException exception = assertThrows(
                ProcedureException.class,
                () -> view.callFunction(
                        prepareContext(),
                        functionHandle != null ? functionHandle.id() : -1,
                        new AnyValue[] {numberValue(1337)}));
        assertThat(exception.getMessage())
                .isEqualTo("There is no function with the internal id `-1` registered for this database instance.");
    }

    @Test
    void shouldNotAllowRegisteringConflictingName() throws Throwable {
        // Given
        procs.register(function);

        ProcedureException exception = assertThrows(ProcedureException.class, () -> procs.register(function));
        assertThat(exception.getMessage())
                .isEqualTo("Unable to register function, because the name `org.myfunc` is already in use.");
    }

    @Test
    void shouldSignalNonExistingFunction() {
        // When
        assertThat(procs.getCurrentView().function(signature.name(), CypherScope.CYPHER_5))
                .isNull();
    }

    @Test
    void shouldMakeContextAvailable() throws Throwable {
        // Given
        procs.register(new CallableUserFunction.BasicUserFunction(signature) {
            @Override
            public AnyValue apply(Context ctx, AnyValue[] input) throws ProcedureException {
                return Values.stringValue(ctx.thread().getName());
            }
        });
        var view = procs.getCurrentView();

        Context ctx = prepareContext();
        int functionId = view.function(signature.name(), CypherScope.CYPHER_5).id();

        // When
        Object result = view.callFunction(ctx, functionId, new AnyValue[0]);

        // Then
        assertThat(result).isEqualTo(Values.stringValue(Thread.currentThread().getName()));
    }

    private CallableUserFunction function(UserFunctionSignature signature) {
        return new CallableUserFunction.BasicUserFunction(signature) {
            @Override
            public AnyValue apply(Context ctx, AnyValue[] input) {
                return input[0];
            }
        };
    }

    private CallableUserAggregationFunction aggregationFunction(UserFunctionSignature signature) {
        return new CallableUserAggregationFunction.BasicUserAggregationFunction(signature) {
            @Override
            public UserAggregator create(Context ctx) {
                return null;
            }
        };
    }

    private Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }
}
