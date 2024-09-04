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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;

@SuppressWarnings("WeakerAccess")
public class ResourceInjectionTest {
    private ProcedureCompiler compiler;
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private InternalLog log = logProvider.getLog(getClass());

    @BeforeEach
    void setUp() {
        ComponentRegistry safeComponents = new ComponentRegistry();
        ComponentRegistry allComponents = new ComponentRegistry();
        safeComponents.register(MyAwesomeAPI.class, ctx -> new MyAwesomeAPI());
        allComponents.register(MyAwesomeAPI.class, ctx -> new MyAwesomeAPI());
        allComponents.register(MyUnsafeAPI.class, ctx -> new MyUnsafeAPI());

        compiler = new ProcedureCompiler(
                new Cypher5TypeCheckers(), safeComponents, allComponents, log, ProcedureConfig.DEFAULT);
    }

    @Test
    void shouldCompileAndRunProcedure() throws Throwable {
        // Given
        CallableProcedure proc =
                compiler.compileProcedure(ProcedureWithInjectedAPI.class, true).get(0);

        // Then
        List<AnyValue[]> out = Iterators.asList(proc.apply(prepareContext(), new AnyValue[0], EMPTY_RESOURCE_TRACKER));

        // Then
        assertThat(out.get(0)).isEqualTo(new AnyValue[] {stringValue("Bonnie")});
        assertThat(out.get(1)).isEqualTo(new AnyValue[] {stringValue("Clyde")});
    }

    @Test
    void shouldFailNicelyWhenUnknownAPI() {
        ProcedureException exception = assertThrows(
                ProcedureException.class, () -> compiler.compileProcedure(ProcedureWithUnknownAPI.class, true));
        assertThat(exception.getMessage())
                .isEqualTo("Unable to set up injection for procedure `ProcedureWithUnknownAPI`, "
                        + "the field `api` has type `class org.neo4j.procedure.impl.ResourceInjectionTest$UnknownAPI` "
                        + "which is not a known injectable component.");
    }

    @Test
    void shouldCompileAndRunUnsafeProcedureUnsafeMode() throws Throwable {
        // Given
        CallableProcedure proc =
                compiler.compileProcedure(ProcedureWithUnsafeAPI.class, true).get(0);

        // Then
        List<AnyValue[]> out = Iterators.asList(proc.apply(prepareContext(), new AnyValue[0], EMPTY_RESOURCE_TRACKER));

        // Then
        assertThat(out.get(0)).isEqualTo(new AnyValue[] {stringValue("Morpheus")});
        assertThat(out.get(1)).isEqualTo(new AnyValue[] {stringValue("Trinity")});
        assertThat(out.get(2)).isEqualTo(new AnyValue[] {stringValue("Neo")});
        assertThat(out.get(3)).isEqualTo(new AnyValue[] {stringValue("Emil")});
    }

    @Test
    void shouldFailNicelyWhenUnsafeAPISafeMode() throws Throwable {
        // When
        List<CallableProcedure> procList = compiler.compileProcedure(ProcedureWithUnsafeAPI.class, false);
        assertThat(logProvider)
                .forClass(getClass())
                .forLevel(WARN)
                .containsMessages("org.neo4j.procedure.impl.listCoolPeople", "unavailable");

        assertThat(procList.size()).isEqualTo(1);
        ProcedureException exception = assertThrows(ProcedureException.class, () -> procList.get(0)
                .apply(prepareContext(), new AnyValue[0], EMPTY_RESOURCE_TRACKER));
        assertThat(exception.getMessage()).contains("org.neo4j.procedure.impl.listCoolPeople", "unavailable");
    }

    @Test
    void shouldCompileAndRunUserFunctions() throws Throwable {
        // Given
        CallableUserFunction proc =
                compiler.compileFunction(FunctionWithInjectedAPI.class, false).get(0);

        // When
        AnyValue out = proc.apply(prepareContext(), new AnyValue[0]);

        // Then
        assertThat(out).isEqualTo(Values.of("[Bonnie, Clyde]"));
    }

    @Test
    void shouldFailNicelyWhenFunctionUsesUnknownAPI() {
        ProcedureException exception = assertThrows(
                ProcedureException.class, () -> compiler.compileFunction(FunctionWithUnknownAPI.class, false));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Unable to set up injection for procedure `FunctionWithUnknownAPI`, "
                                + "the field `api` has type `class org.neo4j.procedure.impl.ResourceInjectionTest$UnknownAPI` which is not a known injectable component.");
    }

    @Test
    void shouldFailNicelyWhenUnsafeAPISafeModeFunction() throws Throwable {
        // When
        List<CallableUserFunction> procList = compiler.compileFunction(FunctionWithUnsafeAPI.class, false);
        assertThat(logProvider)
                .forClass(getClass())
                .forLevel(WARN)
                .containsMessages("org.neo4j.procedure.impl.listCoolPeople", "unavailable");

        assertThat(procList.size()).isEqualTo(1);
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> procList.get(0).apply(prepareContext(), new AnyValue[0]));
        assertThat(exception.getMessage()).contains("org.neo4j.procedure.impl.listCoolPeople", "unavailable");
    }

    @Test
    void shouldCompileAndRunUserAggregationFunctions() throws Throwable {
        // Given
        CallableUserAggregationFunction proc = compiler.compileAggregationFunction(
                        AggregationFunctionWithInjectedAPI.class)
                .get(0);
        // When
        UserAggregationReducer reducer = proc.createReducer(prepareContext());
        var updater = reducer.newUpdater();
        updater.update(new AnyValue[] {});
        AnyValue out = reducer.result();

        // Then
        assertThat(out).isEqualTo(stringValue("[Bonnie, Clyde]"));
    }

    @Test
    void shouldFailNicelyWhenAggregationFunctionUsesUnknownAPI() {
        ProcedureException exception = assertThrows(
                ProcedureException.class,
                () -> compiler.compileAggregationFunction(AggregationFunctionWithUnknownAPI.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Unable to set up injection for procedure `AggregationFunctionWithUnknownAPI`, "
                                + "the field `api` has type `class org.neo4j.procedure.impl.ResourceInjectionTest$UnknownAPI` which is not a known injectable component.");
    }

    @Test
    void shouldFailNicelyWhenUnsafeAPISafeModeAggregationFunction() throws Throwable {
        // When
        List<CallableUserAggregationFunction> procList =
                compiler.compileAggregationFunction(AggregationFunctionWithUnsafeAPI.class);
        assertThat(logProvider)
                .forClass(getClass())
                .forLevel(WARN)
                .containsMessages("org.neo4j.procedure.impl.listCoolPeople", "unavailable");

        assertThat(procList.size()).isEqualTo(1);
        ProcedureException exception = assertThrows(ProcedureException.class, () -> {
            var reducer = procList.get(0).createReducer(prepareContext());
            var updater = reducer.newUpdater();
            updater.update(new AnyValue[] {});
            reducer.result();
        });
        assertThat(exception.getMessage()).contains("org.neo4j.procedure.impl.listCoolPeople", "unavailable");
    }

    @Test
    void shouldFailNicelyWhenAllUsesUnsafeAPI() throws Throwable {
        // When
        compiler.compileFunction(FunctionsAndProcedureUnsafe.class, false);
        compiler.compileProcedure(FunctionsAndProcedureUnsafe.class, false);
        compiler.compileAggregationFunction(FunctionsAndProcedureUnsafe.class);
        // Then
        assertThat(logProvider)
                .forClass(getClass())
                .forLevel(WARN)
                .containsMessages(
                        "org.neo4j.procedure.impl.safeUserFunctionInUnsafeAPIClass is unavailable",
                        "org.neo4j.procedure.impl.listCoolPeopleProcedure is unavailable",
                        "org.neo4j.procedure.impl.listCoolPeople is unavailable");
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }

    public static class MyOutputRecord {
        public String name;

        public MyOutputRecord(String name) {
            this.name = name;
        }
    }

    public static class MyAwesomeAPI {

        List<String> listCoolPeople() {
            return asList("Bonnie", "Clyde");
        }
    }

    public static class UnknownAPI {

        List<String> listCoolPeople() {
            return singletonList("booh!");
        }
    }

    public static class MyUnsafeAPI {
        List<String> listCoolPeople() {
            return asList("Morpheus", "Trinity", "Neo", "Emil");
        }
    }

    public static class ProcedureWithInjectedAPI {
        @Context
        public MyAwesomeAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople() {
            return api.listCoolPeople().stream().map(MyOutputRecord::new);
        }
    }

    public static class FunctionWithInjectedAPI {
        @Context
        public MyAwesomeAPI api;

        @UserFunction
        public String listCoolPeople() {
            return api.listCoolPeople().toString();
        }
    }

    public static class AggregationFunctionWithInjectedAPI {
        @Context
        public MyAwesomeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople() {
            return new VoidOutput(api);
        }

        public static class VoidOutput {
            private MyAwesomeAPI api;

            public VoidOutput(MyAwesomeAPI api) {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result() {
                return api.listCoolPeople().toString();
            }
        }
    }

    public static class ProcedureWithUnknownAPI {
        @Context
        public UnknownAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople() {
            return api.listCoolPeople().stream().map(MyOutputRecord::new);
        }
    }

    public static class FunctionWithUnknownAPI {
        @Context
        public UnknownAPI api;

        @UserFunction
        public String listCoolPeople() {
            return api.listCoolPeople().toString();
        }
    }

    public static class AggregationFunctionWithUnknownAPI {
        @Context
        public UnknownAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople() {
            return new VoidOutput(api);
        }

        public static class VoidOutput {
            private UnknownAPI api;

            public VoidOutput(UnknownAPI api) {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result() {
                return api.listCoolPeople().toString();
            }
        }
    }

    public static class ProcedureWithUnsafeAPI {
        @Context
        public MyUnsafeAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople() {
            return api.listCoolPeople().stream().map(MyOutputRecord::new);
        }
    }

    public static class FunctionWithUnsafeAPI {
        @Context
        public MyUnsafeAPI api;

        @UserFunction
        public String listCoolPeople() {
            return api.listCoolPeople().toString();
        }
    }

    public static class AggregationFunctionWithUnsafeAPI {
        @Context
        public MyUnsafeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople() {
            return new VoidOutput(api);
        }

        public static class VoidOutput {
            private MyUnsafeAPI api;

            public VoidOutput(MyUnsafeAPI api) {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result() {
                return api.listCoolPeople().toString();
            }
        }
    }

    public static class FunctionsAndProcedureUnsafe {
        @Context
        public MyUnsafeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople() {
            return new VoidOutput(api);
        }

        public static class VoidOutput {
            private MyUnsafeAPI api;

            public VoidOutput(MyUnsafeAPI api) {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result() {
                return api.listCoolPeople().toString();
            }
        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeopleProcedure() {
            return api.listCoolPeople().stream().map(MyOutputRecord::new);
        }

        @UserFunction
        public String safeUserFunctionInUnsafeAPIClass() {
            return "a safe function";
        }
    }
}
