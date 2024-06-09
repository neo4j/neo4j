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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CypherVersionScope;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@SuppressWarnings({"WeakerAccess", "unused"})
public class UserAggregationFunctionTest {
    private ProcedureCompiler procedureCompiler;
    private ComponentRegistry components;
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));

    @BeforeEach
    void setUp() {
        components = new ComponentRegistry();
        procedureCompiler = new ProcedureCompiler(
                new TypeCheckers(), components, components, NullLog.getInstance(), ProcedureConfig.DEFAULT);
    }

    @Test
    void shouldCompileAggregationFunction() throws Throwable {
        // When
        List<CallableUserAggregationFunction> function = compile(SingleAggregationFunction.class);

        // Then
        assertEquals(1, function.size());
        assertThat(function.get(0).signature())
                .isEqualTo(functionSignature(new QualifiedName("org", "neo4j", "procedure", "impl", "collectCool"))
                        .in("name", Neo4jTypes.NTString)
                        .out(Neo4jTypes.NTList(Neo4jTypes.NTAny))
                        .build());
    }

    @Test
    void shouldRunAggregationFunction() throws Throwable {
        // Given
        CallableUserAggregationFunction func =
                compile(SingleAggregationFunction.class).get(0);

        // When
        var aggregator = func.createReducer(prepareContext());
        var updater = aggregator.newUpdater();

        updater.update(new AnyValue[] {stringValue("Harry")});
        updater.update(new AnyValue[] {stringValue("Bonnie")});
        updater.update(new AnyValue[] {stringValue("Sally")});
        updater.update(new AnyValue[] {stringValue("Clyde")});
        updater.applyUpdates();

        // Then
        assertThat(aggregator.result()).isEqualTo(VirtualValues.list(stringValue("Bonnie"), stringValue("Clyde")));
    }

    @Test
    void shouldInjectLogging() throws KernelException {
        // Given
        InternalLog log = spy(InternalLog.class);
        components.register(InternalLog.class, ctx -> log);
        CallableUserAggregationFunction function = procedureCompiler
                .compileAggregationFunction(LoggingFunction.class)
                .get(0);

        // When
        var aggregator = function.createReducer(prepareContext());
        var updater = aggregator.newUpdater();
        updater.update(new AnyValue[] {});
        updater.applyUpdates();
        aggregator.result();

        // Then
        verify(log).debug("1");
        verify(log).info("2");
        verify(log).warn("3");
        verify(log).error("4");
    }

    @Test
    void shouldIgnoreClassesWithNoFunctions() throws Throwable {
        // When
        List<CallableUserAggregationFunction> functions = compile(PrivateConstructorButNoFunctions.class);

        // Then
        assertEquals(0, functions.size());
    }

    @Test
    void shouldRunClassWithMultipleFunctionsDeclared() throws Throwable {
        // Given
        List<CallableUserAggregationFunction> compiled = compile(MultiFunction.class);
        CallableUserAggregationFunction f1 = compiled.get(0);
        CallableUserAggregationFunction f2 = compiled.get(1);

        // When
        var f1Aggregator = f1.createReducer(prepareContext());
        var f1Updater = f1Aggregator.newUpdater();
        f1Updater.update(new AnyValue[] {stringValue("Bonnie")});
        f1Updater.update(new AnyValue[] {stringValue("Clyde")});
        f1Updater.applyUpdates();
        var f2Aggregator = f2.createReducer(prepareContext());
        var f2Updater = f2Aggregator.newUpdater();
        f2Updater.update(new AnyValue[] {stringValue("Bonnie"), longValue(1337L)});
        f2Updater.update(new AnyValue[] {stringValue("Bonnie"), longValue(42L)});
        f2Updater.applyUpdates();

        // Then
        assertThat(f1Aggregator.result()).isEqualTo(VirtualValues.list(stringValue("Bonnie"), stringValue("Clyde")));
        assertThat(((MapValue) f2Aggregator.result()).get("Bonnie")).isEqualTo(longValue(1337L));
    }

    @Test
    void shouldGiveHelpfulErrorOnConstructorThatRequiresArgument() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(WeirdConstructorFunction.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Unable to find a usable public no-argument constructor in the class `WeirdConstructorFunction`. Please add a "
                                + "valid, public constructor, recompile the class and try again.");
    }

    @Test
    void shouldGiveHelpfulErrorOnNoPublicConstructor() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(PrivateConstructorFunction.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Unable to find a usable public no-argument constructor in the class `PrivateConstructorFunction`. Please add "
                                + "a valid, public constructor, recompile the class and try again.");
    }

    @Test
    void shouldNotAllowVoidOutput() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithVoidOutput.class));
        assertThat(exception.getMessage()).startsWith("Don't know how to map `void` to the Neo4j Type System.");
    }

    @Test
    void shouldNotAllowNonVoidUpdate() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithNonVoidUpdate.class));
        assertThat(exception.getMessage())
                .isEqualTo("Update method 'update' in VoidOutput has type 'long' but must have return type 'void'.");
    }

    @Test
    void shouldNotAllowMissingAnnotations() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithMissingAnnotations.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Class 'MissingAggregator' must contain methods annotated with both '@UserAggregationResult' as well as '@UserAggregationUpdate'.");
    }

    @Test
    void shouldNotAllowMultipleUpdateAnnotations() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithDuplicateUpdateAnnotations.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Class 'MissingAggregator' contains multiple methods annotated with '@UserAggregationUpdate'.");
    }

    @Test
    void shouldNotAllowMultipleResultAnnotations() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithDuplicateResultAnnotations.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Class 'MissingAggregator' contains multiple methods annotated with '@UserAggregationResult'.");
    }

    @Test
    void shouldNotAllowNonPublicMethod() {
        ProcedureException exception = assertThrows(ProcedureException.class, () -> compile(NonPublicTestMethod.class));
        assertThat(exception.getMessage())
                .isEqualTo("Aggregation method 'test' in NonPublicTestMethod must be public.");
    }

    @Test
    void shouldNotAllowNonPublicUpdateMethod() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(NonPublicUpdateMethod.class));
        assertThat(exception.getMessage())
                .isEqualTo("Aggregation update method 'update' in InnerAggregator must be public.");
    }

    @Test
    void shouldNotAllowNonPublicResultMethod() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(NonPublicResultMethod.class));
        assertThat(exception.getMessage())
                .isEqualTo("Aggregation result method 'result' in InnerAggregator must be public.");
    }

    @Test
    void shouldGiveHelpfulErrorOnFunctionReturningInvalidType() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithInvalidOutput.class));
        assertThat(exception.getMessage())
                .isEqualTo(String.format("Don't know how to map `char[]` to the Neo4j Type System.%n"
                        + "Please refer to to the documentation for full details.%n"
                        + "For your reference, known types are: [boolean, byte[], double, java.lang.Boolean, "
                        + "java.lang.Double, java.lang.Long, java.lang.Number, java.lang.Object, "
                        + "java.lang.String, java.time.LocalDate, java.time.LocalDateTime, "
                        + "java.time.LocalTime, java.time.OffsetTime, java.time.ZonedDateTime, "
                        + "java.time.temporal.TemporalAmount, java.util.List, java.util.Map, long]"));
    }

    @Test
    void shouldGiveHelpfulErrorOnContextAnnotatedStaticField() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithStaticContextAnnotatedField.class));
        assertThat(exception.getMessage())
                .isEqualTo(String.format(
                        "The field `gdb` in the class named `FunctionWithStaticContextAnnotatedField` is annotated as a @Context field,%n"
                                + "but it is static. @Context fields must be public, non-final and non-static,%n"
                                + "because they are reset each time a procedure is invoked."));
    }

    @Test
    void shouldAllowOverridingProcedureName() throws Throwable {
        // When
        CallableUserAggregationFunction method =
                compile(FunctionWithOverriddenName.class).get(0);

        // Then
        assertEquals(
                "org.mystuff.thisisActuallyTheName", method.signature().name().toString());
    }

    @Test
    void shouldNotAllowOverridingFunctionNameWithoutNamespace() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(FunctionWithSingleName.class));
        assertThat(exception.getMessage())
                .isEqualTo("It is not allowed to define functions in the root namespace. Please define a "
                        + "namespace, e.g. `@UserFunction(\"org.example.com.singleName\")");
    }

    @Test
    void shouldGiveHelpfulErrorOnNullMessageException() throws Throwable {
        // Given
        CallableUserAggregationFunction method =
                compile(FunctionThatThrowsNullMsgExceptionAtInvocation.class).get(0);

        ProcedureException exception = assertThrows(
                ProcedureException.class,
                () -> method.createReducer(prepareContext()).newUpdater().update(new AnyValue[] {}));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Failed to invoke function `org.neo4j.procedure.impl.test`: Caused by: java.lang.IndexOutOfBoundsException");
    }

    @Test
    void shouldLoadWhiteListedFunction() throws Throwable {
        // Given
        procedureCompiler = new ProcedureCompiler(
                new TypeCheckers(),
                components,
                new ComponentRegistry(),
                NullLog.getInstance(),
                new ProcedureConfig(Config.defaults(
                        GraphDatabaseSettings.procedure_allowlist, List.of("org.neo4j.procedure.impl.collectCool"))));

        CallableUserAggregationFunction method =
                compile(SingleAggregationFunction.class).get(0);

        // Expect
        var created = method.createReducer(prepareContext());
        var updater = created.newUpdater();
        updater.update(new AnyValue[] {stringValue("Bonnie")});
        assertThat(created.result()).isEqualTo(VirtualValues.list(stringValue("Bonnie")));
    }

    @Test
    void shouldNotLoadNoneWhiteListedFunction() throws Throwable {
        // Given
        InternalLog log = spy(InternalLog.class);
        procedureCompiler = new ProcedureCompiler(
                new TypeCheckers(),
                components,
                new ComponentRegistry(),
                log,
                new ProcedureConfig(Config.defaults(GraphDatabaseSettings.procedure_allowlist, List.of("WrongName"))));

        List<CallableUserAggregationFunction> method = compile(SingleAggregationFunction.class);
        verify(log)
                .warn(
                        "The function 'org.neo4j.procedure.impl.collectCool' is not on the allowlist and won't be loaded.");
        assertThat(method.size()).isEqualTo(0);
    }

    @Test
    void shouldNotLoadAnyFunctionIfConfigIsEmpty() throws Throwable {
        // Given
        InternalLog log = spy(InternalLog.class);
        procedureCompiler = new ProcedureCompiler(
                new TypeCheckers(),
                components,
                new ComponentRegistry(),
                log,
                new ProcedureConfig(Config.defaults(GraphDatabaseSettings.procedure_allowlist, List.of(""))));

        List<CallableUserAggregationFunction> method = compile(SingleAggregationFunction.class);
        verify(log)
                .warn(
                        "The function 'org.neo4j.procedure.impl.collectCool' is not on the allowlist and won't be loaded.");
        assertThat(method.size()).isEqualTo(0);
    }

    @Test
    void shouldSupportFunctionDeprecation() throws Throwable {
        // Given
        InternalLog log = mock(InternalLog.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler(
                new TypeCheckers(), components, new ComponentRegistry(), log, ProcedureConfig.DEFAULT);

        // When
        List<CallableUserAggregationFunction> funcs =
                procedureCompiler.compileAggregationFunction(FunctionWithDeprecation.class);

        // Then
        verify(log)
                .warn(
                        "Use of @UserAggregationFunction(deprecatedBy) without @Deprecated in org.neo4j.procedure.impl.badFunc");
        verifyNoMoreInteractions(log);
        for (CallableUserAggregationFunction func : funcs) {
            String name = func.signature().name().name();
            func.createReducer(prepareContext());
            switch (name) {
                case "newFunc" -> assertFalse(func.signature().deprecated().isPresent(), "Should not be deprecated");
                case "oldFunc", "badFunc" -> {
                    assertTrue(func.signature().deprecated().isPresent(), "Should be deprecated");
                    assertThat(func.signature().deprecated().get()).isEqualTo("newFunc");
                }
                default -> fail("Unexpected function: " + name);
            }
        }
    }

    @Test
    void shouldRunAggregationFunctionWithInternalTypes() throws Throwable {
        // Given
        CallableUserAggregationFunction func = compile(InternalTypes.class).get(0);

        // When
        var aggregator = func.createReducer(prepareContext());
        var updater = aggregator.newUpdater();

        updater.update(new AnyValue[] {longValue(1)});
        updater.update(new AnyValue[] {longValue(1)});
        updater.update(new AnyValue[] {longValue(1)});
        updater.update(new AnyValue[] {longValue(1)});
        updater.update(new AnyValue[] {longValue(1)});
        updater.applyUpdates();

        // Then
        assertThat(aggregator.result()).isEqualTo(longValue(5));
    }

    @Test
    void shouldOverloadNameWhenDifferentVersions() throws KernelException {
        var pairs = compile(ClassWithVersionedFunctions.class).stream()
                .map((p) -> Tuples.pair(
                        p.signature().name().toString(), p.signature().supportedCypherScopes()));
        assertThat(pairs)
                .containsExactlyInAnyOrder(
                        Tuples.pair("root.chamber", CypherScope.ALL_SCOPES),
                        Tuples.pair("root.echo", Set.of(CypherScope.CYPHER_5)),
                        Tuples.pair("root.echo", Set.of(CypherScope.CYPHER_FUTURE)));
    }

    @Test
    void shouldIgnoreEmptyCypherScopeRequirement() throws KernelException {
        assertThat(compile(EmptyScopeRequirement.class).get(0).signature().supportedCypherScopes())
                .isEqualTo(CypherScope.ALL_SCOPES);
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }

    public static class SingleAggregationFunction {
        @UserAggregationFunction
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }
    }

    public static class CoolPeopleAggregator {
        private List<String> coolPeople = new ArrayList<>();

        @UserAggregationUpdate
        public void update(@Name("name") String name) {
            if (name.equals("Bonnie") || name.equals("Clyde")) {
                coolPeople.add(name);
            }
        }

        @UserAggregationResult
        public List<String> result() {
            return coolPeople;
        }
    }

    public static class FunctionWithVoidOutput {
        @UserAggregationFunction
        public VoidOutput voidOutput() {
            return new VoidOutput();
        }

        public static class VoidOutput {
            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public void result() {}
        }
    }

    public static class FunctionWithMissingAnnotations {
        @UserAggregationFunction
        public MissingAggregator test() {
            return new MissingAggregator();
        }

        public static class MissingAggregator {
            public void update() {}

            public String result() {
                return "test";
            }
        }
    }

    public static class FunctionWithDuplicateUpdateAnnotations {
        @UserAggregationFunction
        public MissingAggregator test() {
            return new MissingAggregator();
        }

        public static class MissingAggregator {
            @UserAggregationUpdate
            public void update1() {}

            @UserAggregationUpdate
            public void update2() {}

            @UserAggregationResult
            public String result() {
                return "test";
            }
        }
    }

    public static class FunctionWithDuplicateResultAnnotations {
        @UserAggregationFunction
        public MissingAggregator test() {
            return new MissingAggregator();
        }

        public static class MissingAggregator {
            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result1() {
                return "test";
            }

            @UserAggregationResult
            public String result2() {
                return "test";
            }
        }
    }

    public static class FunctionWithNonVoidUpdate {
        @UserAggregationFunction
        public VoidOutput voidOutput() {
            return new VoidOutput();
        }

        public static class VoidOutput {
            @UserAggregationUpdate
            public long update() {
                return 42L;
            }

            @UserAggregationResult
            public long result() {
                return 42L;
            }
        }
    }

    public static class LoggingFunction {
        @Context
        public InternalLog log;

        @UserAggregationFunction
        public LoggingAggregator log() {
            return new LoggingAggregator();
        }

        public class LoggingAggregator {
            @UserAggregationUpdate
            public void logAround() {
                log.debug("1");
                log.info("2");
                log.warn("3");
                log.error("4");
            }

            @UserAggregationResult
            public long result() {
                return 1337L;
            }
        }
    }

    public static class MapAggregator {
        private Map<String, Object> map = new HashMap<>();

        @UserAggregationUpdate
        public void update(@Name("name") String name, @Name("value") long value) {
            Long prev = (Long) map.getOrDefault(name, 0L);
            if (value > prev) {
                map.put(name, value);
            }
        }

        @UserAggregationResult
        public Map<String, Object> result() {
            return map;
        }
    }

    public static class MultiFunction {
        @UserAggregationFunction
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }

        @UserAggregationFunction
        public MapAggregator collectMap() {
            return new MapAggregator();
        }
    }

    public static class WeirdConstructorFunction {
        public WeirdConstructorFunction(WeirdConstructorFunction wat) {}

        @UserAggregationFunction
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithInvalidOutput {
        @UserAggregationFunction
        public InvalidAggregator test() {
            return new InvalidAggregator();
        }

        public static class InvalidAggregator {
            @UserAggregationUpdate
            public void update() {
                // dd nothing
            }

            @UserAggregationResult
            public char[] result() {
                return "Testing".toCharArray();
            }
        }
    }

    public static class FunctionWithStaticContextAnnotatedField {
        @Context
        public static GraphDatabaseService gdb;

        @UserAggregationFunction
        public InvalidAggregator test() {
            return new InvalidAggregator();
        }

        public static class InvalidAggregator {

            @UserAggregationUpdate
            public void update() {
                // dd nothing
            }

            @UserAggregationResult
            public String result() {
                return "Testing";
            }
        }
    }

    public static class FunctionThatThrowsNullMsgExceptionAtInvocation {
        @UserAggregationFunction
        public ThrowingAggregator test() {
            return new ThrowingAggregator();
        }

        public static class ThrowingAggregator {
            @UserAggregationUpdate
            public void update() {
                throw new IndexOutOfBoundsException();
            }

            @UserAggregationResult
            public String result() {
                return "Testing";
            }
        }
    }

    public static class PrivateConstructorFunction {
        private PrivateConstructorFunction() {}

        @UserAggregationFunction
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }
    }

    public static class PrivateConstructorButNoFunctions {
        private PrivateConstructorButNoFunctions() {}

        public String thisIsNotAFunction() {
            return null;
        }
    }

    public static class FunctionWithOverriddenName {
        @UserAggregationFunction("org.mystuff.thisisActuallyTheName")
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithSingleName {
        @UserAggregationFunction("singleName")
        public CoolPeopleAggregator collectCool() {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithDeprecation {
        @UserAggregationFunction()
        public CoolPeopleAggregator newFunc() {
            return new CoolPeopleAggregator();
        }

        @Deprecated
        @UserAggregationFunction(deprecatedBy = "newFunc")
        public CoolPeopleAggregator oldFunc() {
            return new CoolPeopleAggregator();
        }

        @UserAggregationFunction(deprecatedBy = "newFunc")
        public CoolPeopleAggregator badFunc() {
            return new CoolPeopleAggregator();
        }
    }

    public static class NonPublicTestMethod {
        @UserAggregationFunction
        InnerAggregator test() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            public String result() {
                return "Testing";
            }
        }
    }

    public static class NonPublicUpdateMethod {
        @UserAggregationFunction
        public InnerAggregator test() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            @UserAggregationUpdate
            void update() {}

            @UserAggregationResult
            public String result() {
                return "Testing";
            }
        }
    }

    public static class NonPublicResultMethod {
        @UserAggregationFunction
        public InnerAggregator test() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            @UserAggregationUpdate
            public void update() {}

            @UserAggregationResult
            String result() {
                return "Testing";
            }
        }
    }

    public static class InternalTypes {
        @UserAggregationFunction
        public InnerAggregator test() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            private long sum;

            @UserAggregationUpdate
            public void update(@Name(value = "in") LongValue in) {
                sum += in.longValue();
            }

            @UserAggregationResult
            public LongValue result() {
                return longValue(sum);
            }
        }
    }

    public static class ClassWithVersionedFunctions {
        @UserAggregationFunction(name = "root.echo")
        @CypherVersionScope(scope = {CypherScope.CYPHER_5})
        public InnerAggregator echo() {
            return new InnerAggregator();
        }

        @UserAggregationFunction(name = "root.echo")
        @CypherVersionScope(scope = {CypherScope.CYPHER_FUTURE})
        public InnerAggregator echoV6() {
            return new InnerAggregator();
        }

        @UserAggregationFunction(name = "root.chamber")
        public InnerAggregator chamber() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            private long sum;

            @UserAggregationUpdate
            public void update(@Name(value = "in") LongValue in) {
                sum += in.longValue();
            }

            @UserAggregationResult
            public LongValue result() {
                return longValue(sum);
            }
        }
    }

    public static class EmptyScopeRequirement {
        @UserAggregationFunction(name = "root.echo")
        @CypherVersionScope(scope = {})
        public InnerAggregator echo() {
            return new InnerAggregator();
        }

        public static class InnerAggregator {
            private long sum;

            @UserAggregationUpdate
            public void update(@Name(value = "in") LongValue in) {
                sum += in.longValue();
            }

            @UserAggregationResult
            public LongValue result() {
                return longValue(sum);
            }
        }
    }

    private List<CallableUserAggregationFunction> compile(Class<?> clazz) throws KernelException {
        return procedureCompiler.compileAggregationFunction(clazz);
    }
}
