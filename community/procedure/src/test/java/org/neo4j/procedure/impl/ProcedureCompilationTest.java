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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTByteArray;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.PI;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

@SuppressWarnings({"unused", "WeakerAccess"})
public class ProcedureCompilationTest {
    private static final AnyValue[] EMPTY = new AnyValue[0];

    private static final QualifiedName FUNC_NAME = new QualifiedName("test", "foo");
    private static final DefaultValueMapper VALUE_MAPPER = new DefaultValueMapper(mock(InternalTransaction.class));
    private static final InternalTransaction TRANSACTION = mock(InternalTransaction.class);
    private static final KernelTransaction KTX = mock(KernelTransaction.class);
    public static final ResourceTracker RESOURCE_TRACKER = mock(ResourceTracker.class);

    private static final ClassLoader defaultClassloader = ProcedureCompilationTest.class.getClassLoader();

    private Context ctx;

    @BeforeEach
    void setUp() throws ProcedureException {
        when(KTX.internalTransaction()).thenReturn(TRANSACTION);
        ctx = mock(Context.class);
        when(ctx.thread()).thenReturn(Thread.currentThread());
        when(ctx.kernelTransaction()).thenReturn(KTX);
        when(ctx.valueMapper()).thenReturn(VALUE_MAPPER);
        when(TRANSACTION.toString()).thenReturn("I'm transaction");
    }

    @Test
    void shouldCallSimpleMethod() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).out(NTInteger).build();
        // When
        CallableUserFunction longMethod = compileFunction(signature, emptyList(), method("longMethod"));

        // Then
        assertEquals(longMethod.apply(ctx, EMPTY), longValue(1337L));
    }

    @Test
    void shouldExposeUserFunctionSignature() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).out(NTInteger).build();
        // When
        CallableUserFunction function = compileFunction(signature, emptyList(), method("longMethod"));

        // Then
        assertEquals(function.signature(), signature);
    }

    @Test
    void functionShouldAccessContext() throws ProcedureException, NoSuchFieldException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).out(NTInteger).build();
        FieldSetter setter1 = createSetter(
                InnerClass.class, "transaction", ctx -> ctx.kernelTransaction().internalTransaction());
        FieldSetter setter2 = createSetter(InnerClass.class, "thread", Context::thread);
        Method longMethod = method(InnerClass.class, "stringMethod");

        // Then
        String threadName = Thread.currentThread().getName();
        assertEquals(
                stringValue("NULL AND NULL"),
                compileFunction(signature, emptyList(), longMethod).apply(ctx, EMPTY));
        assertEquals(
                stringValue("I'm transaction AND NULL"),
                compileFunction(signature, singletonList(setter1), longMethod).apply(ctx, EMPTY));
        assertEquals(
                stringValue("NULL AND " + threadName),
                compileFunction(signature, singletonList(setter2), longMethod).apply(ctx, EMPTY));
        assertEquals(
                stringValue("I'm transaction AND " + threadName),
                compileFunction(signature, asList(setter1, setter2), longMethod).apply(ctx, EMPTY));
    }

    @Test
    void shouldHandleThrowingUDF() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).out(NTInteger).build();

        // When
        CallableUserFunction longMethod = compileFunction(signature, emptyList(), method("throwingLongMethod"));

        // Then
        assertThrows(ProcedureException.class, () -> longMethod.apply(ctx, EMPTY));
    }

    @Test
    void shouldCallMethodWithParameters() throws ProcedureException {
        // Given
        UserFunctionSignature signature = functionSignature(FUNC_NAME)
                .in("l", NTInteger)
                .in("d", NTFloat)
                .in("b", NTBoolean)
                .out(NTString)
                .build();

        // When
        CallableUserFunction concatMethod =
                compileFunction(signature, emptyList(), method("concat", long.class, Double.class, boolean.class));

        // Then
        assertEquals(
                stringValue("421.1true"),
                concatMethod.apply(ctx, new AnyValue[] {longValue(42), doubleValue(1.1), booleanValue(true)}));
    }

    @Test
    void shouldCallMethodWithCompositeParameters() throws ProcedureException {
        // Given
        UserFunctionSignature signature = functionSignature(FUNC_NAME)
                .in("l", NTList(NTAny))
                .out(NTString)
                .build();

        // When
        CallableUserFunction concatMethod = compileFunction(signature, emptyList(), method("concat", List.class));

        // Then
        assertEquals(
                stringValue("421.1true"),
                concatMethod.apply(ctx, new AnyValue[] {list(longValue(42), doubleValue(1.1), TRUE)}));
    }

    @Test
    void shouldHandleNulls() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("b", NTBoolean).out(NTFloat).build();

        // When
        CallableUserFunction nullyMethod =
                compileFunction(signature, emptyList(), method("nullyMethod", Boolean.class));

        // Then
        assertEquals(Values.NO_VALUE, nullyMethod.apply(ctx, new AnyValue[] {TRUE}));
        assertEquals(PI, nullyMethod.apply(ctx, new AnyValue[] {Values.NO_VALUE}));
    }

    @Test
    void shouldHandleNumberOutput() throws ProcedureException {
        // Given
        UserFunctionSignature signature = functionSignature(FUNC_NAME)
                .in("numbers", NTList(NTNumber))
                .out(NTNumber)
                .build();

        // When
        CallableUserFunction sumMethod = compileFunction(signature, emptyList(), method("sum", List.class));

        // Then
        assertEquals(longValue(3), sumMethod.apply(ctx, new AnyValue[] {list(longValue(1), longValue(2))}));
    }

    @Test
    void shouldHandleByteArrays() throws ProcedureException {
        // Given
        UserFunctionSignature signature = functionSignature(FUNC_NAME)
                .in("bytes", NTByteArray)
                .out(NTByteArray)
                .build();

        // When
        CallableUserFunction bytesMethod = compileFunction(signature, emptyList(), method("testMethod", byte[].class));

        // Then
        assertEquals(
                byteArray(new byte[] {1, 2, 3}),
                bytesMethod.apply(ctx, new AnyValue[] {byteArray(new byte[] {1, 2, 3})}));
        assertEquals(byteArray(new byte[] {1, 2, 3}), bytesMethod.apply(ctx, new AnyValue[] {
            list(byteValue((byte) 1), byteValue((byte) 2), byteValue((byte) 3))
        }));
        assertEquals(NO_VALUE, bytesMethod.apply(ctx, new AnyValue[] {NO_VALUE}));
    }

    @Test
    void shouldHandleStrings() throws ProcedureException {
        // Given
        UserFunctionSignature signature = functionSignature(FUNC_NAME)
                .in("string", NTString)
                .out(NTString)
                .build();

        // When
        CallableUserFunction stringMethod = compileFunction(signature, emptyList(), method("testMethod", String.class));

        // Then
        assertEquals(stringValue("good"), stringMethod.apply(ctx, new AnyValue[] {stringValue("good")}));
        assertEquals(NO_VALUE, stringMethod.apply(ctx, new AnyValue[] {NO_VALUE}));
    }

    @Test
    void shouldHandleAllTypes() throws ProcedureException {
        Map<Type, Method> allTypes = typeMaps();
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTAny).out(NTAny).build();

        for (Entry<Type, Method> entry : allTypes.entrySet()) {

            CallableUserFunction function = compileFunction(signature, emptyList(), entry.getValue());
            Type type = entry.getKey();

            if (type.equals(long.class)) {
                assertEquals(longValue(1337L), function.apply(ctx, new AnyValue[] {longValue(1337L)}));
            } else if (type.equals(double.class)) {
                assertEquals(PI, function.apply(ctx, new AnyValue[] {PI}));
            } else if (type.equals(boolean.class)) {
                assertEquals(TRUE, function.apply(ctx, new AnyValue[] {TRUE}));
            } else if (type instanceof Class<?> && AnyValue.class.isAssignableFrom((Class<?>) type)) {
                assertEquals(NO_VALUE, function.apply(ctx, new AnyValue[] {null}));
            } else {
                assertEquals(NO_VALUE, function.apply(ctx, new AnyValue[] {NO_VALUE}));
            }
        }
    }

    @Test
    void shouldCallSimpleProcedure() throws ProcedureException {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .in("in", NTInteger)
                .out(singletonList(inputField("name", NTInteger)))
                .build();
        // When
        CallableProcedure longStream = compileProcedure(signature, emptyList(), method("longStream", long.class));

        // Then
        RawIterator<AnyValue[], ProcedureException> iterator =
                longStream.apply(ctx, new AnyValue[] {longValue(1337L)}, RESOURCE_TRACKER);
        assertArrayEquals(new AnyValue[] {longValue(1337L)}, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldExposeProcedureSignature() throws ProcedureException {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .in("in", NTInteger)
                .out(singletonList(inputField("name", NTInteger)))
                .build();
        // When
        CallableProcedure longStream = compileProcedure(signature, emptyList(), method("longStream", long.class));

        // Then
        assertEquals(signature, longStream.signature());
    }

    @Test
    void procedureShouldAccessContext() throws ProcedureException, NoSuchFieldException {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .in("in", NTString)
                .out(singletonList(inputField("name", NTString)))
                .build();
        FieldSetter setter1 = createSetter(
                InnerClass.class, "transaction", ctx -> ctx.kernelTransaction().internalTransaction());
        FieldSetter setter2 = createSetter(InnerClass.class, "thread", Context::thread);
        Method stringStream = method(InnerClass.class, "stringStream");

        // Then
        String threadName = Thread.currentThread().getName();
        assertEquals(
                stringValue("NULL AND NULL"),
                compileProcedure(signature, emptyList(), stringStream)
                        .apply(ctx, EMPTY, RESOURCE_TRACKER)
                        .next()[0]);
        assertEquals(
                stringValue("I'm transaction AND NULL"),
                compileProcedure(signature, singletonList(setter1), stringStream)
                        .apply(ctx, EMPTY, RESOURCE_TRACKER)
                        .next()[0]);
        assertEquals(
                stringValue("NULL AND " + threadName),
                compileProcedure(signature, singletonList(setter2), stringStream)
                        .apply(ctx, EMPTY, RESOURCE_TRACKER)
                        .next()[0]);
        assertEquals(
                stringValue("I'm transaction AND " + threadName),
                compileProcedure(signature, asList(setter1, setter2), stringStream)
                        .apply(ctx, EMPTY, RESOURCE_TRACKER)
                        .next()[0]);
    }

    @Test
    void shouldHandleThrowingProcedure() throws ProcedureException {
        // Given
        ResourceTracker tracker = mock(ResourceTracker.class);
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .in("in", NTString)
                .out(singletonList(inputField("name", NTString)))
                .build();

        // When
        CallableProcedure longMethod = compileProcedure(signature, emptyList(), method("throwingLongStreamMethod"));

        // Then
        assertThrows(
                ProcedureException.class,
                () -> longMethod.apply(ctx, EMPTY, tracker).next());
        verify(tracker).registerCloseableResource(any(Stream.class));
        verify(tracker).unregisterCloseableResource(any(Stream.class));
    }

    @Test
    void shouldCallVoidProcedure() throws ProcedureException, NoSuchFieldException {
        // Given
        ProcedureSignature signature =
                ProcedureSignature.procedureSignature(FUNC_NAME).build();
        // When
        FieldSetter setter = createSetter(
                InnerClass.class, "transaction", ctx -> ctx.kernelTransaction().internalTransaction());
        CallableProcedure voidMethod =
                compileProcedure(signature, singletonList(setter), method(InnerClass.class, "voidMethod"));

        // Then
        RawIterator<AnyValue[], ProcedureException> iterator = voidMethod.apply(ctx, EMPTY, RESOURCE_TRACKER);
        assertFalse(iterator.hasNext());
        verify(TRANSACTION).traversalDescription();
    }

    @Test
    void shouldHandleNonStaticInnerClasses() throws ProcedureException {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .out(singletonList(inputField("name", NTString)))
                .build();
        // When
        CallableProcedure stringStream =
                compileProcedure(signature, emptyList(), method(InnerClass.class, "innerStream"));

        // Then
        RawIterator<AnyValue[], ProcedureException> iterator = stringStream.apply(ctx, EMPTY, RESOURCE_TRACKER);
        assertArrayEquals(new AnyValue[] {stringValue("hello")}, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldHandleResultClassWithMultipleFields() throws ProcedureException {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(FUNC_NAME)
                .out(List.of(inputField("name", NTString), inputField("value", NTInteger)))
                .build();
        // When
        CallableProcedure stringStream =
                compileProcedure(signature, emptyList(), method(MultiFieldClass.class, "inner"));

        // Then
        RawIterator<AnyValue[], ProcedureException> iterator = stringStream.apply(ctx, EMPTY, RESOURCE_TRACKER);
        assertArrayEquals(new AnyValue[] {stringValue("hello"), longValue(42L)}, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldCallAggregationFunction() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTInteger).out(NTInteger).build();

        // When
        CallableUserAggregationFunction adder = compileAggregation(
                signature,
                emptyList(),
                method("createAdder"),
                method(Adder.class, "update", long.class),
                method(Adder.class, "result"));

        // Then
        var aggregator = adder.createReducer(ctx);
        var updater = aggregator.newUpdater();
        for (int i = 1; i <= 10; i++) {
            updater.update(new AnyValue[] {longValue(i)});
        }
        updater.applyUpdates();
        assertEquals(longValue(55), aggregator.result());
    }

    @Test
    void shouldExposeUserFunctionSignatureOnAggregations() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTInteger).out(NTInteger).build();

        // When
        CallableUserAggregationFunction adder = compileAggregation(
                signature,
                emptyList(),
                method("createAdder"),
                method(Adder.class, "update", long.class),
                method(Adder.class, "result"));

        // Then
        assertEquals(adder.signature(), signature);
    }

    @Test
    void aggregationShouldAccessContext() throws ProcedureException, NoSuchFieldException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTString).out(NTString).build();
        FieldSetter setter = createSetter(InnerClass.class, "thread", Context::thread);
        String threadName = Thread.currentThread().getName();
        UserAggregationReducer aggregator = compileAggregation(
                        signature,
                        singletonList(setter),
                        method(InnerClass.class, "create"),
                        method(InnerClass.Aggregator.class, "update", String.class),
                        method(InnerClass.Aggregator.class, "result"))
                .createReducer(ctx);
        var updater = aggregator.newUpdater();

        // When
        updater.update(new AnyValue[] {stringValue("1:")});
        updater.update(new AnyValue[] {stringValue("2:")});
        updater.update(new AnyValue[] {stringValue("3:")});

        // Then
        assertEquals(
                stringValue(format("1: %s, 2: %s, 3: %s", threadName, threadName, threadName)), aggregator.result());
    }

    @Test
    void shouldHandleThrowingAggregations() throws ProcedureException {
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).out(NTInteger).build();

        UserAggregationReducer aggregator = compileAggregation(
                        signature,
                        emptyList(),
                        method("blackAdder"),
                        method(BlackAdder.class, "update"),
                        method(BlackAdder.class, "result"))
                .createReducer(ctx);

        assertThrows(ProcedureException.class, () -> aggregator.newUpdater().update(EMPTY));
        assertThrows(ProcedureException.class, aggregator::result);
    }

    @Test
    void shouldCallAggregationFunctionWithObject() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTAny).out(NTAny).build();

        // When
        CallableUserAggregationFunction first = compileAggregation(
                signature,
                emptyList(),
                method("first"),
                method(First.class, "update", Object.class),
                method(First.class, "result"));

        // Then
        UserAggregationReducer aggregator = first.createReducer(ctx);
        var updater = aggregator.newUpdater();
        updater.update(new AnyValue[] {longValue(3)});
        updater.update(new AnyValue[] {longValue(4)});
        updater.update(new AnyValue[] {longValue(5)});
        updater.applyUpdates();
        assertEquals(longValue(3), aggregator.result());
    }

    @Test
    void shouldCallResultOnAggregationOnlyOnceOnMapResults() throws ProcedureException {
        // Given
        UserFunctionSignature signature =
                functionSignature(FUNC_NAME).in("in", NTInteger).out(NTMap).build();

        // When
        CallableUserAggregationFunction once = compileAggregation(
                signature,
                emptyList(),
                method("createOnce"),
                method(Once.class, "update", long.class),
                method(Once.class, "result"));

        // Then
        UserAggregationReducer aggregator = once.createReducer(ctx);
        var updater = aggregator.newUpdater();
        updater.update(new AnyValue[] {longValue(42L)});
        updater.applyUpdates();
        assertEquals(asMapValue(Map.of("result", longValue(42))), aggregator.result());
    }

    private <T> FieldSetter createSetter(
            Class<?> owner, String field, ThrowingFunction<Context, T, ProcedureException> provider)
            throws NoSuchFieldException {
        Field declaredField = owner.getDeclaredField(field);
        return new FieldSetter(declaredField, provider);
    }

    private Method method(String name, Class<?>... types) {
        return method(this.getClass(), name, types);
    }

    private Method method(Class<?> owner, String name, Class<?>... types) {
        try {
            return owner.getMethod(name, types);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    public long longMethod() {
        return 1337L;
    }

    public long throwingLongMethod() {
        throw new RuntimeException("wut!");
    }

    public static class InnerClass {
        public Transaction transaction;
        public Thread thread;

        public String stringMethod() {
            String first = transaction != null ? transaction.toString() : "NULL";
            String second = thread != null ? thread.getName() : "NULL";
            return first + " AND " + second;
        }

        public Stream<StringOut> stringStream() {
            String first = transaction != null ? transaction.toString() : "NULL";
            String second = thread != null ? thread.getName() : "NULL";
            return Stream.of(new StringOut(first + " AND " + second));
        }

        public Aggregator create() {
            return new Aggregator();
        }

        public void voidMethod() {
            transaction.traversalDescription();
        }

        public Stream<NonStaticInner> innerStream() {
            return Stream.of(new NonStaticInner());
        }

        public class NonStaticInner {
            public String value = "hello";
        }

        public class Aggregator {
            StringBuilder aggregator = new StringBuilder();
            private boolean first = true;

            public void update(String in) {
                String string = thread != null ? thread.getName() : "NULL";
                if (!first) {
                    aggregator.append(", ");
                }
                first = false;
                aggregator.append(in).append(' ').append(string);
            }

            public String result() {
                return aggregator.toString();
            }
        }
    }

    public static class MultiFieldClass {
        public Stream<Inner> inner() {
            return Stream.of(new Inner());
        }

        public class Inner {
            public String name = "hello";
            public long value = 42;
        }
    }

    public String concat(long l, Double d, boolean b) {
        return l + d.toString() + b;
    }

    public String concat(List<Object> list) {
        StringBuilder builder = new StringBuilder();
        for (Object o : list) {
            builder.append(o);
        }
        return builder.toString();
    }

    public Double nullyMethod(Boolean b) {
        if (b != null) {
            return null;
        } else {
            return Math.PI;
        }
    }

    public Number sum(List<Number> numbers) {
        return numbers.stream().mapToDouble(Number::doubleValue).sum();
    }

    // Exhaustive implementation of supported types
    public String testMethod(String in) {
        return in;
    }

    public long testMethod(long in) {
        return in;
    }

    public Long testMethod(Long in) {
        return in;
    }

    public double testMethod(double in) {
        return in;
    }

    public Double testMethod(Double in) {
        return in;
    }

    public Number testMethod(Number in) {
        return in;
    }

    public boolean testMethod(boolean in) {
        return in;
    }

    public Boolean testMethod(Boolean in) {
        return in;
    }

    public List<Object> testMethod(List<Object> in) {
        return in;
    }

    public Map<String, Object> testMethod(Map<String, Object> in) {
        return in;
    }

    public byte[] testMethod(byte[] bytes) {
        return bytes;
    }

    public Object testMethod(Object in) {
        return in;
    }

    public ZonedDateTime testMethod(ZonedDateTime in) {
        return in;
    }

    public LocalDateTime testMethod(LocalDateTime in) {
        return in;
    }

    public LocalDate testMethod(LocalDate in) {
        return in;
    }

    public OffsetTime testMethod(OffsetTime in) {
        return in;
    }

    public LocalTime testMethod(LocalTime in) {
        return in;
    }

    public TemporalAmount testMethod(TemporalAmount in) {
        return in;
    }

    public LocalDateTimeValue testMethod(LocalDateTimeValue in) {
        return in;
    }

    public LocalTimeValue testMethod(LocalTimeValue in) {
        return in;
    }

    public TimeValue testMethod(TimeValue in) {
        return in;
    }

    public DateValue testMethod(DateValue in) {
        return in;
    }

    public DateTimeValue testMethod(DateTimeValue in) {
        return in;
    }

    public ByteArray testMethod(ByteArray in) {
        return in;
    }

    public AnyValue testMethod(AnyValue in) {
        return in;
    }

    public ListValue testMethod(ListValue in) {
        return in;
    }

    public MapValue testMethod(MapValue in) {
        return in;
    }

    public BooleanValue testMethod(BooleanValue in) {
        return in;
    }

    public NumberValue testMethod(NumberValue in) {
        return in;
    }

    public IntegralValue testMethod(IntegralValue in) {
        return in;
    }

    public FloatingPointValue testMethod(FloatingPointValue in) {
        return in;
    }

    public TextValue testMethod(TextValue in) {
        return in;
    }

    public DurationValue testMethod(DurationValue in) {
        return in;
    }

    private Map<Type, Method> typeMaps() {
        Map<Type, Method> methodHashMap = new HashMap<>();
        methodHashMap.put(String.class, method("testMethod", String.class));
        methodHashMap.put(long.class, method("testMethod", long.class));
        methodHashMap.put(Long.class, method("testMethod", Long.class));
        methodHashMap.put(double.class, method("testMethod", double.class));
        methodHashMap.put(Double.class, method("testMethod", Double.class));
        methodHashMap.put(Number.class, method("testMethod", Number.class));
        methodHashMap.put(boolean.class, method("testMethod", boolean.class));
        methodHashMap.put(Boolean.class, method("testMethod", Boolean.class));
        methodHashMap.put(byte[].class, method("testMethod", byte[].class));
        methodHashMap.put(ByteArray.class, method("testMethod", ByteArray.class));
        methodHashMap.put(List.class, method("testMethod", List.class));
        methodHashMap.put(Map.class, method("testMethod", Map.class));
        methodHashMap.put(Object.class, method("testMethod", Object.class));
        methodHashMap.put(ZonedDateTime.class, method("testMethod", ZonedDateTime.class));
        methodHashMap.put(LocalDateTime.class, method("testMethod", LocalDateTime.class));
        methodHashMap.put(LocalDate.class, method("testMethod", LocalDate.class));
        methodHashMap.put(OffsetTime.class, method("testMethod", OffsetTime.class));
        methodHashMap.put(LocalTime.class, method("testMethod", LocalTime.class));
        methodHashMap.put(TemporalAmount.class, method("testMethod", TemporalAmount.class));
        methodHashMap.put(LocalTimeValue.class, method("testMethod", LocalTimeValue.class));
        methodHashMap.put(LocalDateTimeValue.class, method("testMethod", LocalDateTimeValue.class));
        methodHashMap.put(TimeValue.class, method("testMethod", TimeValue.class));
        methodHashMap.put(DateValue.class, method("testMethod", DateValue.class));
        methodHashMap.put(DateTimeValue.class, method("testMethod", DateTimeValue.class));
        methodHashMap.put(AnyValue.class, method("testMethod", AnyValue.class));
        methodHashMap.put(ListValue.class, method("testMethod", ListValue.class));
        methodHashMap.put(MapValue.class, method("testMethod", MapValue.class));
        methodHashMap.put(BooleanValue.class, method("testMethod", BooleanValue.class));
        methodHashMap.put(NumberValue.class, method("testMethod", NumberValue.class));
        methodHashMap.put(FloatingPointValue.class, method("testMethod", FloatingPointValue.class));
        methodHashMap.put(IntegralValue.class, method("testMethod", IntegralValue.class));
        methodHashMap.put(TextValue.class, method("testMethod", TextValue.class));
        methodHashMap.put(DurationValue.class, method("testMethod", DurationValue.class));

        // safety check, make sure we are testing all types
        Set<Type> types = new TypeCheckers().allTypes();
        for (Type type : types) {
            assertTrue(methodHashMap.containsKey(type), type + " is not being tested!");
        }
        return methodHashMap;
    }

    private CallableUserFunction compileFunction(
            UserFunctionSignature signature, List<FieldSetter> fieldSetters, Method methodToCall)
            throws ProcedureException {
        return ProcedureCompilation.compileFunction(signature, fieldSetters, methodToCall, defaultClassloader);
    }

    private CallableUserAggregationFunction compileAggregation(
            UserFunctionSignature signature,
            List<FieldSetter> fieldSetters,
            Method create,
            Method update,
            Method result)
            throws ProcedureException {
        return ProcedureCompilation.compileAggregation(
                signature, fieldSetters, create, update, result, defaultClassloader);
    }

    private CallableProcedure compileProcedure(
            ProcedureSignature signature, List<FieldSetter> fieldSetters, Method methodToCall)
            throws ProcedureException {
        return ProcedureCompilation.compileProcedure(signature, fieldSetters, methodToCall, defaultClassloader);
    }

    public Stream<LongOut> longStream(long in) {
        return Stream.of(new LongOut(in));
    }

    public Stream<LongOut> throwingLongStreamMethod() {
        return Stream.generate(() -> {
            throw new RuntimeException("wut!");
        });
    }

    public static class LongOut {
        public long field;

        public LongOut(long field) {
            this.field = field;
        }
    }

    public static class StringOut {
        public String field;

        public StringOut(String field) {
            this.field = field;
        }
    }

    public Adder createAdder() {
        return new Adder();
    }

    public BlackAdder blackAdder() {
        return new BlackAdder();
    }

    public First first() {
        return new First();
    }

    public Once createOnce() {
        return new Once();
    }

    public static class Adder {
        private long sum;

        public void update(long in) {
            sum += in;
        }

        public long result() {
            return sum;
        }
    }

    public static class BlackAdder {
        public void update() {
            throw new RuntimeException("you can't update");
        }

        public long result() {
            throw new RuntimeException("you can't result");
        }
    }

    public static class First {
        private Object first;

        public void update(Object in) {
            if (first == null) {
                first = in;
            }
        }

        public Object result() {
            return first;
        }
    }

    public static class Once {
        private boolean consumed;
        private long result;

        public void update(long in) {
            result = in;
        }

        public Map<String, Object> result() {
            checkState(!consumed, "Cannot call result twice");
            consumed = true;
            return Map.of("result", result);
        }
    }
}
