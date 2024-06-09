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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.VirtualValues;

@SuppressWarnings("WeakerAccess")
public class ProcedureWithArgumentsTest {
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));

    @Test
    void shouldCompileSimpleProcedure() throws Throwable {
        // When
        List<CallableProcedure> procedures = compile(ClassWithProcedureWithSimpleArgs.class);

        // Then
        assertEquals(1, procedures.size());
        assertThat(procedures.get(0).signature())
                .isEqualTo(procedureSignature(new QualifiedName("org", "neo4j", "procedure", "impl", "listCoolPeople"))
                        .in("name", Neo4jTypes.NTString)
                        .in("age", Neo4jTypes.NTInteger)
                        .out("name", Neo4jTypes.NTString)
                        .build());
    }

    @Test
    void shouldRunSimpleProcedure() throws Throwable {
        // Given
        CallableProcedure procedure =
                compile(ClassWithProcedureWithSimpleArgs.class).get(0);

        // When
        RawIterator<AnyValue[], ProcedureException> out = procedure.apply(
                prepareContext(), new AnyValue[] {stringValue("Pontus"), longValue(35L)}, EMPTY_RESOURCE_TRACKER);

        // Then
        List<AnyValue[]> collect = asList(out);
        assertThat(collect.get(0)[0]).isEqualTo(stringValue("Pontus is 35 years old."));
    }

    @Test
    void shouldRunGenericProcedure() throws Throwable {
        // Given
        CallableProcedure procedure =
                compile(ClassWithProcedureWithGenericArgs.class).get(0);

        // When
        RawIterator<AnyValue[], ProcedureException> out = procedure.apply(
                prepareContext(),
                new AnyValue[] {
                    VirtualValues.list(
                            stringValue("Roland"), stringValue("Eddie"), stringValue("Susan"), stringValue("Jake")),
                    VirtualValues.list(longValue(1000L), longValue(23L), longValue(29L), longValue(12L))
                },
                EMPTY_RESOURCE_TRACKER);

        // Then
        List<AnyValue[]> collect = asList(out);
        assertThat(collect.get(0)[0]).isEqualTo(stringValue("Roland is 1000 years old."));
        assertThat(collect.get(1)[0]).isEqualTo(stringValue("Eddie is 23 years old."));
        assertThat(collect.get(2)[0]).isEqualTo(stringValue("Susan is 29 years old."));
        assertThat(collect.get(3)[0]).isEqualTo(stringValue("Jake is 12 years old."));
    }

    @Test
    void shouldFailIfMissingAnnotations() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(ClassWithProcedureWithoutAnnotatedArgs.class));
        assertThat(exception.getMessage())
                .isEqualTo(String.format(
                        "Argument at position 0 in method `listCoolPeople` is missing an `@Name` annotation.%n"
                                + "Please add the annotation, recompile the class and try again."));
    }

    @Test
    void shouldFailIfMisplacedDefaultValue() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(ClassWithProcedureWithMisplacedDefault.class));
        assertThat(exception.getMessage())
                .contains(
                        "Non-default argument at position 2 with name c in method defaultValues follows default argument. "
                                + "Add a default value or rearrange arguments so that the non-default values comes first.");
    }

    @Test
    void shouldFailIfWronglyTypedDefaultValue() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> compile(ClassWithProcedureWithBadlyTypedDefault.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        String.format(
                                "Argument `a` at position 0 in `defaultValues` with%n"
                                        + "type `long` cannot be converted to a Neo4j type: Default value `forty-two` could not be parsed as a INTEGER"));
    }

    private Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }

    public static class MyOutputRecord {
        public String name;

        public MyOutputRecord(String name) {
            this.name = name;
        }
    }

    public static class ClassWithProcedureWithSimpleArgs {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople(@Name("name") String name, @Name("age") long age) {
            return Stream.of(new MyOutputRecord(name + " is " + age + " years old."));
        }
    }

    public static class ClassWithProcedureWithGenericArgs {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople(@Name("names") List<String> names, @Name("age") List<Long> ages) {
            Iterator<String> nameIterator = names.iterator();
            Iterator<Long> ageIterator = ages.iterator();
            List<MyOutputRecord> result = new ArrayList<>(names.size());
            while (nameIterator.hasNext()) {
                long age = ageIterator.hasNext() ? ageIterator.next() : -1;
                result.add(new MyOutputRecord(nameIterator.next() + " is " + age + " years old."));
            }
            return result.stream();
        }
    }

    public static class ClassWithProcedureWithoutAnnotatedArgs {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople(String name, int age) {
            return Stream.of(new MyOutputRecord(name + " is " + age + " years old."));
        }
    }

    public static class ClassWithProcedureWithDefaults {
        @Procedure
        public Stream<MyOutputRecord> defaultValues(
                @Name(value = "a", defaultValue = "a") String a,
                @Name(value = "b", defaultValue = "42") long b,
                @Name(value = "c", defaultValue = "3.14") double c) {
            return Stream.empty();
        }
    }

    public static class ClassWithProcedureWithMisplacedDefault {
        @Procedure
        public Stream<MyOutputRecord> defaultValues(
                @Name("a") String a, @Name(value = "b", defaultValue = "42") long b, @Name("c") Object c) {
            return Stream.empty();
        }
    }

    public static class ClassWithProcedureWithBadlyTypedDefault {
        @Procedure
        public Stream<MyOutputRecord> defaultValues(@Name(value = "a", defaultValue = "forty-two") long b) {
            return Stream.empty();
        }
    }

    private List<CallableProcedure> compile(Class<?> clazz) throws KernelException {
        return new ProcedureCompiler(
                        new TypeCheckers(),
                        new ComponentRegistry(),
                        new ComponentRegistry(),
                        NullLog.getInstance(),
                        ProcedureConfig.DEFAULT)
                .compileProcedure(clazz, true);
    }
}
