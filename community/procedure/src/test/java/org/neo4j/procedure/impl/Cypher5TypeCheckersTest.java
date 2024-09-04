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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

class Cypher5TypeCheckersTest {
    private static Stream<Arguments> parameters() {
        return Stream.of(
                of(Object.class, NTAny),
                of(String.class, NTString),
                of(Map.class, NTMap),
                of(List.class, NTList(NTAny)),
                of(listOfListOfMap, NTList(NTList(NTMap))),
                of(boolean.class, NTBoolean),
                of(Number.class, NTNumber),
                of(long.class, NTInteger),
                of(Long.class, NTInteger),
                of(double.class, NTFloat),
                of(Double.class, NTFloat));
    }

    private static Stream<Arguments> defaultValues() {
        return Stream.of(
                of(Object.class, "null", DefaultParameterValue.nullValue(NTAny)),
                of(Object.class, "{}", DefaultParameterValue.ntMap(emptyMap()).castAs(NTAny)),
                of(
                        Object.class,
                        "[]",
                        DefaultParameterValue.ntList(emptyList(), NTAny).castAs(NTAny)),
                of(Object.class, "true", DefaultParameterValue.ntBoolean(true).castAs(NTAny)),
                of(Object.class, "false", DefaultParameterValue.ntBoolean(false).castAs(NTAny)),
                of(Object.class, "42", DefaultParameterValue.ntInteger(42).castAs(NTAny)),
                of(Object.class, "13.37", DefaultParameterValue.ntFloat(13.37).castAs(NTAny)),
                of(Object.class, "foo", DefaultParameterValue.ntString("foo").castAs(NTAny)),
                of(
                        Object.class,
                        "{foo: 'bar'}",
                        DefaultParameterValue.ntMap(Map.of("foo", "bar")).castAs(NTAny)),
                of(
                        Object.class,
                        "['foo', 42, true]",
                        DefaultParameterValue.ntList(List.of("foo", 42L, true), NTAny)
                                .castAs(NTAny)),
                of(Map.class, "null", DefaultParameterValue.ntMap(null)),
                of(Map.class, "{}", DefaultParameterValue.ntMap(emptyMap())),
                of(Map.class, "{foo: 'bar'}", DefaultParameterValue.ntMap(Map.of("foo", "bar"))),
                of(List.class, "null", DefaultParameterValue.ntList(null, NTAny)),
                of(List.class, "[]", DefaultParameterValue.ntList(emptyList(), NTAny)),
                of(List.class, "['foo', 42, true]", DefaultParameterValue.ntList(List.of("foo", 42L, true), NTAny)),
                of(List.class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntList(List.of(1L, 3L, 3L, 7L, 42L), NTAny)),
                of(byte[].class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntByteArray(new byte[] {1, 3, 3, 7, 42})),
                of(byte[].class, "[]", DefaultParameterValue.ntByteArray(new byte[] {})),
                of(byte[].class, "[127, -128]", DefaultParameterValue.ntByteArray(new byte[] {127, -128})),
                of(boolean.class, "true", DefaultParameterValue.ntBoolean(true)),
                of(boolean.class, "false", DefaultParameterValue.ntBoolean(false)),
                of(Boolean.class, "true", DefaultParameterValue.ntBoolean(true)),
                of(Boolean.class, "false", DefaultParameterValue.ntBoolean(false)),
                of(long.class, "42", DefaultParameterValue.ntInteger(42)),
                of(Long.class, "42", DefaultParameterValue.ntInteger(42)),
                of(double.class, "13.37", DefaultParameterValue.ntFloat(13.37)),
                of(Double.class, "13.37", DefaultParameterValue.ntFloat(13.37)),
                of(Number.class, "42", DefaultParameterValue.ntInteger(42).castAs(NTNumber)),
                of(Number.class, "13.37", DefaultParameterValue.ntFloat(13.37).castAs(NTNumber)),
                of(String.class, "null", DefaultParameterValue.ntString("null")),
                of(String.class, "{}", DefaultParameterValue.ntString("{}")),
                of(String.class, "[]", DefaultParameterValue.ntString("[]")),
                of(String.class, "true", DefaultParameterValue.ntString("true")),
                of(String.class, "false", DefaultParameterValue.ntString("false")),
                of(String.class, "42", DefaultParameterValue.ntString("42")),
                of(String.class, "13.37", DefaultParameterValue.ntString("13.37")),
                of(String.class, "foo", DefaultParameterValue.ntString("foo")),
                of(String.class, "{foo: 'bar'}", DefaultParameterValue.ntString("{foo: 'bar'}")),
                of(String.class, "['foo', 42, true]", DefaultParameterValue.ntString("['foo', 42, true]")),
                of(String.class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntString("[1, 3, 3, 7, 42]")));
    }

    private static Stream<Arguments> defaultValuesNegative() {
        return Stream.of(
                of(Map.class, "[]"),
                of(Map.class, "true"),
                of(Map.class, "3.14"),
                of(List.class, "{}"),
                of(List.class, "false"),
                of(List.class, "-42"),
                of(byte[].class, "-42"),
                of(byte[].class, "{}"),
                of(byte[].class, "false"),
                of(boolean.class, "[]"),
                of(boolean.class, "{}"),
                of(Boolean.class, "null"),
                of(Boolean.class, "3.14"),
                of(long.class, "3.14"),
                of(long.class, "[]"),
                of(long.class, "{}"),
                of(long.class, "null"),
                of(long.class, "false"),
                of(Long.class, "3.14"),
                of(Long.class, "[]"),
                of(Long.class, "{}"),
                of(Long.class, "null"),
                of(Long.class, "true"),
                of(double.class, "[]"),
                of(double.class, "{}"),
                of(double.class, "null"),
                of(double.class, "false"),
                of(Double.class, "[]"),
                of(Double.class, "{}"),
                of(Double.class, "null"),
                of(Double.class, "true"),
                of(Number.class, "[]"),
                of(Number.class, "{}"),
                of(Number.class, "null"),
                of(Number.class, "true"),
                of(Number.class, "foo"));
    }

    private static final Type listOfListOfMap = typeOf("listOfListOfMap");

    @ParameterizedTest(name = "{0} to {1}")
    @MethodSource("parameters")
    void shouldDetectCorrectTypeAndMap(Type javaClass, Neo4jTypes.AnyType expected) throws Throwable {
        var actual = new Cypher5TypeCheckers().checkerFor(javaClass).type();
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1} as {0} -> {2}")
    @MethodSource("defaultValues")
    void shouldConvertDefaultValue(Type javaClass, String defaultValue, Object expected) throws Throwable {
        var maybeParsedValue = new Cypher5TypeCheckers().converterFor(javaClass).defaultValue(defaultValue);
        assertTrue(maybeParsedValue.isPresent());
        assertEquals(expected, maybeParsedValue.get());
    }

    @ParameterizedTest(name = "{1} as {0} -> {2}")
    @MethodSource("defaultValuesNegative")
    void shouldFailToConvertInvalidDefaultValue(Type javaClass, String defaultValue) throws Throwable {
        var converter = new Cypher5TypeCheckers().converterFor(javaClass);
        var exception = assertThrows(ProcedureException.class, () -> converter.defaultValue(defaultValue));

        var expectedMessage =
                String.format("Default value `%s` could not be parsed as a %s", defaultValue, converter.type);
        assertEquals(expectedMessage, exception.getMessage());
    }

    @SuppressWarnings("unused")
    interface ClassToGetGenericTypeSignatures {
        void listOfListOfMap(List<List<Map<String, Object>>> arg);
    }

    static Type typeOf(String methodName) {
        for (Method method : ClassToGetGenericTypeSignatures.class.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return method.getGenericParameterTypes()[0];
            }
        }
        throw new AssertionError("No method named " + methodName);
    }
}
