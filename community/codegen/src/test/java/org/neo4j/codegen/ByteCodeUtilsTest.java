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
package org.neo4j.codegen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.codegen.ByteCodeUtils.assertMethodExists;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.TypeReference.extending;
import static org.neo4j.codegen.TypeReference.typeParameter;
import static org.neo4j.codegen.TypeReference.typeReference;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.neo4j.values.storable.Values;

class ByteCodeUtilsTest {
    @Test
    void shouldTranslateTypeNames() {
        // primitive types
        assertTypeName(int.class, "I");
        assertTypeName(byte.class, "B");
        assertTypeName(short.class, "S");
        assertTypeName(char.class, "C");
        assertTypeName(float.class, "F");
        assertTypeName(double.class, "D");
        assertTypeName(boolean.class, "Z");
        assertTypeName(void.class, "V");

        // primitive array types
        assertTypeName(int[].class, "[I");
        assertTypeName(byte[].class, "[B");
        assertTypeName(short[].class, "[S");
        assertTypeName(char[].class, "[C");
        assertTypeName(float[].class, "[F");
        assertTypeName(double[].class, "[D");
        assertTypeName(boolean[].class, "[Z");

        // reference type
        assertTypeName(String.class, "Ljava/lang/String;");

        // reference array type
        assertTypeName(String[].class, "[Ljava/lang/String;");

        // nested arrays
        assertTypeName(int[][].class, "[[I");
        assertTypeName(byte[][].class, "[[B");
        assertTypeName(short[][].class, "[[S");
        assertTypeName(char[][].class, "[[C");
        assertTypeName(float[][].class, "[[F");
        assertTypeName(double[][].class, "[[D");
        assertTypeName(boolean[][].class, "[[Z");

        assertTypeName(String[][].class, "[[Ljava/lang/String;");
    }

    @Test
    void validByteCodeName() {
        assertThat(ByteCodeUtils.byteCodeName(typeReference(String.class))).isEqualTo("java/lang/String");
        assertThat(ByteCodeUtils.byteCodeName(typeReference(String[].class))).isEqualTo("[Ljava/lang/String;");
        assertThat(ByteCodeUtils.byteCodeName(typeReference(ByteCodeUtils.class)))
                .isEqualTo("org/neo4j/codegen/ByteCodeUtils");
        assertThat(ByteCodeUtils.byteCodeName(typeReference(Values.class)))
                .isEqualTo("org/neo4j/values/storable/Values");
    }

    @Test
    void shouldSupportArrayNestingInClassName() {
        assertThat(ByteCodeUtils.className(typeReference(String.class))).isEqualTo("java.lang.String");
        assertThat(ByteCodeUtils.className(typeReference(String[].class))).isEqualTo("[Ljava.lang.String;");
        assertThat(ByteCodeUtils.className(typeReference(String[][].class))).isEqualTo("[[Ljava.lang.String;");
    }

    @Test
    void shouldDescribeMethodWithNoParameters() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = method(boolean.class, "foo").build(owner);

        // WHEN
        String description = desc(declaration);

        // THEN
        assertThat(description).isEqualTo("()Z");
    }

    @Test
    void shouldDescribeMethodWithParameters() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = method(
                        List.class, "foo", param(String.class, "string"), param(char[].class, "chararray"))
                .build(owner);

        // WHEN
        String description = desc(declaration);

        // THEN
        assertThat(description).isEqualTo("(Ljava/lang/String;[C)Ljava/util/List;");
    }

    @Test
    void signatureShouldBeNullWhenNotGeneric() {
        // GIVEN
        TypeReference reference = typeReference(String.class);

        // WHEN
        String signature = signature(reference);

        // THEN
        assertNull(signature);
    }

    @Test
    void signatureShouldBeCorrectWhenGeneric() {
        // GIVEN
        TypeReference reference = TypeReference.parameterizedType(List.class, String.class);

        // WHEN
        String signature = signature(reference);

        // THEN
        assertThat(signature).isEqualTo("Ljava/util/List<Ljava/lang/String;>;");
    }

    @Test
    void methodSignatureShouldBeNullWhenNotGeneric() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = method(
                        String.class, "foo", param(String.class, "string"), param(char[].class, "chararray"))
                .build(owner);

        // WHEN
        String signature = signature(declaration);

        // THEN
        assertNull(signature);
    }

    @Test
    void methodSignatureShouldBeCorrectWhenGeneric() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = method(
                        TypeReference.parameterizedType(List.class, String.class), "foo", param(String.class, "string"))
                .build(owner);

        // WHEN
        String signature = signature(declaration);

        // THEN
        assertThat(signature).isEqualTo("(Ljava/lang/String;)Ljava/util/List<Ljava/lang/String;>;");
    }

    @Test
    void shouldHandleGenericReturnType() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = MethodDeclaration.method(typeParameter("T"), "fail")
                .parameterizedWith("T", extending(Object.class))
                .build(owner);

        // WHEN
        String desc = desc(declaration);
        String signature = signature(declaration);

        // THEN
        assertThat(desc).isEqualTo("()Ljava/lang/Object;");
        assertThat(signature).isEqualTo("<T:Ljava/lang/Object;>()TT;");
    }

    @Test
    void shouldHandleGenericThrows() {
        // GIVEN
        TypeReference owner = typeReference(ByteCodeUtilsTest.class);
        MethodDeclaration declaration = MethodDeclaration.method(
                        void.class,
                        "fail",
                        param(
                                TypeReference.parameterizedType(CodeGenerationTest.Thrower.class, typeParameter("E")),
                                "thrower"))
                .parameterizedWith("E", extending(Exception.class))
                .throwsException(typeParameter("E"))
                .build(owner);

        // WHEN
        String signature = signature(declaration);
        String[] exceptions = exceptions(declaration);
        // THEN
        assertThat(signature)
                .isEqualTo("<E:Ljava/lang/Exception;>(Lorg/neo4j/codegen/CodeGenerationTest$Thrower<TE;>;)V^TE;");
        assertThat(exceptions).isEqualTo(new String[] {"java/lang/Exception"});
    }

    @Test
    void shouldHandleNestedInnerClasses() {
        // Given
        TypeReference innerInner = typeReference(Inner.InnerInner.class);

        // When
        String byteCodeName = ByteCodeUtils.byteCodeName(innerInner);

        // Then
        assertThat(byteCodeName).isEqualTo("org/neo4j/codegen/ByteCodeUtilsTest$Inner$InnerInner");
    }

    @Test
    void assertMethodExistsShouldHandlePrimitiveAndReferenceTypes() {
        List<Class<?>> types = Arrays.asList(
                byte.class,
                char.class,
                short.class,
                int.class,
                long.class,
                float.class,
                double.class,
                boolean.class,
                String.class);

        TypeReference owner = typeReference(Tester.class);
        for (Class<?> type : types) {
            assertMethodExists(methodReference(
                    owner,
                    typeReference(type),
                    type.getSimpleName().toLowerCase(Locale.ROOT) + "Method",
                    typeReference(type)));
        }
    }

    @Test
    void assertMethodExistsShouldFailOnBadMethodName() {
        assertThrows(
                AssertionError.class,
                () -> assertMethodExists(methodReference(
                        typeReference(Tester.class),
                        typeReference(byte.class),
                        "bteMethod",
                        typeReference(byte.class))));
    }

    @Test
    void assertMethodExistsShouldFailOnBadReturnType() {
        assertThrows(
                AssertionError.class,
                () -> assertMethodExists(methodReference(
                        typeReference(Tester.class),
                        typeReference(float.class),
                        "byteMethod",
                        typeReference(byte.class))));
    }

    @Test
    void assertMethodExistsShouldFailOnBadParameterType() {
        assertThrows(
                AssertionError.class,
                () -> assertMethodExists(methodReference(
                        typeReference(Tester.class),
                        typeReference(byte.class),
                        "byteMethod",
                        typeReference(float.class))));
    }

    @Test
    void assertMethodExistsShouldFailOnMissingParameter() {
        assertThrows(
                AssertionError.class,
                () -> assertMethodExists(
                        methodReference(typeReference(Tester.class), typeReference(byte.class), "byteMethod")));
    }

    @Test
    void assertMethodExistsShouldFailOnTooManyParameters() {
        assertThrows(
                AssertionError.class,
                () -> assertMethodExists(methodReference(
                        typeReference(Tester.class),
                        typeReference(byte.class),
                        "byteMethod",
                        typeReference(byte.class),
                        typeReference(byte.class))));
    }

    class Inner {
        class InnerInner {}
    }

    private void assertTypeName(Class<?> type, String expected) {
        // GIVEN
        TypeReference reference = typeReference(type);

        // WHEN
        String byteCodeName = typeName(reference);

        // THEN
        assertThat(byteCodeName).isEqualTo(expected);
    }

    @SuppressWarnings("unused")
    static class Tester {
        public byte byteMethod(byte b) {
            return b;
        }

        public char charMethod(char c) {
            return c;
        }

        public short shortMethod(short s) {
            return s;
        }

        public int intMethod(int i) {
            return i;
        }

        public long longMethod(long l) {
            return l;
        }

        public float floatMethod(float f) {
            return f;
        }

        public double doubleMethod(double d) {
            return d;
        }

        public boolean booleanMethod(boolean b) {
            return b;
        }

        public String stringMethod(String s) {
            return s;
        }
    }
}
