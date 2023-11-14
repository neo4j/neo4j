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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.codegen.Expression.add;
import static org.neo4j.codegen.Expression.and;
import static org.neo4j.codegen.Expression.arrayLoad;
import static org.neo4j.codegen.Expression.arraySet;
import static org.neo4j.codegen.Expression.constant;
import static org.neo4j.codegen.Expression.equal;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.isNull;
import static org.neo4j.codegen.Expression.multiply;
import static org.neo4j.codegen.Expression.newArray;
import static org.neo4j.codegen.Expression.newInitializedArray;
import static org.neo4j.codegen.Expression.newInstance;
import static org.neo4j.codegen.Expression.not;
import static org.neo4j.codegen.Expression.notNull;
import static org.neo4j.codegen.Expression.or;
import static org.neo4j.codegen.Expression.subtract;
import static org.neo4j.codegen.Expression.ternary;
import static org.neo4j.codegen.ExpressionTemplate.cast;
import static org.neo4j.codegen.ExpressionTemplate.load;
import static org.neo4j.codegen.ExpressionTemplate.self;
import static org.neo4j.codegen.MethodReference.constructorReference;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.TypeReference.extending;
import static org.neo4j.codegen.TypeReference.parameterizedType;
import static org.neo4j.codegen.TypeReference.typeParameter;
import static org.neo4j.codegen.TypeReference.typeReference;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.codegen.bytecode.ByteCode;
import org.neo4j.codegen.source.SourceCode;

@SuppressWarnings("WeakerAccess")
public abstract class CodeGenerationTest {
    private static final MethodReference RUN = createMethod(Runnable.class, void.class, "run");

    abstract CodeGenerator getGenerator();

    @BeforeEach
    void createGenerator() {
        generator = getGenerator();
    }

    @Test
    void shouldGenerateClass() throws Exception {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            handle = simple.handle();
        }

        // when
        Class<?> aClass = handle.loadClass();

        // then
        assertNotNull(aClass, "null class loaded");
        assertNotNull(aClass.getPackage(), "null package of: " + aClass.getName());
        assertEquals(PACKAGE, aClass.getPackage().getName());
        assertEquals("SimpleClass", aClass.getSimpleName());
    }

    @Test
    void shouldGenerateTwoClassesInTheSamePackage() throws Exception {
        // given
        ClassHandle one;
        ClassHandle two;
        try (ClassGenerator simple = generateClass("One")) {
            one = simple.handle();
        }
        try (ClassGenerator simple = generateClass("Two")) {
            two = simple.handle();
        }

        // when
        Class<?> classOne = one.loadClass();
        Class<?> classTwo = two.loadClass();

        // then
        assertNotNull(classOne.getPackage());
        assertSame(classOne.getPackage(), classTwo.getPackage());
        assertEquals("One", classOne.getSimpleName());
        assertEquals("Two", classTwo.getSimpleName());
    }

    @Test
    void shouldGenerateDefaultConstructor() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass(NamedBase.class, "SimpleClass")) {
            handle = simple.handle();
        }

        // when
        Object instance = constructor(handle.loadClass()).invoke();
        Object constructorCalled =
                instanceMethod(instance, "defaultConstructorCalled").invoke();

        // then
        assertTrue((Boolean) constructorCalled);
    }

    @Test
    void shouldGenerateField() throws Exception {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            simple.field(String.class, "theField");
            handle = simple.handle();
        }

        // when
        Class<?> clazz = handle.loadClass();

        // then
        Field theField = clazz.getDeclaredField("theField");
        assertSame(String.class, theField.getType());
    }

    @Test
    void shouldGenerateParameterizedTypeField() throws Exception {
        // given
        ClassHandle handle;
        TypeReference stringList = TypeReference.parameterizedType(List.class, String.class);
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            simple.field(stringList, "theField");
            handle = simple.handle();
        }

        // when
        Class<?> clazz = handle.loadClass();

        // then
        Field theField = clazz.getDeclaredField("theField");
        assertSame(List.class, theField.getType());
    }

    @Test
    void shouldGenerateMethodReturningFieldValue() throws Throwable {
        assertMethodReturningField(byte.class, (byte) 42);
        assertMethodReturningField(short.class, (short) 42);
        assertMethodReturningField(char.class, (char) 42);
        assertMethodReturningField(int.class, 42);
        assertMethodReturningField(long.class, 42L);
        assertMethodReturningField(float.class, 42F);
        assertMethodReturningField(double.class, 42D);
        assertMethodReturningField(String.class, "42");
        assertMethodReturningField(int[].class, new int[] {42});
        assertMethodReturningField(
                Map.Entry[].class, Collections.singletonMap(42, "42").entrySet().toArray(new Map.Entry[0]));
    }

    @Test
    void shouldGenerateMethodReturningArrayValue() throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {

            simple.generate(MethodTemplate.method(int[].class, "value")
                    .returns(newInitializedArray(typeReference(int.class), constant(1), constant(2), constant(3)))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance = constructor(handle.loadClass()).invoke();

        // then
        assertArrayEquals(
                new int[] {1, 2, 3}, (int[]) instanceMethod(instance, "value").invoke());
    }

    @Test
    void shouldGenerateMethodReturningParameterizedTypeValue() throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            TypeReference stringList = parameterizedType(List.class, String.class);
            simple.generate(MethodTemplate.method(stringList, "value")
                    .returns(Expression.invoke(
                            methodReference(Arrays.class, stringList, "asList", Object[].class),
                            newInitializedArray(typeReference(String.class), constant("a"), constant("b"))))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance = constructor(handle.loadClass()).invoke();

        // then
        assertEquals(Arrays.asList("a", "b"), instanceMethod(instance, "value").invoke());
    }

    @Test
    void shouldGenerateStaticPrimitiveField() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            FieldReference foo = simple.privateStaticFinalField(int.class, "FOO", constant(42));
            try (CodeBlock get = simple.generateMethod(int.class, "get")) {
                get.returns(Expression.getStatic(foo));
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod(handle.newInstance(), "get").invoke();

        // then
        assertEquals(42, foo);
    }

    @Test
    void shouldGenerateStaticReferenceTypeField() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            FieldReference foo = simple.privateStaticFinalField(String.class, "FOO", constant("42"));
            try (CodeBlock get = simple.generateMethod(String.class, "get")) {
                get.returns(Expression.getStatic(foo));
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod(handle.newInstance(), "get").invoke();

        // then
        assertEquals("42", foo);
    }

    @Test
    void shouldGenerateStaticParameterizedTypeField() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            TypeReference stringList = TypeReference.parameterizedType(List.class, String.class);
            FieldReference foo = simple.privateStaticFinalField(
                    stringList,
                    "FOO",
                    Expression.invoke(
                            methodReference(Arrays.class, stringList, "asList", Object[].class),
                            newInitializedArray(
                                    typeReference(String.class), constant("FOO"), constant("BAR"), constant("BAZ"))));
            try (CodeBlock get = simple.generateMethod(stringList, "get")) {
                get.returns(Expression.getStatic(foo));
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod(handle.newInstance(), "get").invoke();

        // then
        assertEquals(Arrays.asList("FOO", "BAR", "BAZ"), foo);
    }

    public interface Thrower<E extends Exception> {
        void doThrow() throws E;
    }

    @Test
    void shouldThrowParameterizedCheckedException() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock fail = simple.generate(MethodDeclaration.method(
                            void.class,
                            "fail",
                            param(TypeReference.parameterizedType(Thrower.class, typeParameter("E")), "thrower"))
                    .parameterizedWith("E", extending(Exception.class))
                    .throwsException(typeParameter("E")))) {
                fail.expression(invoke(fail.load("thrower"), methodReference(Thrower.class, void.class, "doThrow")));
            }
            handle = simple.handle();
        }

        // when
        try {
            instanceMethod(handle.newInstance(), "fail", Thrower.class).invoke((Thrower<IOException>) () -> {
                throw new IOException("Hello from the inside");
            });

            fail("expected exception");
        }
        // then
        catch (IOException e) {
            assertEquals("Hello from the inside", e.getMessage());
        }
    }

    @Test
    void shouldAssignLocalVariable() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock create = simple.generateMethod(
                    SomeBean.class, "createBean", param(String.class, "foo"), param(String.class, "bar"))) {
                create.assign(
                        SomeBean.class,
                        "bean",
                        invoke(newInstance(SomeBean.class), constructorReference(SomeBean.class)));
                create.expression(invoke(
                        create.load("bean"),
                        methodReference(SomeBean.class, void.class, "setFoo", String.class),
                        create.load("foo")));
                create.expression(invoke(
                        create.load("bean"),
                        methodReference(SomeBean.class, void.class, "setBar", String.class),
                        create.load("bar")));
                create.returns(create.load("bean"));
            }
            handle = simple.handle();
        }

        // when
        MethodHandle method = instanceMethod(handle.newInstance(), "createBean", String.class, String.class);
        SomeBean bean = (SomeBean) method.invoke("hello", "world");

        // then
        assertEquals("hello", bean.foo);
        assertEquals("world", bean.bar);
    }

    @Test
    void shouldDeclareAndAssignLocalVariable() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock create = simple.generateMethod(
                    SomeBean.class, "createBean", param(String.class, "foo"), param(String.class, "bar"))) {
                LocalVariable localVariable = create.declare(typeReference(SomeBean.class), "bean");
                create.assign(localVariable, invoke(newInstance(SomeBean.class), constructorReference(SomeBean.class)));
                create.expression(invoke(
                        create.load("bean"),
                        methodReference(SomeBean.class, void.class, "setFoo", String.class),
                        create.load("foo")));
                create.expression(invoke(
                        create.load("bean"),
                        methodReference(SomeBean.class, void.class, "setBar", String.class),
                        create.load("bar")));
                create.returns(create.load("bean"));
            }
            handle = simple.handle();
        }

        // when
        MethodHandle method = instanceMethod(handle.newInstance(), "createBean", String.class, String.class);
        SomeBean bean = (SomeBean) method.invoke("hello", "world");

        // then
        assertEquals("hello", bean.foo);
        assertEquals("world", bean.bar);
    }

    @Test
    void shouldGenerateWhileLoop() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targets"))) {
                try (CodeBlock loop = callEach.whileLoop(
                        invoke(callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    loop.expression(invoke(
                            Expression.cast(
                                    Runnable.class,
                                    invoke(
                                            callEach.load("targets"),
                                            methodReference(Iterator.class, Object.class, "next"))),
                            methodReference(Runnable.class, void.class, "run")));
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        // when
        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterator.class);
        callEach.invoke(Arrays.asList(a, b, c).iterator());

        // then
        InOrder order = inOrder(a, b, c);
        order.verify(a).run();
        order.verify(b).run();
        order.verify(c).run();
        verifyNoMoreInteractions(a, b, c);
    }

    @Test
    void shouldGenerateWhileLoopWithMultipleTestExpressions() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "check",
                    param(boolean.class, "a"),
                    param(boolean.class, "b"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock loop = callEach.whileLoop(and(callEach.load("a"), callEach.load("b")))) {
                    loop.expression(invoke(loop.load("runner"), methodReference(Runnable.class, void.class, "run")));
                    loop.returns();
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);
        Runnable d = mock(Runnable.class);

        // when
        MethodHandle callEach =
                instanceMethod(handle.newInstance(), "check", boolean.class, boolean.class, Runnable.class);
        callEach.invoke(true, true, a);
        callEach.invoke(true, false, b);
        callEach.invoke(false, true, c);
        callEach.invoke(false, false, d);

        // then
        verify(a).run();
        verifyNoMoreInteractions(a);
        verifyNoInteractions(b);
        verifyNoInteractions(c);
        verifyNoInteractions(d);
    }

    @Test
    void shouldGenerateNestedWhileLoop() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targets"))) {
                try (CodeBlock loop = callEach.whileLoop(
                        invoke(callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    try (CodeBlock inner = loop.whileLoop(invoke(
                            callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {

                        inner.expression(invoke(
                                Expression.cast(
                                        Runnable.class,
                                        invoke(
                                                callEach.load("targets"),
                                                methodReference(Iterator.class, Object.class, "next"))),
                                methodReference(Runnable.class, void.class, "run")));
                    }
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        // when
        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterator.class);
        callEach.invoke(Arrays.asList(a, b, c).iterator());

        // then
        InOrder order = inOrder(a, b, c);
        order.verify(a).run();
        order.verify(b).run();
        order.verify(c).run();
        verifyNoMoreInteractions(a, b, c);
    }

    @Test
    void shouldGenerateWhileLoopContinue() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targets"),
                    param(TypeReference.parameterizedType(Iterator.class, Boolean.class), "skipTargets"))) {
                try (CodeBlock loop = callEach.whileLoop(
                        invoke(callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    loop.declare(TypeReference.typeReference(Runnable.class), "target");
                    loop.assign(
                            loop.local("target"),
                            Expression.cast(
                                    Runnable.class,
                                    invoke(
                                            callEach.load("targets"),
                                            methodReference(Iterator.class, Object.class, "next"))));

                    loop.declare(TypeReference.BOOLEAN, "skip");
                    loop.assign(
                            loop.local("skip"),
                            invoke(
                                    Expression.cast(
                                            Boolean.class,
                                            invoke(
                                                    callEach.load("skipTargets"),
                                                    methodReference(Iterator.class, Object.class, "next"))),
                                    methodReference(Boolean.class, boolean.class, "booleanValue")));

                    try (CodeBlock ifBlock = loop.ifStatement(loop.load("skip"))) {
                        ifBlock.continueIfPossible();
                    }

                    loop.expression(invoke(loop.load("target"), methodReference(Runnable.class, void.class, "run")));
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        // when
        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterator.class, Iterator.class);
        callEach.invoke(
                Arrays.asList(a, b, c).iterator(),
                Arrays.asList(false, true, false).iterator());

        // then
        InOrder order = inOrder(a, b, c);
        order.verify(a).run();
        order.verify(c).run();
        verifyNoMoreInteractions(a, b, c);
    }

    @Test
    void shouldGenerateNestedWhileLoopInnerContinue() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targetTargets"),
                    param(TypeReference.parameterizedType(Iterator.class, Boolean.class), "skipTargets"))) {
                try (CodeBlock outer = callEach.whileLoop(invoke(
                        callEach.load("targetTargets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    outer.declare(TypeReference.typeReference(Iterator.class), "targets");
                    outer.assign(
                            outer.local("targets"),
                            Expression.cast(
                                    Iterator.class,
                                    invoke(
                                            callEach.load("targetTargets"),
                                            methodReference(Iterator.class, Object.class, "next"))));

                    try (CodeBlock inner = outer.whileLoop(
                            invoke(outer.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                        inner.declare(TypeReference.typeReference(Runnable.class), "target");
                        inner.assign(
                                inner.local("target"),
                                Expression.cast(
                                        Runnable.class,
                                        invoke(
                                                outer.load("targets"),
                                                methodReference(Iterator.class, Object.class, "next"))));

                        inner.declare(TypeReference.BOOLEAN, "skip");
                        inner.assign(
                                inner.local("skip"),
                                invoke(
                                        Expression.cast(
                                                Boolean.class,
                                                invoke(
                                                        callEach.load("skipTargets"),
                                                        methodReference(Iterator.class, Object.class, "next"))),
                                        methodReference(Boolean.class, boolean.class, "booleanValue")));

                        try (CodeBlock ifBlock = inner.ifStatement(inner.load("skip"))) {
                            ifBlock.continueIfPossible();
                        }

                        inner.expression(
                                invoke(inner.load("target"), methodReference(Runnable.class, void.class, "run")));
                    }
                }
            }

            handle = simple.handle();
        }

        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);
        Runnable d = mock(Runnable.class);
        Runnable e = mock(Runnable.class);
        Runnable f = mock(Runnable.class);

        // when
        Iterator<Iterator<Runnable>> input = Arrays.asList(
                        Arrays.asList(a, b).iterator(),
                        Arrays.asList(c, d).iterator(),
                        Arrays.asList(e, f).iterator())
                .iterator();
        Iterator<Boolean> skips =
                Arrays.asList(false, true, true, false, false, true).iterator();

        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterator.class, Iterator.class);
        callEach.invoke(input, skips);

        // then
        InOrder order = inOrder(a, b, c, d, e, f);
        order.verify(a).run();
        order.verify(d).run();
        order.verify(e).run();
        verifyNoMoreInteractions(a, b, c, d, e, f);
    }

    @Test
    void shouldGenerateNestedWhileLoopInnerBreakWithLabel() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targetTargets"),
                    param(TypeReference.parameterizedType(Iterator.class, Boolean.class), "stopTargets"))) {
                try (CodeBlock outer = callEach.whileLoop(
                        invoke(
                                callEach.load("targetTargets"),
                                methodReference(Iterator.class, boolean.class, "hasNext")),
                        "outerLabel")) {
                    outer.declare(TypeReference.typeReference(Iterator.class), "targets");
                    outer.assign(
                            outer.local("targets"),
                            Expression.cast(
                                    Iterator.class,
                                    invoke(
                                            callEach.load("targetTargets"),
                                            methodReference(Iterator.class, Object.class, "next"))));

                    try (CodeBlock inner = outer.whileLoop(
                            invoke(outer.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                        inner.declare(TypeReference.typeReference(Runnable.class), "target");
                        inner.assign(
                                inner.local("target"),
                                Expression.cast(
                                        Runnable.class,
                                        invoke(
                                                outer.load("targets"),
                                                methodReference(Iterator.class, Object.class, "next"))));

                        inner.declare(TypeReference.BOOLEAN, "stop");
                        inner.assign(
                                inner.local("stop"),
                                invoke(
                                        Expression.cast(
                                                Boolean.class,
                                                invoke(
                                                        callEach.load("stopTargets"),
                                                        methodReference(Iterator.class, Object.class, "next"))),
                                        methodReference(Boolean.class, boolean.class, "booleanValue")));

                        try (CodeBlock ifBlock = inner.ifStatement(inner.load("stop"))) {
                            ifBlock.breaks("outerLabel");
                        }

                        inner.expression(
                                invoke(inner.load("target"), methodReference(Runnable.class, void.class, "run")));
                    }
                }
            }

            handle = simple.handle();
        }

        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);
        Runnable d = mock(Runnable.class);
        Runnable e = mock(Runnable.class);
        Runnable f = mock(Runnable.class);

        // when
        Iterator<Iterator<Runnable>> input = Arrays.asList(
                        Arrays.asList(a, b).iterator(),
                        Arrays.asList(c, d).iterator(),
                        Arrays.asList(e, f).iterator())
                .iterator();
        Iterator<Boolean> stops =
                Arrays.asList(false, false, false, true, false, false).iterator();

        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterator.class, Iterator.class);
        callEach.invoke(input, stops);

        // then
        InOrder order = inOrder(a, b, c, d, e, f);
        order.verify(a).run();
        order.verify(b).run();
        order.verify(c).run();
        verifyNoMoreInteractions(a, b, c, d, e, f);
    }

    @Test
    void shouldGenerateNestedWhileLoopDoubleContinue() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targetTargets"),
                    param(TypeReference.parameterizedType(Iterator.class, Boolean.class), "skipOuters"),
                    param(TypeReference.parameterizedType(Iterator.class, Boolean.class), "skipInners"))) {
                try (CodeBlock outer = callEach.whileLoop(invoke(
                        callEach.load("targetTargets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    outer.declare(TypeReference.typeReference(Iterator.class), "targets");
                    outer.assign(
                            outer.local("targets"),
                            Expression.cast(
                                    Iterator.class,
                                    invoke(
                                            callEach.load("targetTargets"),
                                            methodReference(Iterator.class, Object.class, "next"))));

                    outer.declare(TypeReference.BOOLEAN, "skipOuter");
                    outer.assign(
                            outer.local("skipOuter"),
                            invoke(
                                    Expression.cast(
                                            Boolean.class,
                                            invoke(
                                                    callEach.load("skipOuters"),
                                                    methodReference(Iterator.class, Object.class, "next"))),
                                    methodReference(Boolean.class, boolean.class, "booleanValue")));

                    try (CodeBlock ifBlock = outer.ifStatement(outer.load("skipOuter"))) {
                        ifBlock.continueIfPossible();
                    }

                    try (CodeBlock inner = outer.whileLoop(
                            invoke(outer.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                        inner.declare(TypeReference.typeReference(Runnable.class), "target");
                        inner.assign(
                                inner.local("target"),
                                Expression.cast(
                                        Runnable.class,
                                        invoke(
                                                outer.load("targets"),
                                                methodReference(Iterator.class, Object.class, "next"))));

                        inner.declare(TypeReference.BOOLEAN, "skipInner");
                        inner.assign(
                                inner.local("skipInner"),
                                invoke(
                                        Expression.cast(
                                                Boolean.class,
                                                invoke(
                                                        callEach.load("skipInners"),
                                                        methodReference(Iterator.class, Object.class, "next"))),
                                        methodReference(Boolean.class, boolean.class, "booleanValue")));

                        try (CodeBlock ifBlock = inner.ifStatement(inner.load("skipInner"))) {
                            ifBlock.continueIfPossible();
                        }

                        inner.expression(
                                invoke(inner.load("target"), methodReference(Runnable.class, void.class, "run")));
                    }
                }
            }

            handle = simple.handle();
        }

        Runnable a1 = mock(Runnable.class);
        Runnable a2 = mock(Runnable.class);
        Runnable b1 = mock(Runnable.class);
        Runnable b2 = mock(Runnable.class);
        Runnable b3 = mock(Runnable.class);
        Runnable b4 = mock(Runnable.class);
        Runnable c1 = mock(Runnable.class);

        // when
        Iterator<Iterator<Runnable>> input = Arrays.asList(
                        Arrays.asList(a1, a2).iterator(),
                        Arrays.asList(b1, b2, b3, b4).iterator(),
                        Collections.singletonList(c1).iterator())
                .iterator();
        Iterator<Boolean> skipOuter = Arrays.asList(true, false, true).iterator();
        Iterator<Boolean> skipInner = Arrays.asList(false, true, false, true).iterator();

        MethodHandle callEach =
                instanceMethod(handle.newInstance(), "callEach", Iterator.class, Iterator.class, Iterator.class);
        callEach.invoke(input, skipOuter, skipInner);

        // then
        InOrder order = inOrder(a1, a2, b1, b2, b3, b4, c1);
        order.verify(b1).run();
        order.verify(b3).run();
        verifyNoMoreInteractions(a1, a2, b1, b2, b3, b4, c1);
    }

    @Test
    void shouldGenerateForEachLoop() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterable.class, Runnable.class), "targets"))) {
                try (CodeBlock loop = callEach.forEach(param(Runnable.class, "runner"), callEach.load("targets"))) {
                    loop.expression(invoke(loop.load("runner"), methodReference(Runnable.class, void.class, "run")));
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        // when
        MethodHandle callEach = instanceMethod(handle.newInstance(), "callEach", Iterable.class);
        callEach.invoke(Arrays.asList(a, b, c));

        // then
        InOrder order = inOrder(a, b, c);
        order.verify(a).run();
        order.verify(b).run();
        order.verify(c).run();
        verifyNoMoreInteractions(a, b, c);
    }

    @Test
    void shouldGenerateIfStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class, "conditional", param(boolean.class, "test"), param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff = conditional.ifStatement(conditional.load("test"))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class, Runnable.class);
        conditional.invoke(true, runner1);
        conditional.invoke(false, runner2);

        // then
        verify(runner1).run();
        verifyNoInteractions(runner2);
    }

    @Test
    void shouldGenerateIfElseStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional =
                    simple.generateMethod(String.class, "conditional", param(boolean.class, "test"))) {
                conditional.ifElseStatement(
                        conditional.load("test"),
                        block -> block.returns(constant("true")),
                        block -> block.returns(constant("false")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class);

        assertThat(conditional.invoke(true)).isEqualTo("true");
        assertThat(conditional.invoke(false)).isEqualTo("false");
    }

    @Test
    void shouldGenerateIfEqualsStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(Object.class, "lhs"),
                    param(Object.class, "rhs"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(equal(conditional.load("lhs"), conditional.load("rhs")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Object a = "a";
        Object b = "b";

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", Object.class, Object.class, Runnable.class);
        conditional.invoke(a, b, runner1);
        conditional.invoke(a, a, runner2);

        // then
        verify(runner2).run();
        verifyNoInteractions(runner1);
    }

    @Test
    void shouldGenerateIfNotEqualsStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(Object.class, "lhs"),
                    param(Object.class, "rhs"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(not(equal(conditional.load("lhs"), conditional.load("rhs"))))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Object a = "a";
        Object b = "b";

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", Object.class, Object.class, Runnable.class);
        conditional.invoke(a, a, runner1);
        conditional.invoke(a, b, runner2);

        // then
        verify(runner2).run();
        verifyNoInteractions(runner1);
    }

    @Test
    void shouldGenerateIfNotExpressionStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class, "conditional", param(boolean.class, "test"), param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff = conditional.ifStatement(not(conditional.load("test")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class, Runnable.class);
        conditional.invoke(true, runner1);
        conditional.invoke(false, runner2);

        // then
        verify(runner2).run();
        verifyNoInteractions(runner1);
    }

    @Test
    void shouldGenerateIfNullStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class, "conditional", param(Object.class, "test"), param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff = conditional.ifStatement(isNull(conditional.load("test")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", Object.class, Runnable.class);
        conditional.invoke(null, runner1);
        conditional.invoke(new Object(), runner2);

        // then
        verify(runner1).run();
        verifyNoInteractions(runner2);
    }

    @Test
    void shouldGenerateIfNonNullStatement() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class, "conditional", param(Object.class, "test"), param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff = conditional.ifStatement(notNull(conditional.load("test")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", Object.class, Runnable.class);
        conditional.invoke(new Object(), runner1);
        conditional.invoke(null, runner2);

        // then
        verify(runner1).run();
        verifyNoInteractions(runner2);
    }

    @Test
    void shouldGenerateTryWithNestedWhileIfLoop() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targets"),
                    param(boolean.class, "test"),
                    param(Runnable.class, "runner"))) {
                try (var tryBlock = callEach.tryCatch(
                        catchBlock -> catchBlock.expression(invoke(catchBlock.load("runner"), RUN)),
                        param(RuntimeException.class, "e"))) {
                    try (CodeBlock loop = tryBlock.whileLoop(invoke(
                            callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {

                        try (CodeBlock doStuff = loop.ifStatement(not(callEach.load("test")))) {
                            doStuff.expression(invoke(doStuff.load("runner"), RUN));
                        }
                        loop.expression(invoke(
                                Expression.cast(
                                        Runnable.class,
                                        invoke(
                                                callEach.load("targets"),
                                                methodReference(Iterator.class, Object.class, "next"))),
                                methodReference(Runnable.class, void.class, "run")));
                    }
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);

        // when
        MethodHandle callEach =
                instanceMethod(handle.newInstance(), "callEach", Iterator.class, boolean.class, Runnable.class);

        callEach.invoke(Arrays.asList(a, b, c).iterator(), false, runner1);
        callEach.invoke(Arrays.asList(a, b, c).iterator(), true, runner2);

        // then
        verify(runner1, times(3)).run();
        verify(runner2, never()).run();
    }

    @Test
    void shouldGenerateWhileWithNestedIfLoop() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock callEach = simple.generateMethod(
                    void.class,
                    "callEach",
                    param(TypeReference.parameterizedType(Iterator.class, Runnable.class), "targets"),
                    param(boolean.class, "test"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock loop = callEach.whileLoop(
                        invoke(callEach.load("targets"), methodReference(Iterator.class, boolean.class, "hasNext")))) {
                    try (CodeBlock doStuff = loop.ifStatement(not(callEach.load("test")))) {
                        doStuff.expression(invoke(doStuff.load("runner"), RUN));
                    }
                    loop.expression(invoke(
                            Expression.cast(
                                    Runnable.class,
                                    invoke(
                                            callEach.load("targets"),
                                            methodReference(Iterator.class, Object.class, "next"))),
                            methodReference(Runnable.class, void.class, "run")));
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock(Runnable.class);
        Runnable b = mock(Runnable.class);
        Runnable c = mock(Runnable.class);

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        // when
        MethodHandle callEach =
                instanceMethod(handle.newInstance(), "callEach", Iterator.class, boolean.class, Runnable.class);

        callEach.invoke(Arrays.asList(a, b, c).iterator(), false, runner1);
        callEach.invoke(Arrays.asList(a, b, c).iterator(), true, runner2);

        // then
        verify(runner1, times(3)).run();
        verify(runner2, never()).run();
    }

    @Test
    void shouldGenerateOr() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(or(conditional.load("test1"), conditional.load("test2")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Runnable runner3 = mock(Runnable.class);
        Runnable runner4 = mock(Runnable.class);

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class);
        conditional.invoke(true, true, runner1);
        conditional.invoke(true, false, runner2);
        conditional.invoke(false, true, runner3);
        conditional.invoke(false, false, runner4);

        // then
        verify(runner1).run();
        verify(runner2).run();
        verify(runner3).run();
        verifyNoInteractions(runner4);
    }

    @Test
    void shouldGenerateIfNotOr() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(not(or(conditional.load("test1"), conditional.load("test2"))))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Runnable runner3 = mock(Runnable.class);
        Runnable runner4 = mock(Runnable.class);

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class);
        conditional.invoke(true, true, runner1);
        conditional.invoke(true, false, runner2);
        conditional.invoke(false, true, runner3);
        conditional.invoke(false, false, runner4);

        // then
        verifyNoInteractions(runner1);
        verifyNoInteractions(runner2);
        verifyNoInteractions(runner3);
        verify(runner4).run();
    }

    @Test
    void shouldGenerateIfNotAnd() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(not(and(conditional.load("test1"), conditional.load("test2"))))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Runnable runner3 = mock(Runnable.class);
        Runnable runner4 = mock(Runnable.class);

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class);
        conditional.invoke(true, true, runner1);
        conditional.invoke(true, false, runner2);
        conditional.invoke(false, true, runner3);
        conditional.invoke(false, false, runner4);

        // then
        verifyNoInteractions(runner1);
        verify(runner2).run();
        verify(runner3).run();
        verify(runner4).run();
    }

    @Test
    void shouldGenerateIfNotNotAnd() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(not(not(and(conditional.load("test1"), conditional.load("test2")))))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Runnable runner3 = mock(Runnable.class);
        Runnable runner4 = mock(Runnable.class);

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class);
        conditional.invoke(true, true, runner1);
        conditional.invoke(true, false, runner2);
        conditional.invoke(false, true, runner3);
        conditional.invoke(false, false, runner4);

        // then
        verify(runner1).run();
        verifyNoInteractions(runner2);
        verifyNoInteractions(runner3);
        verifyNoInteractions(runner4);
    }

    @Test
    void shouldGenerateMethodUsingOr() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class, "conditional", param(boolean.class, "test1"), param(boolean.class, "test2"))) {
                conditional.returns(or(conditional.load("test1"), conditional.load("test2")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false)).isEqualTo(true);
        assertThat(conditional.invoke(false, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateAnd() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    void.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(Runnable.class, "runner"))) {
                try (CodeBlock doStuff =
                        conditional.ifStatement(and(conditional.load("test1"), conditional.load("test2")))) {
                    doStuff.expression(invoke(doStuff.load("runner"), RUN));
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock(Runnable.class);
        Runnable runner2 = mock(Runnable.class);
        Runnable runner3 = mock(Runnable.class);
        Runnable runner4 = mock(Runnable.class);

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class);
        conditional.invoke(true, true, runner1);
        conditional.invoke(true, false, runner2);
        conditional.invoke(false, true, runner3);
        conditional.invoke(false, false, runner4);

        // then
        verify(runner1).run();
        verifyNoInteractions(runner2);
        verifyNoInteractions(runner3);
        verifyNoInteractions(runner4);
    }

    @Test
    void shouldGenerateMethodUsingAnd() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class, "conditional", param(boolean.class, "test1"), param(boolean.class, "test2"))) {
                conditional.returns(and(conditional.load("test1"), conditional.load("test2")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, true)).isEqualTo(false);
        assertThat(conditional.invoke(false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateMethodUsingMultipleAnds() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(boolean.class, "test3"))) {
                conditional.returns(
                        and(conditional.load("test1"), and(conditional.load("test2"), conditional.load("test3"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, true)).isEqualTo(false);
        assertThat(conditional.invoke(false, true, true)).isEqualTo(false);
        assertThat(conditional.invoke(false, false, true)).isEqualTo(false);
        assertThat(conditional.invoke(true, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(true, false, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateMethodUsingMultipleAnds2() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(boolean.class, "test3"))) {
                conditional.returns(
                        and(and(conditional.load("test1"), conditional.load("test2")), conditional.load("test3")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, true)).isEqualTo(false);
        assertThat(conditional.invoke(false, true, true)).isEqualTo(false);
        assertThat(conditional.invoke(false, false, true)).isEqualTo(false);
        assertThat(conditional.invoke(true, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(true, false, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateMethodUsingMultipleOrs() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(boolean.class, "test3"))) {
                conditional.returns(
                        or(conditional.load("test1"), or(conditional.load("test2"), conditional.load("test3"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, false, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, true, false)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, false)).isEqualTo(true);
        assertThat(conditional.invoke(false, true, false)).isEqualTo(true);
        assertThat(conditional.invoke(false, false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateMethodUsingMultipleOrs2() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(boolean.class, "test3"))) {
                conditional.returns(
                        or(or(conditional.load("test1"), conditional.load("test2")), conditional.load("test3")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, false, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, true, false)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, false)).isEqualTo(true);
        assertThat(conditional.invoke(false, true, false)).isEqualTo(true);
        assertThat(conditional.invoke(false, false, false)).isEqualTo(false);
    }

    @Test
    void shouldGenerateMethodUsingAndsAndOrs() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional = simple.generateMethod(
                    boolean.class,
                    "conditional",
                    param(boolean.class, "test1"),
                    param(boolean.class, "test2"),
                    param(boolean.class, "test3"))) {
                conditional.returns(
                        and(or(conditional.load("test1"), conditional.load("test2")), conditional.load("test3")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod(handle.newInstance(), "conditional", boolean.class, boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke(true, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(true, false, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, true, true)).isEqualTo(true);
        assertThat(conditional.invoke(false, false, true)).isEqualTo(false);
        assertThat(conditional.invoke(true, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(true, false, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, true, false)).isEqualTo(false);
        assertThat(conditional.invoke(false, false, false)).isEqualTo(false);
    }

    @Test
    void shouldHandleNot() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional =
                    simple.generateMethod(boolean.class, "conditional", param(boolean.class, "test"))) {
                conditional.returns(not(conditional.load("test")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional = instanceMethod(handle.newInstance(), "conditional", boolean.class);

        // then
        assertThat(conditional.invoke(true)).isEqualTo(false);
        assertThat(conditional.invoke(false)).isEqualTo(true);
    }

    @Test
    void shouldHandleTernaryOperator() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock ternaryBlock = simple.generateMethod(
                    String.class, "ternary", param(boolean.class, "test"), param(TernaryChecker.class, "check"))) {
                ternaryBlock.returns(ternary(
                        ternaryBlock.load("test"),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onTrue")),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onFalse"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary = instanceMethod(handle.newInstance(), "ternary", boolean.class, TernaryChecker.class);

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat(ternary.invoke(true, checker1)).isEqualTo("on true");
        assertTrue(checker1.ranOnTrue);
        assertFalse(checker1.ranOnFalse);

        TernaryChecker checker2 = new TernaryChecker();
        assertThat(ternary.invoke(false, checker2)).isEqualTo("on false");
        assertFalse(checker2.ranOnTrue);
        assertTrue(checker2.ranOnFalse);
    }

    @Test
    void shouldHandleTernaryOnNullOperator() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock ternaryBlock = simple.generateMethod(
                    String.class, "ternary", param(Object.class, "test"), param(TernaryChecker.class, "check"))) {
                ternaryBlock.returns(ternary(
                        isNull(ternaryBlock.load("test")),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onTrue")),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onFalse"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary = instanceMethod(handle.newInstance(), "ternary", Object.class, TernaryChecker.class);

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat(ternary.invoke(null, checker1)).isEqualTo("on true");
        assertTrue(checker1.ranOnTrue);
        assertFalse(checker1.ranOnFalse);

        TernaryChecker checker2 = new TernaryChecker();
        assertThat(ternary.invoke(new Object(), checker2)).isEqualTo("on false");
        assertFalse(checker2.ranOnTrue);
        assertTrue(checker2.ranOnFalse);
    }

    @Test
    void shouldHandleTernaryOnNonNullOperator() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock ternaryBlock = simple.generateMethod(
                    String.class, "ternary", param(Object.class, "test"), param(TernaryChecker.class, "check"))) {
                ternaryBlock.returns(ternary(
                        notNull(ternaryBlock.load("test")),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onTrue")),
                        invoke(
                                ternaryBlock.load("check"),
                                methodReference(TernaryChecker.class, String.class, "onFalse"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary = instanceMethod(handle.newInstance(), "ternary", Object.class, TernaryChecker.class);

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat(ternary.invoke(new Object(), checker1)).isEqualTo("on true");
        assertTrue(checker1.ranOnTrue);
        assertFalse(checker1.ranOnFalse);

        TernaryChecker checker2 = new TernaryChecker();
        assertThat(ternary.invoke(null, checker2)).isEqualTo("on false");
        assertFalse(checker2.ranOnTrue);
        assertTrue(checker2.ranOnFalse);
    }

    @Test
    void shouldHandleEquality() throws Throwable {
        // boolean
        assertTrue(compareForType(boolean.class, true, true, Expression::equal));
        assertTrue(compareForType(boolean.class, false, false, Expression::equal));
        assertFalse(compareForType(boolean.class, true, false, Expression::equal));
        assertFalse(compareForType(boolean.class, false, true, Expression::equal));

        // byte
        assertTrue(compareForType(byte.class, (byte) 42, (byte) 42, Expression::equal));
        assertFalse(compareForType(byte.class, (byte) 43, (byte) 42, Expression::equal));
        assertFalse(compareForType(byte.class, (byte) 42, (byte) 43, Expression::equal));

        // short
        assertTrue(compareForType(short.class, (short) 42, (short) 42, Expression::equal));
        assertFalse(compareForType(short.class, (short) 43, (short) 42, Expression::equal));
        assertFalse(compareForType(short.class, (short) 42, (short) 43, Expression::equal));

        // char
        assertTrue(compareForType(char.class, (char) 42, (char) 42, Expression::equal));
        assertFalse(compareForType(char.class, (char) 43, (char) 42, Expression::equal));
        assertFalse(compareForType(char.class, (char) 42, (char) 43, Expression::equal));

        // int
        assertTrue(compareForType(int.class, 42, 42, Expression::equal));
        assertFalse(compareForType(int.class, 43, 42, Expression::equal));
        assertFalse(compareForType(int.class, 42, 43, Expression::equal));

        // long
        assertTrue(compareForType(long.class, 42L, 42L, Expression::equal));
        assertFalse(compareForType(long.class, 43L, 42L, Expression::equal));
        assertFalse(compareForType(long.class, 42L, 43L, Expression::equal));

        // float
        assertTrue(compareForType(float.class, 42F, 42F, Expression::equal));
        assertFalse(compareForType(float.class, 43F, 42F, Expression::equal));
        assertFalse(compareForType(float.class, 42F, 43F, Expression::equal));

        // double
        assertTrue(compareForType(double.class, 42D, 42D, Expression::equal));
        assertFalse(compareForType(double.class, 43D, 42D, Expression::equal));
        assertFalse(compareForType(double.class, 42D, 43D, Expression::equal));

        // reference
        Object obj1 = new Object();
        Object obj2 = new Object();
        assertTrue(compareForType(Object.class, obj1, obj1, Expression::equal));
        assertFalse(compareForType(Object.class, obj1, obj2, Expression::equal));
        assertFalse(compareForType(Object.class, obj2, obj1, Expression::equal));
    }

    @Test
    void shouldHandleGreaterThan() throws Throwable {
        assertTrue(compareForType(float.class, 43F, 42F, Expression::gt));
        assertTrue(compareForType(long.class, 43L, 42L, Expression::gt));

        // byte
        assertTrue(compareForType(byte.class, (byte) 43, (byte) 42, Expression::gt));
        assertFalse(compareForType(byte.class, (byte) 42, (byte) 42, Expression::gt));
        assertFalse(compareForType(byte.class, (byte) 42, (byte) 43, Expression::gt));

        // short
        assertTrue(compareForType(short.class, (short) 43, (short) 42, Expression::gt));
        assertFalse(compareForType(short.class, (short) 42, (short) 42, Expression::gt));
        assertFalse(compareForType(short.class, (short) 42, (short) 43, Expression::gt));

        // char
        assertTrue(compareForType(char.class, (char) 43, (char) 42, Expression::gt));
        assertFalse(compareForType(char.class, (char) 42, (char) 42, Expression::gt));
        assertFalse(compareForType(char.class, (char) 42, (char) 43, Expression::gt));

        // int
        assertTrue(compareForType(int.class, 43, 42, Expression::gt));
        assertFalse(compareForType(int.class, 42, 42, Expression::gt));
        assertFalse(compareForType(int.class, 42, 43, Expression::gt));

        // long
        assertTrue(compareForType(long.class, 43L, 42L, Expression::gt));
        assertFalse(compareForType(long.class, 42L, 42L, Expression::gt));
        assertFalse(compareForType(long.class, 42L, 43L, Expression::gt));

        // float
        assertTrue(compareForType(float.class, 43F, 42F, Expression::gt));
        assertFalse(compareForType(float.class, 42F, 42F, Expression::gt));
        assertFalse(compareForType(float.class, 42F, 43F, Expression::gt));

        // double
        assertTrue(compareForType(double.class, 43D, 42D, Expression::gt));
        assertFalse(compareForType(double.class, 42D, 42D, Expression::gt));
        assertFalse(compareForType(double.class, 42D, 43D, Expression::gt));
    }

    @Test
    void shouldHandleAddition() throws Throwable {
        assertThat(addForType(int.class, 17, 18)).isEqualTo(35);
        assertThat(addForType(long.class, 17L, 18L)).isEqualTo(35L);
        assertThat(addForType(double.class, 17D, 18D)).isEqualTo(35D);
    }

    @Test
    void shouldHandleSubtraction() throws Throwable {
        assertThat(subtractForType(int.class, 19, 18)).isEqualTo(1);
        assertThat(subtractForType(long.class, 19L, 18L)).isEqualTo(1L);
        assertThat(subtractForType(double.class, 19D, 18D)).isEqualTo(1D);
    }

    @Test
    void shouldHandleMultiplication() throws Throwable {
        assertThat(multiplyForType(int.class, 17, 18)).isEqualTo(306);
        assertThat(multiplyForType(long.class, 17L, 18L)).isEqualTo(306L);
        assertThat(multiplyForType(double.class, 17D, 18D)).isEqualTo(306D);
    }

    @Test
    void shouldHandleOuterMultiplyInnerAdd() throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        Class clazz = int.class;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock block = simple.generateMethod(
                    clazz, "outerMultiplyInnerAdd", param(clazz, "a"), param(clazz, "b"), param(clazz, "c"))) {
                block.returns(multiply(block.load("a"), add(block.load("b"), block.load("c"))));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle code = instanceMethod(handle.newInstance(), "outerMultiplyInnerAdd", clazz, clazz, clazz);

        // then
        assertEquals(2 * (3 + 4), code.invoke(2, 3, 4));
    }

    @SuppressWarnings("unchecked")
    private <T> T addForType(Class<T> clazz, T lhs, T rhs) throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock block = simple.generateMethod(clazz, "add", param(clazz, "a"), param(clazz, "b"))) {
                block.returns(add(block.load("a"), block.load("b")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle add = instanceMethod(handle.newInstance(), "add", clazz, clazz);

        // then
        return (T) add.invoke(lhs, rhs);
    }

    @SuppressWarnings("unchecked")
    private <T> T subtractForType(Class<T> clazz, T lhs, T rhs) throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock block = simple.generateMethod(clazz, "sub", param(clazz, "a"), param(clazz, "b"))) {
                block.returns(subtract(block.load("a"), block.load("b")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle sub = instanceMethod(handle.newInstance(), "sub", clazz, clazz);

        // then
        return (T) sub.invoke(lhs, rhs);
    }

    @SuppressWarnings("unchecked")
    private <T> T multiplyForType(Class<T> clazz, T lhs, T rhs) throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock block = simple.generateMethod(clazz, "multiply", param(clazz, "a"), param(clazz, "b"))) {
                block.returns(multiply(block.load("a"), block.load("b")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle sub = instanceMethod(handle.newInstance(), "multiply", clazz, clazz);

        // then
        return (T) sub.invoke(lhs, rhs);
    }

    private <T> boolean compareForType(
            Class<T> clazz, T lhs, T rhs, BiFunction<Expression, Expression, Expression> compare) throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock block =
                    simple.generateMethod(boolean.class, "compare", param(clazz, "a"), param(clazz, "b"))) {
                block.returns(compare.apply(block.load("a"), block.load("b")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle compareFcn = instanceMethod(handle.newInstance(), "compare", clazz, clazz);

        // then
        return (boolean) compareFcn.invoke(lhs, rhs);
    }

    public static class TernaryChecker {
        private boolean ranOnTrue;
        private boolean ranOnFalse;

        public String onTrue() {
            ranOnTrue = true;
            return "on true";
        }

        public String onFalse() {
            ranOnFalse = true;
            return "on false";
        }
    }

    @Test
    void shouldGenerateTryCatch() throws Throwable {
        // given
        ClassHandle handle;

        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock run = simple.generateMethod(
                    void.class, "run", param(Runnable.class, "body"), param(Runnable.class, "catcher"))) {
                try (var body = run.tryCatch(
                        handler -> handler.expression(invoke(handler.load("catcher"), RUN)),
                        param(RuntimeException.class, "E"))) {
                    body.expression(invoke(body.load("body"), RUN));
                }
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock(Runnable.class);
        Runnable failBody = mock(Runnable.class);
        Runnable successCatch = mock(Runnable.class);
        Runnable failCatch = mock(Runnable.class);
        RuntimeException theFailure = new RuntimeException();
        doThrow(theFailure).when(failBody).run();
        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class, Runnable.class);

        // success
        run.invoke(successBody, successCatch);
        verify(successBody).run();
        verify(successCatch, never()).run();

        // failure
        run.invoke(failBody, failCatch);
        InOrder orderFailure = inOrder(failBody, failCatch);
        orderFailure.verify(failBody).run();
        orderFailure.verify(failCatch).run();
    }

    @Test
    void shouldGenerateTryCatch2() throws Throwable {
        // given
        int success = -1;
        int fail = 1;
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock outer = simple.generateMethod(int.class, "run", param(Runnable.class, "body"))) {
                try (var body = outer.tryCatch(
                        onError -> onError.returns(constant(fail)), param(RuntimeException.class, "E1"))) {
                    body.expression(invoke(body.load("body"), RUN));
                }
                outer.returns(constant(success));
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock(Runnable.class);
        Runnable failBody = mock(Runnable.class);
        RuntimeException theFailure = new RuntimeException();
        doThrow(theFailure).when(failBody).run();
        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class);

        assertThat(run.invoke(successBody)).isEqualTo(success);
        assertThat(run.invoke(failBody)).isEqualTo(fail);
    }

    @Test
    void shouldGenerateNestedTryCatch() throws Throwable {
        // given
        int success = -1;
        int failOuter = 1;
        int failInner = 2;
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock outer = simple.generateMethod(int.class, "run", param(Runnable.class, "body"))) {
                try (var body = outer.tryCatch(
                        handler -> handler.returns(constant(failOuter)), param(MyFirstException.class, "E1"))) {
                    try (var inner = body.tryCatch(
                            handler -> handler.returns(constant(failInner)), param(MySecondException.class, "E2"))) {
                        inner.expression(invoke(outer.load("body"), RUN));
                    }
                }
                outer.returns(constant(success));
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock(Runnable.class);
        Runnable fail1 = mock(Runnable.class);
        MyFirstException failure1 = new MyFirstException();
        doThrow(failure1).when(fail1).run();
        Runnable fail2 = mock(Runnable.class);
        MySecondException failure2 = new MySecondException();
        doThrow(failure2).when(fail2).run();
        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class);

        assertThat(run.invoke(successBody)).isEqualTo(success);
        assertThat(run.invoke(fail1)).isEqualTo(failOuter);
        assertThat(run.invoke(fail2)).isEqualTo(failInner);
    }

    @Test
    void shouldGenerateNestedTryCatchWithRethrow() throws Throwable {
        // given
        int success = -1;
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock outer = simple.generateMethod(
                    int.class,
                    "run",
                    param(Runnable.class, "beforeAll"),
                    param(Runnable.class, "beforeInner"),
                    param(Runnable.class, "inner"),
                    param(Runnable.class, "afterInner"),
                    param(Runnable.class, "afterAll"))) {
                outer.expression(invoke(outer.load("beforeAll"), RUN));
                try (var firstBlock = outer.tryCatch(
                        onError -> onError.throwException(newRuntimeException("outer")),
                        param(MyFirstException.class, "E1"))) {
                    firstBlock.expression(invoke(firstBlock.load("beforeInner"), RUN));
                    try (var secondBlock = firstBlock.tryCatch(
                            onError -> onError.throwException(newRuntimeException("inner")),
                            param(MyFirstException.class, "E2"))) {
                        secondBlock.expression(invoke(secondBlock.load("inner"), RUN));
                    }
                    firstBlock.expression(invoke(firstBlock.load("afterInner"), RUN));
                }
                outer.expression(invoke(outer.load("afterAll"), RUN));
                outer.returns(constant(success));
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock(Runnable.class);
        Runnable failBody = mock(Runnable.class);
        MyFirstException theFailure = new MyFirstException();
        doThrow(theFailure).when(failBody).run();
        MethodHandle run = instanceMethod(
                handle.newInstance(),
                "run",
                Runnable.class,
                Runnable.class,
                Runnable.class,
                Runnable.class,
                Runnable.class);

        // success
        assertThat(run.invoke(successBody, successBody, successBody, successBody, successBody))
                .isEqualTo(success);
        assertThatThrownBy(() -> run.invoke(failBody, successBody, successBody, successBody, successBody))
                .isEqualTo(theFailure);
        assertThatThrownBy(() -> run.invoke(successBody, successBody, successBody, successBody, failBody))
                .isEqualTo(theFailure);
        assertThatThrownBy(() -> run.invoke(successBody, failBody, successBody, successBody, successBody))
                .hasMessage("outer");
        assertThatThrownBy(() -> run.invoke(successBody, failBody, failBody, successBody, successBody))
                .hasMessage("outer");
        assertThatThrownBy(() -> run.invoke(successBody, successBody, failBody, failBody, failBody))
                .hasMessage("inner");
        assertThatThrownBy(() -> run.invoke(successBody, successBody, successBody, failBody, failBody))
                .hasMessage("outer");
        assertThatThrownBy(() -> run.invoke(successBody, successBody, successBody, failBody, successBody))
                .hasMessage("outer");
    }

    @Test
    void shouldGenerateDeeplyNestedTryCatch() throws Throwable {
        // given
        int success = -1;
        int level1 = 1;
        int level2 = 2;
        int level3 = 3;
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock outer = simple.generateMethod(int.class, "run", param(Runnable.class, "body"))) {
                try (var firstBlock = outer.tryCatch(
                        onError -> onError.returns(constant(level1)), param(MyFirstException.class, "E1"))) {
                    try (var secondBlock = firstBlock.tryCatch(
                            onError -> onError.returns(constant(level2)), param(MySecondException.class, "E2"))) {
                        try (var thirdBlock = secondBlock.tryCatch(
                                onError -> onError.returns(constant(level3)), param(MyThirdException.class, "E3"))) {
                            thirdBlock.expression(invoke(secondBlock.load("body"), RUN));
                        }
                    }
                }
                outer.returns(constant(success));
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock(Runnable.class);
        Runnable fail1 = mock(Runnable.class);
        Runnable fail2 = mock(Runnable.class);
        Runnable fail3 = mock(Runnable.class);
        MyFirstException failAtLevel1 = new MyFirstException();
        MySecondException failAtLevel2 = new MySecondException();
        MyThirdException failAtLevel3 = new MyThirdException();
        doThrow(failAtLevel1).when(fail1).run();
        doThrow(failAtLevel2).when(fail2).run();
        doThrow(failAtLevel3).when(fail3).run();
        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class);

        assertThat(run.invoke(successBody)).isEqualTo(success);
        assertThat(run.invoke(fail1)).isEqualTo(level1);
        assertThat(run.invoke(fail2)).isEqualTo(level2);
        assertThat(run.invoke(fail3)).isEqualTo(level3);
    }

    @Test
    void shouldGenerateTryCatchWithNestedBlock() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock run = simple.generateMethod(
                    void.class,
                    "run",
                    param(Runnable.class, "body"),
                    param(Runnable.class, "catcher"),
                    param(boolean.class, "test"))) {

                try (var tryBlock = run.tryCatch(
                        catchBlock -> catchBlock.expression(invoke(run.load("catcher"), RUN)),
                        param(RuntimeException.class, "E"))) {
                    try (CodeBlock ifBlock = tryBlock.ifStatement(run.load("test"))) {
                        ifBlock.expression(invoke(run.load("body"), RUN));
                    }
                }
            }
            handle = simple.handle();
        }

        // when
        Runnable runnable = mock(Runnable.class);
        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class, Runnable.class, boolean.class);

        // then
        run.invoke(runnable, mock(Runnable.class), false);
        verify(runnable, never()).run();
        run.invoke(runnable, mock(Runnable.class), true);
        verify(runnable).run();
    }

    @Test
    void shouldGenerateTryAndMultipleCatch() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock run = simple.generateMethod(
                    void.class,
                    "run",
                    param(Runnable.class, "body"),
                    param(Runnable.class, "catcher1"),
                    param(Runnable.class, "catcher2"))) {

                try (var tryBlock = run.tryCatch(
                        catchBlock2 -> catchBlock2.expression(invoke(run.load("catcher2"), RUN)),
                        param(MySecondException.class, "E2"))) {
                    try (var innerTry = tryBlock.tryCatch(
                            catchBlock1 -> catchBlock1.expression(invoke(run.load("catcher1"), RUN)),
                            param(MyFirstException.class, "E1"))) {
                        innerTry.expression(invoke(run.load("body"), RUN));
                    }
                }
            }
            handle = simple.handle();
        }

        // when
        Runnable body1 = mock(Runnable.class);
        Runnable body2 = mock(Runnable.class);
        Runnable catcher11 = mock(Runnable.class);
        Runnable catcher12 = mock(Runnable.class);
        Runnable catcher21 = mock(Runnable.class);
        Runnable catcher22 = mock(Runnable.class);
        doThrow(MyFirstException.class).when(body1).run();
        doThrow(MySecondException.class).when(body2).run();

        MethodHandle run = instanceMethod(handle.newInstance(), "run", Runnable.class, Runnable.class, Runnable.class);

        run.invoke(body1, catcher11, catcher12);
        verify(body1).run();
        verify(catcher11).run();
        verify(catcher12, never()).run();

        run.invoke(body2, catcher21, catcher22);
        verify(body2).run();
        verify(catcher22).run();
        verify(catcher21, never()).run();
    }

    @Test
    void shouldThrowException() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock thrower = simple.generateMethod(void.class, "thrower")) {
                thrower.throwException(invoke(
                        newInstance(RuntimeException.class),
                        constructorReference(RuntimeException.class, String.class),
                        constant("hello world")));
            }
            handle = simple.handle();
        }

        // when
        try {
            instanceMethod(handle.newInstance(), "thrower").invoke();
            fail("expected exception");
        }
        // then
        catch (RuntimeException exception) {
            assertEquals("hello world", exception.getMessage());
        }
    }

    @Test
    void shouldBeAbleToCast() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass(NamedBase.class, "SimpleClass")) {
            simple.field(String.class, "foo");
            simple.generate(MethodTemplate.constructor(param(String.class, "name"), param(Object.class, "foo"))
                    .invokeSuper(
                            new ExpressionTemplate[] {load("name", typeReference(String.class))},
                            new TypeReference[] {typeReference(String.class)})
                    .put(
                            self(simple.handle()),
                            String.class,
                            "foo",
                            cast(String.class, load("foo", typeReference(Object.class))))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance =
                constructor(handle.loadClass(), String.class, Object.class).invoke("Pontus", "Tobias");

        // then
        assertEquals("SimpleClass", instance.getClass().getSimpleName());
        assertThat(instance).isInstanceOf(NamedBase.class);
        assertEquals("Pontus", ((NamedBase) instance).name);
        assertEquals("Tobias", getField(instance, "foo"));
    }

    @Test
    void shouldBeAbleToCastSomePrimitiveTypes() throws Throwable {
        castTest(Integer.TYPE, 42, Long.TYPE, 42L);
        castTest(Float.TYPE, 42.0F, Long.TYPE, 42L);
        castTest(Double.TYPE, 42.0, Long.TYPE, 42L);
        castTest(Long.TYPE, 42L, Integer.TYPE, 42);
        castTest(Float.TYPE, 42.0F, Integer.TYPE, 42);
        castTest(Double.TYPE, 42.0D, Integer.TYPE, 42);
    }

    @Test
    void shouldBeAbleToBox() throws Throwable {
        assertThat(boxTest(boolean.class, true)).isEqualTo(Boolean.TRUE);
        assertThat(boxTest(boolean.class, false)).isEqualTo(Boolean.FALSE);
        assertThat(boxTest(byte.class, (byte) 12)).isEqualTo((byte) 12);
        assertThat(boxTest(short.class, (short) 12)).isEqualTo((short) 12);
        assertThat(boxTest(int.class, 12)).isEqualTo(12);
        assertThat(boxTest(long.class, 12L)).isEqualTo(12L);
        assertThat(boxTest(float.class, 12F)).isEqualTo(12F);
        assertThat(boxTest(double.class, 12D)).isEqualTo(12D);
        assertThat(boxTest(char.class, 'a')).isEqualTo('a');
    }

    @Test
    void shouldBeAbleToUnbox() throws Throwable {
        assertThat(unboxTest(Boolean.class, boolean.class, true)).isEqualTo(true);
        assertThat(unboxTest(Boolean.class, boolean.class, false)).isEqualTo(false);
        assertThat(unboxTest(Byte.class, byte.class, (byte) 12)).isEqualTo((byte) 12);
        assertThat(unboxTest(Short.class, short.class, (short) 12)).isEqualTo((short) 12);
        assertThat(unboxTest(Integer.class, int.class, 12)).isEqualTo(12);
        assertThat(unboxTest(Long.class, long.class, 12L)).isEqualTo(12L);
        assertThat(unboxTest(Float.class, float.class, 12F)).isEqualTo(12F);
        assertThat(unboxTest(Double.class, double.class, 12D)).isEqualTo(12D);
        assertThat(unboxTest(Character.class, char.class, 'a')).isEqualTo('a');
    }

    @Test
    void shouldHandleInfinityAndNan() throws Throwable {
        assertTrue(
                Double.isInfinite(generateDoubleMethod(Double.POSITIVE_INFINITY).get()));
        assertTrue(
                Double.isInfinite(generateDoubleMethod(Double.NEGATIVE_INFINITY).get()));
        assertTrue(Double.isNaN(generateDoubleMethod(Double.NaN).get()));
    }

    @Test
    void shouldGenerateInstanceOf() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock conditional =
                    simple.generateMethod(boolean.class, "isString", param(Object.class, "test"))) {
                conditional.returns(Expression.instanceOf(typeReference(String.class), conditional.load("test")));
            }

            handle = simple.handle();
        }

        // when
        MethodHandle isString = instanceMethod(handle.newInstance(), "isString", Object.class);

        // then
        assertTrue((Boolean) isString.invoke("this is surely a string"));
        assertFalse((Boolean) isString.invoke("this is surely a string".length()));
    }

    @Test
    void shouldUpdateStaticField() throws Throwable {
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            FieldReference foo = simple.privateStaticField(int.class, "FOO", constant(42));
            try (CodeBlock get = simple.generateMethod(int.class, "get")) {
                get.putStatic(foo, constant(84));
                get.returns(Expression.getStatic(foo));
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod(handle.newInstance(), "get").invoke();

        // then
        assertEquals(84, foo);
    }

    @Test
    void shouldAccessArray() throws Throwable {
        assertArrayLoad(long.class, long[].class, new long[] {1L, 2L, 3L}, 2, 3L);
        assertArrayLoad(int.class, int[].class, new int[] {1, 2, 3}, 1, 2);
        assertArrayLoad(short.class, short[].class, new short[] {1, 2, 3}, 1, (short) 2);
        assertArrayLoad(byte.class, byte[].class, new byte[] {1, 2, 3}, 0, (byte) 1);
        assertArrayLoad(char.class, char[].class, new char[] {'a', 'b', 'c'}, 2, 'c');
        assertArrayLoad(float.class, float[].class, new float[] {1, 2, 3}, 2, 3F);
        assertArrayLoad(double.class, double[].class, new double[] {1, 2, 3}, 2, 3D);
        assertArrayLoad(boolean.class, boolean[].class, new boolean[] {true, false, true}, 2, true);
        assertArrayLoad(String.class, String[].class, new String[] {"a", "b", "c"}, 2, "c");
    }

    @Test
    void shouldCreateAndPopulatePrimitiveArray() throws Throwable {
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("LongArrayClass")) {
            try (CodeBlock body =
                    simple.generateMethod(long[].class, "get", param(long.class, "p1"), param(long.class, "p2"))) {
                body.assign(typeReference(long[].class), "array", newArray(typeReference(long.class), 2));
                body.expression(arraySet(body.load("array"), constant(1), body.load("p1")));
                body.expression(arraySet(body.load("array"), constant(0), body.load("p2")));
                body.returns(body.load("array"));
            }
            handle = simple.handle();
        }

        assertArrayEquals(
                new long[] {4L, 3L}, (long[]) instanceMethod(handle.newInstance(), "get", long.class, long.class)
                        .invoke(3L, 4L));
    }

    @Test
    void shouldCreateAndPopulateNonPrimitiveArray() throws Throwable {
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("StringArrayClass")) {
            try (CodeBlock body = simple.generateMethod(
                    String[].class, "get", param(String.class, "p1"), param(String.class, "p2"))) {
                body.assign(typeReference(String[].class), "array", newArray(typeReference(String.class), 2));
                body.expression(arraySet(body.load("array"), constant(1), body.load("p1")));
                body.expression(arraySet(body.load("array"), constant(0), body.load("p2")));
                body.returns(body.load("array"));
            }
            handle = simple.handle();
        }

        assertArrayEquals(new String[] {"b", "a"}, (String[])
                instanceMethod(handle.newInstance(), "get", String.class, String.class)
                        .invoke("a", "b"));
    }

    @Test
    void shouldCheckLengthOfArray() throws Throwable {
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("lengthOfArray")) {
            try (CodeBlock body = simple.generateMethod(int.class, "length", param(long[].class, "array"))) {
                body.returns(Expression.arrayLength(body.load("array")));
            }
            handle = simple.handle();
        }

        assertEquals(3, (int)
                instanceMethod(handle.newInstance(), "length", long[].class).invoke(new long[] {3L, 4L, 3L}));
    }

    @Test
    void shouldCreateAndPopulateArrayWithDynamicSize() throws Throwable {
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("LongArrayClass")) {
            try (CodeBlock body =
                    simple.generateMethod(long[].class, "get", param(int.class, "size"), param(long.class, "value"))) {
                body.assign(
                        typeReference(long[].class), "array", newArray(typeReference(long.class), body.load("size")));
                var i = body.declare(typeReference(int.class), "i");
                body.assign(i, constant(0));
                try (var innerBody = body.whileLoop(Expression.lt(body.load("i"), body.load("size")))) {
                    innerBody.expression(arraySet(body.load("array"), innerBody.load("i"), innerBody.load("value")));
                    innerBody.assign(i, add(innerBody.load("i"), constant(1)));
                }
                body.returns(body.load("array"));
            }
            handle = simple.handle();
        }

        assertArrayEquals(
                new long[] {42L, 42L, 42L}, (long[]) instanceMethod(handle.newInstance(), "get", int.class, long.class)
                        .invoke(3, 42L));
    }

    private <T, U> void assertArrayLoad(Class<T> returnType, Class<U> arrayType, U array, int index, T expected)
            throws Throwable {
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass" + returnType.getSimpleName())) {
            try (CodeBlock body =
                    simple.generateMethod(returnType, "get", param(arrayType, "array"), param(int.class, "index"))) {
                body.returns(arrayLoad(body.load("array"), body.load("index")));
            }
            handle = simple.handle();
        }

        assertEquals(
                expected,
                instanceMethod(handle.newInstance(), "get", arrayType, int.class)
                        .invoke(array, index));
    }

    private Supplier<Double> generateDoubleMethod(double toBeReturned) throws Throwable {
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            simple.generate(MethodTemplate.method(double.class, "value")
                    .returns(constant(toBeReturned))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance = constructor(handle.loadClass()).invoke();

        MethodHandle method = instanceMethod(instance, "value");
        return () -> {
            try {
                return (Double) method.invoke();
            } catch (Throwable throwable) {
                throw new AssertionError(throwable);
            }
        };
    }

    private <FROM, TO> void castTest(Class<FROM> fromType, FROM fromValue, Class<TO> toType, TO toValue)
            throws Throwable {
        // given
        ClassHandle handle;
        String simpleClassName =
                "SimpleClass" + Integer.toHexString(UUID.randomUUID().hashCode());
        try (ClassGenerator simple = generateClass(NamedBase.class, simpleClassName)) {
            simple.field(toType, "toValue");
            simple.generate(MethodTemplate.constructor(param(String.class, "name"), param(fromType, "fromValue"))
                    .invokeSuper(
                            new ExpressionTemplate[] {load("name", typeReference(String.class))},
                            new TypeReference[] {typeReference(String.class)})
                    // Add and then subtract fromValue to get a more complex expression with the same result, i.e.
                    // toValue = (toType) ((fromValue + fromValue) - fromValue)
                    .put(
                            self(simple.handle()),
                            toType,
                            "toValue",
                            cast(
                                    toType,
                                    subtract(
                                            add(
                                                    load("fromValue", typeReference(fromType)),
                                                    load("fromValue", typeReference(fromType)),
                                                    typeReference(fromType)),
                                            load("fromValue", typeReference(fromType)),
                                            typeReference(fromType))))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance =
                constructor(handle.loadClass(), String.class, fromType).invoke("Pontus", fromValue);

        // then
        assertEquals(simpleClassName, instance.getClass().getSimpleName());
        assertThat(instance).isInstanceOf(NamedBase.class);
        assertEquals("Pontus", ((NamedBase) instance).name);
        assertEquals(toValue, getField(instance, "toValue"));
    }

    private <T> Object unboxTest(Class<T> boxedType, Class<?> unboxedType, T value) throws Throwable {
        createGenerator();
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock method = simple.generateMethod(unboxedType, "unbox", param(boxedType, "test"))) {
                method.returns(Expression.unbox(method.load("test")));
            }

            handle = simple.handle();
        }

        // when
        return instanceMethod(handle.newInstance(), "unbox", boxedType).invoke(value);
    }

    private <T> Object boxTest(Class<T> unboxedType, T value) throws Throwable {
        createGenerator();
        // given
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            try (CodeBlock method = simple.generateMethod(Object.class, "box", param(unboxedType, "test"))) {
                method.returns(Expression.box(method.load("test")));
            }

            handle = simple.handle();
        }

        // when
        return instanceMethod(handle.newInstance(), "box", unboxedType).invoke(value);
    }

    private static MethodHandle method(Class<?> target, String name, Class<?>... parameters) throws Exception {
        return MethodHandles.lookup().unreflect(target.getMethod(name, parameters));
    }

    private static MethodHandle instanceMethod(Object instance, String name, Class<?>... parameters) throws Exception {
        return method(instance.getClass(), name, parameters).bindTo(instance);
    }

    private static Object getField(Object instance, String field) throws Exception {
        return instance.getClass().getField(field).get(instance);
    }

    private static MethodHandle constructor(Class<?> target, Class<?>... parameters) throws Exception {
        return MethodHandles.lookup().unreflectConstructor(target.getConstructor(parameters));
    }

    public static final String PACKAGE = "org.neo4j.codegen.test";
    private CodeGenerator generator;

    private ClassGenerator generateClass(Class<?> base, String name, Class<?>... interfaces) {
        return generator.generateClass(base, PACKAGE, name, interfaces);
    }

    private ClassGenerator generateClass(String name, TypeReference... interfaces) {
        return generator.generateClass(PACKAGE, name, interfaces);
    }

    public static class NamedBase {
        final String name;
        private boolean defaultConstructorCalled;

        public NamedBase() {
            this.defaultConstructorCalled = true;
            this.name = null;
        }

        public NamedBase(String name) {
            this.name = name;
        }

        public boolean defaultConstructorCalled() {
            return defaultConstructorCalled;
        }
    }

    public static class SomeBean {
        private String foo;
        private String bar;

        public void setFoo(String foo) {
            this.foo = foo;
        }

        public void setBar(String bar) {
            this.bar = bar;
        }
    }

    private <T> void assertMethodReturningField(Class<T> clazz, T argument) throws Throwable {
        // given
        createGenerator();
        ClassHandle handle;
        try (ClassGenerator simple = generateClass("SimpleClass")) {
            FieldReference value = simple.field(clazz, "value");
            simple.generate(MethodTemplate.constructor(param(clazz, "value"))
                    .invokeSuper()
                    .put(self(simple.handle()), value.type(), value.name(), load("value", value.type()))
                    .build());
            simple.generate(MethodTemplate.method(clazz, "value")
                    .returns(ExpressionTemplate.get(self(simple.handle()), clazz, "value"))
                    .build());
            handle = simple.handle();
        }

        // when
        Object instance = constructor(handle.loadClass(), clazz).invoke(argument);

        // then
        assertEquals(argument, instanceMethod(instance, "value").invoke());
    }

    private static MethodReference createMethod(Class<?> owner, Class<?> returnType, String name) {
        return methodReference(Runnable.class, void.class, "run");
    }

    public static class MyFirstException extends RuntimeException {}

    public static class MySecondException extends RuntimeException {}

    public static class MyThirdException extends RuntimeException {}

    private Expression newRuntimeException(String msg) {
        return invoke(
                newInstance(RuntimeException.class),
                constructorReference(RuntimeException.class, String.class),
                constant(msg));
    }
}

class ByteCodeCodeGenerationTest extends CodeGenerationTest {

    @Override
    CodeGenerator getGenerator() {
        try {
            return CodeGenerator.generateCode(ByteCode.BYTECODE);
        } catch (CodeGenerationNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}

class SourceCodeCodeGenerationTest extends CodeGenerationTest {

    @Override
    CodeGenerator getGenerator() {
        try {
            return CodeGenerator.generateCode(SourceCode.SOURCECODE);
        } catch (CodeGenerationNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
