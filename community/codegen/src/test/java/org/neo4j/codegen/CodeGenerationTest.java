/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.codegen;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InOrder;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import org.neo4j.codegen.source.Configuration;
import org.neo4j.codegen.source.SourceCode;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.codegen.Expression.add;
import static org.neo4j.codegen.Expression.and;
import static org.neo4j.codegen.Expression.constant;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.multiply;
import static org.neo4j.codegen.Expression.newArray;
import static org.neo4j.codegen.Expression.newInstance;
import static org.neo4j.codegen.Expression.not;
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

@RunWith( Parameterized.class )
public class CodeGenerationTest
{
    public static final MethodReference RUN = createMethod( Runnable.class, void.class, "run" );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> generators()
    {
        return Arrays.asList( new Object[]{SourceCode.SOURCECODE}, new Object[]{SourceCode.BYTECODE} );
    }

    @Parameterized.Parameter( 0 )
    public CodeGenerationStrategy<Configuration> strategy;

    @Before
    public void createGenerator()
    {
        try
        {
            generator = CodeGenerator.generateCode( strategy );
        }
        catch ( CodeGenerationNotSupportedException e )
        {
            throw new AssertionError( "Cannot compile code.", e );
        }
    }

    @Test
    public void shouldGenerateClass() throws Exception
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            handle = simple.handle();
        }

        // when
        Class<?> aClass = handle.loadClass();

        // then
        assertNotNull( "null class loaded", aClass );
        assertNotNull( "null package of: " + aClass.getName(), aClass.getPackage() );
        assertEquals( PACKAGE, aClass.getPackage().getName() );
        assertEquals( "SimpleClass", aClass.getSimpleName() );
    }

    @Test
    public void shouldGenerateTwoClassesInTheSamePackage() throws Exception
    {
        // given
        ClassHandle one, two;
        try ( ClassGenerator simple = generateClass( "One" ) )
        {
            one = simple.handle();
        }
        try ( ClassGenerator simple = generateClass( "Two" ) )
        {
            two = simple.handle();
        }

        // when
        Class<?> classOne = one.loadClass();
        Class<?> classTwo = two.loadClass();

        // then
        assertNotNull( classOne.getPackage() );
        assertSame( classOne.getPackage(), classTwo.getPackage() );
        assertEquals( "One", classOne.getSimpleName() );
        assertEquals( "Two", classTwo.getSimpleName() );
    }

    @Test
    public void shouldGenerateDefaultConstructor() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( NamedBase.class, "SimpleClass" ) )
        {
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass() ).invoke();
        Object constructorCalled = instanceMethod( instance, "defaultConstructorCalled" ).invoke();

        // then
        assertTrue( (Boolean) constructorCalled );
    }

    @Test
    public void shouldGenerateField() throws Exception
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            simple.field( String.class, "theField" );
            handle = simple.handle();
        }

        // when
        Class<?> clazz = handle.loadClass();

        // then
        Field theField = clazz.getDeclaredField( "theField" );
        assertSame( String.class, theField.getType() );
    }

    @Test
    public void shouldGenerateParameterizedTypeField() throws Exception
    {
        // given
        ClassHandle handle;
        TypeReference stringList = TypeReference.parameterizedType( List.class, String.class );
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            simple.field( stringList, "theField" );
            handle = simple.handle();
        }

        // when
        Class<?> clazz = handle.loadClass();

        // then
        Field theField = clazz.getDeclaredField( "theField" );
        assertSame( List.class, theField.getType() );
    }

    @Test
    public void shouldGenerateMethodReturningFieldValue() throws Throwable
    {
        assertMethodReturningField( byte.class, (byte) 42 );
        assertMethodReturningField( short.class, (short) 42 );
        assertMethodReturningField( char.class, (char) 42 );
        assertMethodReturningField( int.class, 42 );
        assertMethodReturningField( long.class, 42L );
        assertMethodReturningField( float.class, 42F );
        assertMethodReturningField( double.class, 42D );
        assertMethodReturningField( String.class, "42" );
        assertMethodReturningField( int[].class, new int[]{42} );
    }

    @Test
    public void shouldGenerateMethodReturningArrayValue() throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {

            simple.generate( MethodTemplate.method( int[].class, "value" )
                    .returns( newArray( typeReference( int.class ), constant( 1 ), constant( 2 ), constant( 3 ) ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass() ).invoke();

        // then
        assertArrayEquals( new int[]{1, 2, 3}, (int[]) instanceMethod( instance, "value" ).invoke() );
    }

    @Test
    public void shouldGenerateMethodReturningParameterizedTypeValue() throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            TypeReference stringList = parameterizedType( List.class, String.class );
            simple.generate( MethodTemplate.method( stringList, "value" )
                    .returns(
                            Expression.invoke(
                                    methodReference( Arrays.class, stringList, "asList", Object[].class ),
                                    newArray( typeReference( String.class ), constant( "a" ), constant( "b" ) ) ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass() ).invoke();

        // then
        assertEquals( Arrays.asList( "a", "b" ), instanceMethod( instance, "value" ).invoke() );
    }

    @Test
    public void shouldGenerateStaticPrimitiveField() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            FieldReference foo = simple.staticField( int.class, "FOO", constant( 42 ) );
            try ( CodeBlock get = simple.generateMethod( int.class, "get" ) )
            {
                get.returns( Expression.getStatic( foo ) );
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod( handle.newInstance(), "get" ).invoke();

        // then
        assertEquals( 42, foo );
    }

    @Test
    public void shouldGenerateStaticReferenceTypeField() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            FieldReference foo = simple.staticField( String.class, "FOO", constant( "42" ) );
            try ( CodeBlock get = simple.generateMethod( String.class, "get" ) )
            {
                get.returns( Expression.getStatic( foo ) );
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod( handle.newInstance(), "get" ).invoke();

        // then
        assertEquals( "42", foo );
    }

    @Test
    public void shouldGenerateStaticParameterizedTypeField() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            TypeReference stringList = TypeReference.parameterizedType( List.class, String.class );
            FieldReference foo = simple.staticField( stringList, "FOO", Expression.invoke(
                    methodReference( Arrays.class, stringList, "asList", Object[].class ),
                    newArray( typeReference( String.class ),
                            constant( "FOO" ), constant( "BAR" ), constant( "BAZ" ) ) ) );
            try ( CodeBlock get = simple.generateMethod( stringList, "get" ) )
            {
                get.returns( Expression.getStatic( foo ) );
            }
            handle = simple.handle();
        }

        // when
        Object foo = instanceMethod( handle.newInstance(), "get" ).invoke();

        // then
        assertEquals( Arrays.asList( "FOO", "BAR", "BAZ" ), foo );
    }

    public interface Thrower<E extends Exception>
    {
        void doThrow() throws E;
    }

    @Test
    public void shouldThrowParameterizedCheckedException() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock fail = simple.generate( MethodDeclaration.method( void.class, "fail",
                    param( TypeReference.parameterizedType( Thrower.class, typeParameter( "E" ) ), "thrower" ) )
                    .parameterizedWith( "E", extending( Exception.class ) )
                    .throwsException( typeParameter( "E" ) ) ) )
            {
                fail.expression(
                        invoke( fail.load( "thrower" ), methodReference( Thrower.class, void.class, "doThrow" ) ) );
            }
            handle = simple.handle();
        }

        // when
        try
        {
            instanceMethod( handle.newInstance(), "fail", Thrower.class ).invoke( (Thrower<IOException>) () -> {
                throw new IOException( "Hello from the inside" );
            } );

            fail( "expected exception" );
        }
        // then
        catch ( IOException e )
        {
            assertEquals( "Hello from the inside", e.getMessage() );
        }
    }

    @Test
    public void shouldAssignLocalVariable() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock create = simple.generateMethod( SomeBean.class, "createBean",
                    param( String.class, "foo" ), param( String.class, "bar" ) ) )
            {
                create.assign( SomeBean.class, "bean",
                        invoke( newInstance( SomeBean.class ), constructorReference( SomeBean.class ) ) );
                create.expression( invoke( create.load( "bean" ),
                        methodReference( SomeBean.class, void.class, "setFoo", String.class ),
                        create.load( "foo" ) ) );
                create.expression( invoke( create.load( "bean" ),
                        methodReference( SomeBean.class, void.class, "setBar", String.class ),
                        create.load( "bar" ) ) );
                create.returns( create.load( "bean" ) );
            }
            handle = simple.handle();
        }

        // when
        MethodHandle method = instanceMethod( handle.newInstance(), "createBean", String.class, String.class );
        SomeBean bean = (SomeBean) method.invoke( "hello", "world" );

        // then
        assertEquals( "hello", bean.foo );
        assertEquals( "world", bean.bar );
    }

    @Test
    public void shouldDeclareAndAssignLocalVariable() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock create = simple.generateMethod( SomeBean.class, "createBean",
                    param( String.class, "foo" ), param( String.class, "bar" ) ) )
            {
                LocalVariable localVariable = create.declare( typeReference( SomeBean.class ), "bean" );
                create.assign( localVariable, invoke(
                        newInstance( SomeBean.class ), constructorReference( SomeBean.class ) ) );
                create.expression( invoke( create.load( "bean" ),
                        methodReference( SomeBean.class, void.class, "setFoo", String.class ),
                        create.load( "foo" ) ) );
                create.expression( invoke( create.load( "bean" ),
                        methodReference( SomeBean.class, void.class, "setBar", String.class ),
                        create.load( "bar" ) ) );
                create.returns( create.load( "bean" ) );
            }
            handle = simple.handle();
        }

        // when
        MethodHandle method = instanceMethod( handle.newInstance(), "createBean", String.class, String.class );
        SomeBean bean = (SomeBean) method.invoke( "hello", "world" );

        // then
        assertEquals( "hello", bean.foo );
        assertEquals( "world", bean.bar );
    }

    @Test
    public void shouldGenerateWhileLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterator.class, Runnable.class ), "targets" ) ) )
            {
                try ( CodeBlock loop = callEach.whileLoop( invoke( callEach.load( "targets" ),
                        methodReference( Iterator.class, boolean.class, "hasNext" ) ) ) )
                {
                    loop.expression( invoke(
                            Expression.cast( Runnable.class,
                                    invoke( callEach.load( "targets" ),
                                            methodReference( Iterator.class, Object.class, "next" ) ) ),
                            methodReference( Runnable.class, void.class, "run" ) ) );
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );

        // when
        MethodHandle callEach = instanceMethod( handle.newInstance(), "callEach", Iterator.class );
        callEach.invoke( Arrays.asList( a, b, c ).iterator() );

        // then
        InOrder order = inOrder( a, b, c );
        order.verify( a ).run();
        order.verify( b ).run();
        order.verify( c ).run();
        verifyNoMoreInteractions( a, b, c );
    }

    @Test
    public void shouldGenerateWhileLoopWithMultipleTestExpressions() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "check",
                    param( boolean.class, "a" ), param( boolean.class, "b" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock loop = callEach.whileLoop( callEach.load( "a" ), callEach.load( "b" ) ) )
                {
                    loop.expression( invoke(
                            loop.load( "runner" ),
                            methodReference( Runnable.class, void.class, "run" ) ) );
                    loop.returns();
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );
        Runnable d = mock( Runnable.class );

        // when
        MethodHandle callEach =
                instanceMethod( handle.newInstance(), "check", boolean.class, boolean.class, Runnable.class );
        callEach.invoke( true, true, a );
        callEach.invoke( true, false, b );
        callEach.invoke( false, true, c );
        callEach.invoke( false, false, d );

        // then
        verify( a ).run();
        verifyNoMoreInteractions( a );
        verifyZeroInteractions( b );
        verifyZeroInteractions( c );
        verifyZeroInteractions( d );

    }

    @Test
    public void shouldGenerateNestedWhileLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterator.class, Runnable.class ), "targets" ) ) )
            {
                try ( CodeBlock loop = callEach.whileLoop( invoke( callEach.load( "targets" ),
                        methodReference( Iterator.class, boolean.class, "hasNext" ) ) ) )
                {
                    try ( CodeBlock inner = loop.whileLoop( invoke( callEach.load( "targets" ),
                            methodReference( Iterator.class, boolean.class, "hasNext" ) ) ) )
                    {

                        inner.expression( invoke(
                                Expression.cast( Runnable.class,
                                        invoke( callEach.load( "targets" ),
                                                methodReference( Iterator.class, Object.class, "next" ) ) ),
                                methodReference( Runnable.class, void.class, "run" ) ) );
                    }
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );

        // when
        MethodHandle callEach = instanceMethod( handle.newInstance(), "callEach", Iterator.class );
        callEach.invoke( Arrays.asList( a, b, c ).iterator() );

        // then
        InOrder order = inOrder( a, b, c );
        order.verify( a ).run();
        order.verify( b ).run();
        order.verify( c ).run();
        verifyNoMoreInteractions( a, b, c );
    }

    @Test
    public void shouldGenerateForEachLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterable.class, Runnable.class ), "targets" ) ) )
            {
                try ( CodeBlock loop = callEach
                        .forEach( param( Runnable.class, "runner" ), callEach.load( "targets" ) ) )
                {
                    loop.expression(
                            invoke( loop.load( "runner" ),
                                    methodReference( Runnable.class, void.class, "run" ) ) );
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );

        // when
        MethodHandle callEach = instanceMethod( handle.newInstance(), "callEach", Iterable.class );
        callEach.invoke( Arrays.asList( a, b, c ) );

        // then
        InOrder order = inOrder( a, b, c );
        order.verify( a ).run();
        order.verify( b ).run();
        order.verify( c ).run();
        verifyNoMoreInteractions( a, b, c );
    }

    @Test
    public void shouldGenerateIfStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifStatement( conditional.load( "test" ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", boolean.class, Runnable.class );
        conditional.invoke( true, runner1 );
        conditional.invoke( false, runner2 );

        // then
        verify( runner1 ).run();
        verifyZeroInteractions( runner2 );
    }

    @Test
    public void shouldGenerateIfWithMultipleTestsStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ),
                    param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional
                        .ifStatement( conditional.load( "test1" ), conditional.load( "test2" ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );
        Runnable runner3 = mock( Runnable.class );
        Runnable runner4 = mock( Runnable.class );

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class );
        conditional.invoke( true, true, runner1 );
        conditional.invoke( false, true, runner2 );
        conditional.invoke( true, false, runner3 );
        conditional.invoke( false, false, runner4 );

        // then
        verify( runner1 ).run();
        verifyZeroInteractions( runner2 );
        verifyZeroInteractions( runner3 );
        verifyZeroInteractions( runner4 );
    }

    @Test
    public void shouldGenerateIfNotExpressionStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifStatement( not( conditional.load( "test" ) ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", boolean.class, Runnable.class );
        conditional.invoke( true, runner1 );
        conditional.invoke( false, runner2 );

        // then
        verify( runner2 ).run();
        verifyZeroInteractions( runner1 );
    }

    @Test
    public void shouldGenerateIfNotStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifNotStatement( conditional.load( "test" ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", boolean.class, Runnable.class );
        conditional.invoke( true, runner1 );
        conditional.invoke( false, runner2 );

        // then
        verify( runner2 ).run();
        verifyZeroInteractions( runner1 );
    }

    @Test
    public void shouldGenerateIfNullStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( Object.class, "test" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifNullStatement( conditional.load( "test" ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", Object.class, Runnable.class );
        conditional.invoke( null, runner1 );
        conditional.invoke( new Object(), runner2 );

        // then
        verify( runner1 ).run();
        verifyZeroInteractions( runner2 );
    }

    @Test
    public void shouldGenerateIfNonNullStatement() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( Object.class, "test" ), param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifNonNullStatement( conditional.load( "test" ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", Object.class, Runnable.class );
        conditional.invoke( new Object(), runner1 );
        conditional.invoke( null, runner2 );

        // then
        verify( runner1 ).run();
        verifyZeroInteractions( runner2 );
    }

    @Test
    public void shouldGenerateTryWithNestedWhileIfLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterator.class, Runnable.class ), "targets" ),
                    param( boolean.class, "test" ),
                    param( Runnable.class, "runner" ) ) )
            {
                callEach.tryCatch( ( tryBlock ) ->
                        {
                            try ( CodeBlock loop = tryBlock.whileLoop( invoke( callEach.load( "targets" ),
                                    methodReference( Iterator.class, boolean.class, "hasNext" ) ) ) )
                            {

                                try ( CodeBlock doStuff = loop.ifStatement( not( callEach.load( "test" ) ) ) )
                                {
                                    doStuff.expression(
                                            invoke( doStuff.load( "runner" ), RUN ) );
                                }
                                loop.expression( invoke(
                                        Expression.cast( Runnable.class,
                                                invoke( callEach.load( "targets" ),
                                                        methodReference( Iterator.class, Object.class, "next" ) ) ),
                                        methodReference( Runnable.class, void.class, "run" ) ) );

                            }
                        },
                        ( catchBlock ) -> catchBlock.expression(
                                invoke( catchBlock.load( "runner" ), RUN ) ), param( RuntimeException.class, "e" ) );
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );

        // when
        MethodHandle callEach =
                instanceMethod( handle.newInstance(), "callEach", Iterator.class, boolean.class, Runnable.class );

        callEach.invoke( Arrays.asList( a, b, c ).iterator(), false, runner1 );
        callEach.invoke( Arrays.asList( a, b, c ).iterator(), true, runner2 );

        // then
        verify( runner1, times( 3 ) ).run();
        verify( runner2, times( 0 ) ).run();
    }

    @Test
    public void shouldGenerateWhileWithNestedIfLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterator.class, Runnable.class ), "targets" ),
                    param( boolean.class, "test" ),
                    param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock loop = callEach.whileLoop( invoke( callEach.load( "targets" ),
                        methodReference( Iterator.class, boolean.class, "hasNext" ) ) ) )
                {
                    try ( CodeBlock doStuff = loop.ifStatement( not( callEach.load( "test" ) ) ) )
                    {
                        doStuff.expression(
                                invoke( doStuff.load( "runner" ), RUN ) );
                    }
                    loop.expression( invoke(
                            Expression.cast( Runnable.class,
                                    invoke( callEach.load( "targets" ),
                                            methodReference( Iterator.class, Object.class, "next" ) ) ),
                            methodReference( Runnable.class, void.class, "run" ) ) );
                }
            }

            handle = simple.handle();
        }
        Runnable a = mock( Runnable.class );
        Runnable b = mock( Runnable.class );
        Runnable c = mock( Runnable.class );

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );
        // when
        MethodHandle callEach =
                instanceMethod( handle.newInstance(), "callEach", Iterator.class, boolean.class, Runnable.class );

        callEach.invoke( Arrays.asList( a, b, c ).iterator(), false, runner1 );
        callEach.invoke( Arrays.asList( a, b, c ).iterator(), true, runner2 );

        // then
        verify( runner1, times( 3 ) ).run();
        verify( runner2, never() ).run();

    }

    @Test
    public void shouldGenerateOr() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ),
                    param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifStatement( Expression.or( conditional.load( "test1" ),
                        conditional.load( "test2" ) ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );
        Runnable runner3 = mock( Runnable.class );
        Runnable runner4 = mock( Runnable.class );

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class );
        conditional.invoke( true, true, runner1 );
        conditional.invoke( true, false, runner2 );
        conditional.invoke( false, true, runner3 );
        conditional.invoke( false, false, runner4 );

        // then
        verify( runner1 ).run();
        verify( runner2 ).run();
        verify( runner3 ).run();
        verifyZeroInteractions( runner4 );
    }

    @Test
    public void shouldGenerateMethodUsingOr() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( boolean.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ) ) )
            {
                conditional.returns( or( conditional.load( "test1" ), conditional.load( "test2" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class );

        // then
        assertThat( conditional.invoke( true, true ), equalTo( true ) );
        assertThat( conditional.invoke( true, false ), equalTo( true ) );
        assertThat( conditional.invoke( false, true ), equalTo( true ) );
        assertThat( conditional.invoke( false, false ), equalTo( false ) );
    }

    @Test
    public void shouldGenerateAnd() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ),
                    param( Runnable.class, "runner" ) ) )
            {
                try ( CodeBlock doStuff = conditional.ifStatement( and( conditional.load( "test1" ),
                        conditional.load( "test2" ) ) ) )
                {
                    doStuff.expression(
                            invoke( doStuff.load( "runner" ), RUN ) );
                }
            }

            handle = simple.handle();
        }

        Runnable runner1 = mock( Runnable.class );
        Runnable runner2 = mock( Runnable.class );
        Runnable runner3 = mock( Runnable.class );
        Runnable runner4 = mock( Runnable.class );

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class, Runnable.class );
        conditional.invoke( true, true, runner1 );
        conditional.invoke( true, false, runner2 );
        conditional.invoke( false, true, runner3 );
        conditional.invoke( false, false, runner4 );

        // then
        verify( runner1 ).run();
        verifyZeroInteractions( runner2 );
        verifyZeroInteractions( runner3 );
        verifyZeroInteractions( runner4 );
    }

    @Test
    public void shouldGenerateMethodUsingAnd() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( boolean.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ) ) )
            {
                conditional.returns( and( conditional.load( "test1" ), conditional.load( "test2" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class );

        // then
        assertThat( conditional.invoke( true, true ), equalTo( true ) );
        assertThat( conditional.invoke( true, false ), equalTo( false ) );
        assertThat( conditional.invoke( false, true ), equalTo( false ) );
        assertThat( conditional.invoke( false, false ), equalTo( false ) );
    }

    @Test
    public void shouldHandleNot() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( boolean.class, "conditional",
                    param( boolean.class, "test" ) ) )
            {
                conditional.returns( not( conditional.load( "test" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class );

        // then
        assertThat( conditional.invoke( true ), equalTo( false ) );
        assertThat( conditional.invoke( false ), equalTo( true ) );
    }

    @Test
    public void shouldHandleTernaryOperator() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock ternaryBlock = simple.generateMethod( String.class, "ternary",
                    param( boolean.class, "test" ), param( TernaryChecker.class, "check" ) ) )
            {
                ternaryBlock.returns(
                        ternary( ternaryBlock.load( "test" ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onTrue" ) ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onFalse" ) ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary =
                instanceMethod( handle.newInstance(), "ternary", boolean.class, TernaryChecker.class );

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat( ternary.invoke( true, checker1 ), equalTo( "on true" ) );
        assertTrue( checker1.ranOnTrue );
        assertFalse( checker1.ranOnFalse );

        TernaryChecker checker2 = new TernaryChecker();
        assertThat( ternary.invoke( false, checker2 ), equalTo( "on false" ) );
        assertFalse( checker2.ranOnTrue );
        assertTrue( checker2.ranOnFalse );
    }

    @Test
    public void shouldHandleTernaryOnNullOperator() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock ternaryBlock = simple.generateMethod( String.class, "ternary",
                    param( Object.class, "test" ), param( TernaryChecker.class, "check" ) ) )
            {
                ternaryBlock.returns(
                        Expression.ternaryOnNull( ternaryBlock.load( "test" ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onTrue" ) ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onFalse" ) ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary =
                instanceMethod( handle.newInstance(), "ternary", Object.class, TernaryChecker.class );

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat( ternary.invoke( null, checker1 ), equalTo( "on true" ) );
        assertTrue( checker1.ranOnTrue );
        assertFalse( checker1.ranOnFalse );

        TernaryChecker checker2 = new TernaryChecker();
        assertThat( ternary.invoke( new Object(), checker2 ), equalTo( "on false" ) );
        assertFalse( checker2.ranOnTrue );
        assertTrue( checker2.ranOnFalse );
    }

    @Test
    public void shouldHandleTernaryOnNonNullOperator() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock ternaryBlock = simple.generateMethod( String.class, "ternary",
                    param( Object.class, "test" ), param( TernaryChecker.class, "check" ) ) )
            {
                ternaryBlock.returns(
                        Expression.ternaryOnNonNull( ternaryBlock.load( "test" ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onTrue" ) ),
                                invoke( ternaryBlock.load( "check" ),
                                        methodReference( TernaryChecker.class, String.class, "onFalse" ) ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle ternary =
                instanceMethod( handle.newInstance(), "ternary", Object.class, TernaryChecker.class );

        // then
        TernaryChecker checker1 = new TernaryChecker();
        assertThat( ternary.invoke( new Object(), checker1 ), equalTo( "on true" ) );
        assertTrue( checker1.ranOnTrue );
        assertFalse( checker1.ranOnFalse );

        TernaryChecker checker2 = new TernaryChecker();
        assertThat( ternary.invoke( null, checker2 ), equalTo( "on false" ) );
        assertFalse( checker2.ranOnTrue );
        assertTrue( checker2.ranOnFalse );
    }

    @Test
    public void shouldHandleEquality() throws Throwable
    {
        // boolean
        assertTrue( compareForType( boolean.class, true, true,
                Expression::equal ) );
        assertTrue( compareForType( boolean.class, false, false,
                Expression::equal ) );
        assertFalse( compareForType( boolean.class, true, false,
                Expression::equal ) );
        assertFalse( compareForType( boolean.class, false, true,
                Expression::equal ) );

        // byte
        assertTrue( compareForType( byte.class, (byte) 42, (byte) 42,
                Expression::equal ) );
        assertFalse( compareForType( byte.class, (byte) 43, (byte) 42,
                Expression::equal ) );
        assertFalse( compareForType( byte.class, (byte) 42, (byte) 43,
                Expression::equal ) );

        // short
        assertTrue( compareForType( short.class, (short) 42, (short) 42,
                Expression::equal ) );
        assertFalse( compareForType( short.class, (short) 43, (short) 42,
                Expression::equal ) );
        assertFalse( compareForType( short.class, (short) 42, (short) 43,
                Expression::equal ) );

        // char
        assertTrue( compareForType( char.class, (char) 42, (char) 42,
                Expression::equal ) );
        assertFalse( compareForType( char.class, (char) 43, (char) 42,
                Expression::equal ) );
        assertFalse( compareForType( char.class, (char) 42, (char) 43,
                Expression::equal ) );

        //int
        assertTrue( compareForType( int.class, 42, 42,
                Expression::equal ) );
        assertFalse( compareForType( int.class, 43, 42,
                Expression::equal ) );
        assertFalse( compareForType( int.class, 42, 43,
                Expression::equal ) );

        //long
        assertTrue( compareForType( long.class, 42L, 42L,
                Expression::equal ) );
        assertFalse( compareForType( long.class, 43L, 42L,
                Expression::equal ) );
        assertFalse( compareForType( long.class, 42L, 43L,
                Expression::equal ) );

        //float
        assertTrue( compareForType( float.class, 42F, 42F,
                Expression::equal ) );
        assertFalse( compareForType( float.class, 43F, 42F,
                Expression::equal ) );
        assertFalse( compareForType( float.class, 42F, 43F,
                Expression::equal ) );

        //double
        assertTrue( compareForType( double.class, 42D, 42D,
                Expression::equal ) );
        assertFalse( compareForType( double.class, 43D, 42D,
                Expression::equal ) );
        assertFalse( compareForType( double.class, 42D, 43D,
                Expression::equal ) );

        //reference
        Object obj1 = new Object();
        Object obj2 = new Object();
        assertTrue( compareForType( Object.class, obj1, obj1,
                Expression::equal ) );
        assertFalse( compareForType( Object.class, obj1, obj2,
                Expression::equal ) );
        assertFalse( compareForType( Object.class, obj2, obj1,
                Expression::equal ) );
    }

    @Test
    public void shouldHandleGreaterThan() throws Throwable
    {
        assertTrue( compareForType( float.class, 43F, 42F,
                Expression::gt ) );
        assertTrue( compareForType( long.class, 43L, 42L,
                Expression::gt ) );

        // byte
        assertTrue( compareForType( byte.class, (byte) 43, (byte) 42,
                Expression::gt ) );
        assertFalse( compareForType( byte.class, (byte) 42, (byte) 42,
                Expression::gt ) );
        assertFalse( compareForType( byte.class, (byte) 42, (byte) 43,
                Expression::gt ) );

        // short
        assertTrue( compareForType( short.class, (short) 43, (short) 42,
                Expression::gt ) );
        assertFalse( compareForType( short.class, (short) 42, (short) 42,
                Expression::gt ) );
        assertFalse( compareForType( short.class, (short) 42, (short) 43,
                Expression::gt ) );

        // char
        assertTrue( compareForType( char.class, (char) 43, (char) 42,
                Expression::gt ) );
        assertFalse( compareForType( char.class, (char) 42, (char) 42,
                Expression::gt ) );
        assertFalse( compareForType( char.class, (char) 42, (char) 43,
                Expression::gt ) );

        //int
        assertTrue(
                compareForType( int.class, 43, 42, Expression::gt ) );
        assertFalse(
                compareForType( int.class, 42, 42, Expression::gt ) );
        assertFalse(
                compareForType( int.class, 42, 43, Expression::gt ) );

        //long
        assertTrue( compareForType( long.class, 43L, 42L,
                Expression::gt ) );
        assertFalse( compareForType( long.class, 42L, 42L,
                Expression::gt ) );
        assertFalse( compareForType( long.class, 42L, 43L,
                Expression::gt ) );

        //float
        assertTrue( compareForType( float.class, 43F, 42F,
                Expression::gt ) );
        assertFalse( compareForType( float.class, 42F, 42F,
                Expression::gt ) );
        assertFalse( compareForType( float.class, 42F, 43F,
                Expression::gt ) );

        //double
        assertTrue( compareForType( double.class, 43D, 42D,
                Expression::gt ) );
        assertFalse( compareForType( double.class, 42D, 42D,
                Expression::gt ) );
        assertFalse( compareForType( double.class, 42D, 43D,
                Expression::gt ) );
    }

    @Test
    public void shouldHandleAddition() throws Throwable
    {
        assertThat( addForType( int.class, 17, 18 ), equalTo( 35 ) );
        assertThat( addForType( long.class, 17L, 18L ), equalTo( 35L ) );
        assertThat( addForType( double.class, 17D, 18D ), equalTo( 35D ) );
    }

    @Test
    public void shouldHandleSubtraction() throws Throwable
    {
        assertThat( subtractForType( int.class, 19, 18 ), equalTo( 1 ) );
        assertThat( subtractForType( long.class, 19L, 18L ), equalTo( 1L ) );
        assertThat( subtractForType( double.class, 19D, 18D ), equalTo( 1D ) );
    }

    @Test
    public void shouldHandleMultiplication() throws Throwable
    {
        assertThat( multiplyForType( int.class, 17, 18 ), equalTo( 306 ) );
        assertThat( multiplyForType( long.class, 17L, 18L ), equalTo( 306L ) );
        assertThat( multiplyForType( double.class, 17D, 18D ), equalTo( 306D ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T> T addForType( Class<T> clazz, T lhs, T rhs ) throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock block = simple.generateMethod( clazz, "add",
                    param( clazz, "a" ), param( clazz, "b" ) ) )
            {
                block.returns( add( block.load( "a" ), block.load( "b" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle add =
                instanceMethod( handle.newInstance(), "add", clazz, clazz );

        // then
        return (T) add.invoke( lhs, rhs );
    }

    @SuppressWarnings( "unchecked" )
    private <T> T subtractForType( Class<T> clazz, T lhs, T rhs ) throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock block = simple.generateMethod( clazz, "sub",
                    param( clazz, "a" ), param( clazz, "b" ) ) )
            {
                    block.returns( subtract( block.load( "a" ), block.load( "b" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle sub =
                instanceMethod( handle.newInstance(), "sub", clazz, clazz );

        // then
        return (T) sub.invoke( lhs, rhs );
    }

    @SuppressWarnings( "unchecked" )
    private <T> T multiplyForType( Class<T> clazz, T lhs, T rhs ) throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock block = simple.generateMethod( clazz, "multiply",
                    param( clazz, "a" ), param( clazz, "b" ) ) )
            {
                block.returns( multiply( block.load( "a" ), block.load( "b" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle sub =
                instanceMethod( handle.newInstance(), "multiply", clazz, clazz );

        // then
        return (T) sub.invoke( lhs, rhs );
    }

    private <T> boolean compareForType( Class<T> clazz, T lhs, T rhs,
            BiFunction<Expression,Expression,Expression> compare ) throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock block = simple.generateMethod( boolean.class, "compare",
                    param( clazz, "a" ), param( clazz, "b" ) ) )
            {
                block.returns( compare.apply( block.load( "a" ), block.load( "b" ) ) );
            }

            handle = simple.handle();
        }

        // when
        MethodHandle compareFcn =
                instanceMethod( handle.newInstance(), "compare", clazz, clazz );

        // then
        return (boolean) compareFcn.invoke( lhs, rhs );
    }

    public static class TernaryChecker
    {
        private boolean ranOnTrue = false;
        private boolean ranOnFalse = false;

        public String onTrue()
        {
            ranOnTrue = true;
            return "on true";
        }

        public String onFalse()
        {
            ranOnFalse = true;
            return "on false";
        }
    }

    public static class ResourceFactory
    {
        int open, close, inside;

        public AutoCloseable resource()
        {
            open++;
            return () -> close++;
        }

        public void inside()
        {
            inside++;
        }
    }

    @Test
    public void shouldGenerateTryCatch() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "catcher" ) ) )
            {
                run.tryCatch(
                        body -> body.expression( invoke( run.load( "body" ), RUN ) ),
                        handler -> handler.expression( invoke( run.load( "catcher" ), RUN ) ),
                        param( RuntimeException.class, "E" ) );
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock( Runnable.class ),
                failBody = mock( Runnable.class ),
                successCatch = mock( Runnable.class ), failCatch = mock( Runnable.class );
        RuntimeException theFailure = new RuntimeException();
        doThrow( theFailure ).when( failBody ).run();
        MethodHandle run = instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class );

        //success
        run.invoke( successBody, successCatch );
        verify( successBody ).run();
        verify( successCatch, never() ).run();

        //failure
        run.invoke( failBody, failCatch );
        InOrder orderFailure = inOrder( failBody, failCatch );
        orderFailure.verify( failBody ).run();
        orderFailure.verify( failCatch ).run();
    }

    @Test
    public void shouldGenerateTryCatchWithNestedBlock() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "catcher" ),
                    param( boolean.class, "test" ) ) )
            {

                run.tryCatch(
                        ( tryBlock ) -> {
                            try ( CodeBlock ifBlock = tryBlock.ifStatement( run.load( "test" ) ) )
                            {
                                ifBlock.expression( invoke( run.load( "body" ), RUN ) );
                            }
                        },
                        ( catchBlock ) -> catchBlock.expression( invoke( run.load( "catcher" ), RUN ) ),
                        param( RuntimeException.class, "E" )
                );
            }
            handle = simple.handle();
        }

        // when
        Runnable runnable = mock( Runnable.class );
        MethodHandle run = instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class, boolean.class );

        // then
        run.invoke( runnable, mock( Runnable.class ), false );
        verify( runnable, never() ).run();
        run.invoke( runnable, mock( Runnable.class ), true );
        verify( runnable ).run();
    }

    @Test
    public void shouldGenerateTryAndMultipleCatch() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "catcher1" ),
                    param( Runnable.class, "catcher2" ) ) )
            {

                run.tryCatch( ( tryBlock ) ->
                                tryBlock.tryCatch(
                                        ( innerTry ) -> innerTry.expression( invoke( run.load( "body" ), RUN ) ),
                                        ( catchBlock1 ) -> catchBlock1.expression( invoke( run.load( "catcher1" ),
                                                RUN ) ),
                                        param( MyFirstException.class, "E" ) ),
                        ( catchBlock2 ) -> catchBlock2.expression( invoke( run.load( "catcher2" ), RUN ) ),
                        param( MySecondException.class, "E" ) );

            }
            handle = simple.handle();
        }

        // when
        Runnable body1 = mock( Runnable.class ), body2 = mock( Runnable.class ),
                catcher11 = mock( Runnable.class ), catcher12 = mock( Runnable.class ),
                catcher21 = mock( Runnable.class ), catcher22 = mock( Runnable.class );
        doThrow( MyFirstException.class ).when( body1 ).run();
        doThrow( MySecondException.class ).when( body2 ).run();

        MethodHandle run =
                instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class, Runnable.class );

        run.invoke( body1, catcher11, catcher12 );
        verify( body1 ).run();
        verify( catcher11 ).run();
        verify( catcher12, never() ).run();

        run.invoke( body2, catcher21, catcher22 );
        verify( body2 ).run();
        verify( catcher22 ).run();
        verify( catcher21, never() ).run();
    }

    @Test
    public void shouldThrowException() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock thrower = simple.generateMethod( void.class, "thrower" ) )
            {
                thrower.throwException( invoke( newInstance( RuntimeException.class ),
                        constructorReference( RuntimeException.class, String.class ), constant( "hello world" ) ) );
            }
            handle = simple.handle();
        }

        // when
        try
        {
            instanceMethod( handle.newInstance(), "thrower" ).invoke();
            fail( "expected exception" );
        }
        // then
        catch ( RuntimeException exception )
        {
            assertEquals( "hello world", exception.getMessage() );
        }
    }

    @Test
    public void shouldBeAbleToCast() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( NamedBase.class, "SimpleClass" ) )
        {
            simple.field( String.class, "foo" );
            simple.generate( MethodTemplate.constructor( param( String.class, "name" ), param( Object.class, "foo" ) )
                    .invokeSuper( new ExpressionTemplate[]{load( "name", typeReference( String.class ) )},
                            new TypeReference[]{typeReference( String.class )} )
                    .put( self( simple.handle() ), String.class, "foo",
                            cast( String.class, load( "foo", typeReference( Object.class ) ) ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass(), String.class, Object.class ).invoke( "Pontus", "Tobias" );

        // then
        assertEquals( "SimpleClass", instance.getClass().getSimpleName() );
        assertThat( instance, instanceOf( NamedBase.class ) );
        assertEquals( "Pontus", ((NamedBase) instance).name );
        assertEquals( "Tobias", getField( instance, "foo" ) );
    }

    @Test
    public void shouldBeAbleToBox() throws Throwable
    {
        assertThat( boxTest( boolean.class, true ), equalTo( Boolean.TRUE ) );
        assertThat( boxTest( boolean.class, false ), equalTo( Boolean.FALSE ) );
        assertThat( boxTest( byte.class, (byte) 12 ), equalTo( (byte) 12 ) );
        assertThat( boxTest( short.class, (short) 12 ), equalTo( (short) 12 ) );
        assertThat( boxTest( int.class, 12 ), equalTo( 12 ) );
        assertThat( boxTest( long.class, 12L ), equalTo( 12L ) );
        assertThat( boxTest( float.class, 12F ), equalTo( 12F ) );
        assertThat( boxTest( double.class, 12D ), equalTo( 12D ) );
        assertThat( boxTest( char.class, 'a' ), equalTo( 'a' ) );
    }

    @Test
    public void shouldBeAbleToUnbox() throws Throwable
    {
        assertThat( unboxTest( Boolean.class, boolean.class, true ), equalTo( true ) );
        assertThat( unboxTest( Boolean.class, boolean.class, false ), equalTo( false ) );
        assertThat( unboxTest( Byte.class, byte.class, (byte) 12 ), equalTo( (byte) 12 ) );
        assertThat( unboxTest( Short.class, short.class, (short) 12 ), equalTo( (short) 12 ) );
        assertThat( unboxTest( Integer.class, int.class, 12 ), equalTo( 12 ) );
        assertThat( unboxTest( Long.class, long.class, 12L ), equalTo( 12L ) );
        assertThat( unboxTest( Float.class, float.class, 12F ), equalTo( 12F ) );
        assertThat( unboxTest( Double.class, double.class, 12D ), equalTo( 12D ) );
        assertThat( unboxTest( Character.class, char.class, 'a' ), equalTo( 'a' ) );
    }

    private <T> Object unboxTest(Class<T> boxedType, Class<?> unboxedType, T value)
            throws Throwable
    {
        createGenerator();
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock method = simple.generateMethod( unboxedType, "unbox",
                    param( boxedType, "test" ) ) )
            {
                method.returns( Expression.unbox( method.load( "test" ) ) );
            }

            handle = simple.handle();
        }

        // when
        return instanceMethod( handle.newInstance(), "unbox", boxedType ).invoke( value );
    }

    private <T> Object boxTest(Class<T> unboxedType, T value)
            throws Throwable
    {
        createGenerator();
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock method = simple.generateMethod( Object.class, "box",
                    param( unboxedType, "test" ) ) )
            {
                method.returns( Expression.box( method.load( "test" ) ) );
            }

            handle = simple.handle();
        }

        // when
        return instanceMethod( handle.newInstance(), "box", unboxedType ).invoke( value );
    }

    static MethodHandle method( Class<?> target, String name, Class<?>... parameters ) throws Exception
    {
        return MethodHandles.lookup().unreflect( target.getMethod( name, parameters ) );
    }

    static MethodHandle instanceMethod( Object instance, String name, Class<?>... parameters ) throws Exception
    {
        return method( instance.getClass(), name, parameters ).bindTo( instance );
    }

    static Object getField( Object instance, String field ) throws Exception
    {
        return instance.getClass().getField( field ).get( instance );
    }

    static MethodHandle constructor( Class<?> target, Class<?>... parameters ) throws Exception
    {
        return MethodHandles.lookup().unreflectConstructor( target.getConstructor( parameters ) );
    }

    private static final String PACKAGE = "org.neo4j.codegen.test";
    private CodeGenerator generator;

    ClassGenerator generateClass( String name, Class<?> firstInterface, Class<?>... more )
    {
        return generator.generateClass( PACKAGE, name, firstInterface, more );
    }

    ClassGenerator generateClass( Class<?> base, String name, Class<?>... interfaces )
    {
        return generator.generateClass( base, PACKAGE, name, interfaces );
    }

    ClassGenerator generateClass( String name, TypeReference... interfaces )
    {
        return generator.generateClass( PACKAGE, name, interfaces );
    }

    ClassGenerator generateClass( TypeReference base, String name, TypeReference... interfaces )
    {
        return generator.generateClass( base, PACKAGE, name, interfaces );
    }

    public static class NamedBase
    {
        final String name;
        private boolean defaultConstructorCalled = false;

        public NamedBase()
        {
            this.defaultConstructorCalled = true;
            this.name = null;
        }

        public NamedBase( String name )
        {
            this.name = name;
        }

        public boolean defaultConstructorCalled()
        {
            return defaultConstructorCalled;
        }
    }

    public static class SomeBean
    {
        private String foo;
        private String bar;

        public void setFoo( String foo )
        {
            this.foo = foo;
        }

        public void setBar( String bar )
        {
            this.bar = bar;
        }
    }

    private <T> void assertMethodReturningField( Class<T> clazz, T argument ) throws Throwable
    {
        // given
        createGenerator();
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            FieldReference value = simple.field( clazz, "value" );
            simple.generate( MethodTemplate.constructor( param( clazz, "value" ) )
                    .invokeSuper()
                    .put( self( simple.handle() ), value.type(), value.name(), load( "value", value.type() ) )
                    .build() );
            simple.generate( MethodTemplate.method( clazz, "value" )
                    .returns( ExpressionTemplate.get( self( simple.handle() ), clazz, "value" ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass(), clazz ).invoke( argument );

        // then
        assertEquals( argument, instanceMethod( instance, "value" ).invoke() );
    }

    private static MethodReference createMethod( Class<?> owner, Class<?> returnType, String name )
    {
        return methodReference( Runnable.class, void.class, "run" );
    }

    public static class MyFirstException extends RuntimeException
    {
    }

    public static class MySecondException extends RuntimeException
    {
    }

}
