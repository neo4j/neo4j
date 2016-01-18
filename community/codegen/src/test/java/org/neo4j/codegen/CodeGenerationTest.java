/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.codegen.Expression.constant;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.newArray;
import static org.neo4j.codegen.Expression.newInstance;
import static org.neo4j.codegen.Expression.not;
import static org.neo4j.codegen.Expression.or;
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

@RunWith(Parameterized.class)
public class CodeGenerationTest
{
    public static final MethodReference RUN = createMethod( Runnable.class, void.class, "run" );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> generators()
    {
        return Arrays.asList(new Object[] {SourceCode.SOURCECODE}, new Object[] {SourceCode.BYTECODE});
    }

    @Parameterized.Parameter( 0 )
    public CodeGenerationStrategy<Configuration> strategy;

    @Before
    public void createGenerator()
    {
        try
        {
            generator = CodeGenerator.generateCode( strategy, SourceCode.PRINT_SOURCE);
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
        try ( ClassGenerator simple = generateClass( NamedBase.class, "SimpleClass"  ) )
        {
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass()).invoke( );
        Object constructorCalled = instanceMethod( instance, "defaultConstructorCalled" ).invoke();

        // then
        assertTrue( (Boolean) constructorCalled );
    }

    @Test
    public void shouldGenerateCallToDefaultSuperConstructor() throws Throwable
    {
        // given
        ClassHandle handle;

        try ( ClassGenerator simple = generateClass( NamedBase.class, "SimpleClass" ) )
        {
            simple.field( String.class, "foo" );
            simple.generate( MethodTemplate.constructor( param( String.class, "name" ), param( String.class, "foo" ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass(), String.class, String.class ).invoke( "Pontus", "Tobias" );
        Object constructorCalled = instanceMethod( instance, "defaultConstructorCalled" ).invoke();

        // then
        assertTrue( (Boolean) constructorCalled );
    }

    @Test
    public void shouldNotGenerateCallToDefaultSuperConstructorIfSuperIsCalled() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( NamedBase.class, "SimpleClass" ) )
        {
            simple.field( String.class, "foo" );
            simple.generate( MethodTemplate.constructor( param( String.class, "name" ), param( String.class, "foo" ) )
                    .invokeSuper( new ExpressionTemplate[]{load( "name" )}, new TypeReference[]{typeReference(String.class)} )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass(), String.class, String.class ).invoke( "Pontus", "Tobias" );
        Object constructorCalled = instanceMethod( instance, "defaultConstructorCalled" ).invoke();

        // then
        assertFalse( (Boolean) constructorCalled );
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
                                    newArray( typeReference( String.class ), constant( "a" ), constant( "b" )) ) )
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
                get.returns( Expression.get( foo ) );
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
                get.returns( Expression.get( foo ) );
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
                get.returns( Expression.get( foo ) );
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
                                    methodReference( Iterator.class, Object.class, "next" ) )),
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
    public void shouldGenerateForEachLoop() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock callEach = simple.generateMethod( void.class, "callEach",
                    param( TypeReference.parameterizedType( Iterable.class, Runnable.class ), "targets" ) ) )
            {
                try ( CodeBlock loop = callEach.forEach( param(Runnable.class, "runner"), callEach.load( "targets" )) )
                {
                    loop.expression(
                            invoke(loop.load("runner"),
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
    public void shouldGenerateOr() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( void.class, "conditional",
                    param( boolean.class, "test1" ), param( boolean.class, "test2" ), param( Runnable.class, "runner" ) ) )
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
        MethodHandle conditional = instanceMethod( handle.newInstance(), "conditional", boolean.class,  boolean.class, Runnable.class );
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
                    param( boolean.class, "test1" ), param( boolean.class, "test2" )) )
            {
                conditional.returns( or( conditional.load( "test1" ), conditional.load( "test2" ) ) );
            }

            handle = simple.handle();
        }


        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class, boolean.class);

        // then
        assertThat(conditional.invoke( true, true), equalTo(true));
        assertThat(conditional.invoke( true, false), equalTo(true));
        assertThat(conditional.invoke( false, true), equalTo(true));
        assertThat(conditional.invoke( false, false), equalTo(false));
    }

    @Test
    public void shouldGenerateMethodUsingNot() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock conditional = simple.generateMethod( boolean.class, "conditional",
                    param( boolean.class, "test" )) )
            {
                conditional.returns( not( conditional.load( "test" ) ) );
            }

            handle = simple.handle();
        }


        // when
        MethodHandle conditional =
                instanceMethod( handle.newInstance(), "conditional", boolean.class);

        // then
        assertThat(conditional.invoke( true), equalTo(false));
        assertThat(conditional.invoke( false), equalTo(true));
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
    public void shouldGenerateTryWithResourceBlock() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock arm = simple.generate( MethodDeclaration.method( void.class, "arm",
                    param( ResourceFactory.class, "factory" ) ).throwsException( Exception.class ) ) )
            {
                try ( CodeBlock block = arm.tryBlock( AutoCloseable.class, "resource", invoke(
                        arm.load( "factory" ), methodReference(
                                ResourceFactory.class, AutoCloseable.class, "resource" ) ) ) )
                {
                    block.expression( invoke( block.load( "factory" ),
                            methodReference( ResourceFactory.class, void.class, "inside" ) ) );
                }
            }
            handle = simple.handle();
        }

        // when
        ResourceFactory factory = new ResourceFactory();
        MethodHandle arm = instanceMethod( handle.newInstance(), "arm", ResourceFactory.class );
        arm.invoke( factory );

        // then
        assertEquals( 1, factory.open );
        assertEquals( 1, factory.inside );
        assertEquals( 1, factory.close );
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
                try ( TryBlock tryBlock = run.tryBlock() )
                {
                    tryBlock.expression( invoke( run.load( "body" ), RUN ) );
                    try ( CodeBlock catchBlock = tryBlock.catchBlock(param(RuntimeException.class, "E")) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher" ), RUN ) );
                    }
                }
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
                try ( TryBlock tryBlock = run.tryBlock() )
                {
                    tryBlock.expression( invoke( run.load( "body" ), RUN ) );
                    try ( CodeBlock catchBlock = tryBlock.catchBlock( param( MyFirstException.class, "E" ) ) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher1" ), RUN ) );
                    }
                    try ( CodeBlock catchBlock = tryBlock.catchBlock( param( MySecondException.class, "E" ) ) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher2" ), RUN ) );
                    }
                }
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
    public void shouldGenerateTryFinally() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "finalize" ) ) )
            {
                try ( TryBlock tryBlock = run.tryBlock() )
                {
                    tryBlock.expression( invoke( run.load( "body" ), RUN ) );
                    try ( CodeBlock finallyBlock = tryBlock.finallyBlock() )
                    {
                        finallyBlock.expression( invoke( run.load( "finalize" ), RUN ) );
                    }
                }
            }
            handle = simple.handle();
        }

        // when
        Runnable body = mock( Runnable.class ), finalize = mock( Runnable.class );
        RuntimeException theFailure = new RuntimeException();
        doThrow( theFailure ).when( body ).run();
        MethodHandle run = instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class );
        try
        {
            run.invoke( body, finalize );
            fail( "expected exception" );
        }
        // then
        catch ( RuntimeException e )
        {
            assertSame( theFailure, e );
        }
        InOrder order = inOrder( body, finalize );
        order.verify( body ).run();
        order.verify( finalize ).run();
    }

    @Test
    public void shouldGenerateTryCatchFinally() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "catcher" ),
                    param( Runnable.class, "finalize" ) ) )
            {
                try ( TryBlock tryBlock = run.tryBlock() )
                {
                    tryBlock.expression( invoke( run.load( "body" ), RUN ) );
                    try ( CodeBlock catchBlock = tryBlock.catchBlock( param( RuntimeException.class, "E" ) ) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher" ), RUN ) );
                    }
                    try ( CodeBlock finallyBlock = tryBlock.finallyBlock() )
                    {
                        finallyBlock.expression( invoke( run.load( "finalize" ), RUN ) );
                    }
                }
            }
            handle = simple.handle();
        }

        // when
        Runnable successBody = mock( Runnable.class ), failBody = mock( Runnable.class ),
                successCatch = mock( Runnable.class ), failCatch = mock( Runnable.class ),
                successFinally = mock( Runnable.class ), failFinally = mock( Runnable.class );
        RuntimeException theFailure = new RuntimeException();
        doThrow( theFailure ).when( failBody ).run();
        MethodHandle run =
                instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class, Runnable.class );

        //success
        run.invoke( successBody, successCatch, successFinally );
        verify( successBody ).run();
        verify( successCatch, never() ).run();
        verify( successFinally ).run();

        //failure
        run.invoke( failBody, failCatch, failFinally );
        InOrder order = inOrder( failBody, failCatch, failFinally );
        order.verify( failBody ).run();
        order.verify( failCatch ).run();
        order.verify( failFinally ).run();
    }

    @Test
    public void shouldGenerateTryMultipleCatchAndFinally() throws Throwable
    {
        // given
        ClassHandle handle;
        try ( ClassGenerator simple = generateClass( "SimpleClass" ) )
        {
            try ( CodeBlock run = simple.generateMethod( void.class, "run",
                    param( Runnable.class, "body" ),
                    param( Runnable.class, "catcher1" ),
                    param( Runnable.class, "catcher2" ),
                    param( Runnable.class, "finalize" )
            ) )
            {
                try ( TryBlock tryBlock = run.tryBlock() )
                {
                    tryBlock.expression( invoke( run.load( "body" ), RUN ) );
                    try ( CodeBlock catchBlock = tryBlock.catchBlock( param( MyFirstException.class, "E" ) ) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher1" ), RUN ) );
                    }
                    try ( CodeBlock catchBlock = tryBlock.catchBlock( param( MySecondException.class, "E" ) ) )
                    {
                        catchBlock.expression( invoke( run.load( "catcher2" ), RUN ) );
                    }
                    try ( CodeBlock finallyBlock = tryBlock.finallyBlock() )
                    {
                        finallyBlock.expression( invoke( run.load( "finalize" ), RUN ) );
                    }
                }
            }
            handle = simple.handle();
        }

        // when

        Runnable body1 = mock( Runnable.class ), body2 = mock( Runnable.class ),
                catcher11 = mock( Runnable.class ), catcher12 = mock( Runnable.class ), finalize1 =
                mock( Runnable.class ),
                catcher21 = mock( Runnable.class ), catcher22 = mock( Runnable.class ), finalize2 =
                mock( Runnable.class );
        doThrow( MyFirstException.class ).when( body1 ).run();
        doThrow( MySecondException.class ).when( body2 ).run();

        MethodHandle run =
                instanceMethod( handle.newInstance(), "run", Runnable.class, Runnable.class, Runnable.class,
                        Runnable.class );


        run.invoke( body1, catcher11, catcher12, finalize1 );
        verify( body1 ).run();
        verify( catcher11 ).run();
        verify( catcher12, never() ).run();
        verify( finalize1 ).run();


        run.invoke( body2, catcher21, catcher22, finalize2 );
        verify( body2 ).run();
        verify( catcher22 ).run();
        verify( catcher21, never() ).run();
        verify( finalize2 ).run();
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
                    .invokeSuper( new ExpressionTemplate[]{load( "name" )}, new TypeReference[]{typeReference(String.class)} )
                    .put( self(), String.class, "foo", cast(String.class, load( "foo" )) )
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
            try ( CodeBlock ctor = simple.generateConstructor( param( clazz, "value" ) ) )
            {
                ctor.put( ctor.self(), value, ctor.load( "value" ) );
            }
            simple.generate( MethodTemplate.method( clazz, "value" )
                    .returns( ExpressionTemplate.get( self(), clazz, "value" ) )
                    .build() );
            handle = simple.handle();
        }

        // when
        Object instance = constructor( handle.loadClass(), clazz ).invoke( argument );

        // then
        assertEquals( argument, instanceMethod( instance, "value" ).invoke() );
    }

    private static MethodReference createMethod(Class<?> owner, Class<?> returnType, String name)
    {
        try
        {
            return methodReference( Runnable.class, void.class, "run" );
        }
        catch ( NoSuchMethodException e )
        {
            throw new AssertionError( "Cannot create method", e );
        }
    }

    public static class MyFirstException extends RuntimeException
    {
    }

    public static class MySecondException extends RuntimeException
    {
    }

}
