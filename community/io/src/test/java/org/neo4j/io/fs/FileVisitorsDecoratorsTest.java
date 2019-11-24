/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.fs;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingConsumer.noop;

class FileVisitorsDecoratorsTest
{
    @SuppressWarnings( "unchecked" )
    private final FileVisitor<Path> wrapped = mock( FileVisitor.class );

    static Stream<Arguments> parameters()
    {
        return Stream.of(
            of(
                "decorator",
                (DecoratorCtor) FileVisitors.Decorator::new,
                false
            ),
            of(
                "onFile",
                (DecoratorCtor) wrapped -> FileVisitors.onFile( noop(), wrapped ),
                false
            ),
            of(
                "onDirectory",
                (DecoratorCtor) wrapped -> FileVisitors.onDirectory( noop(), wrapped ),
                false
            ),
            of(
                "throwExceptions",
                (DecoratorCtor) FileVisitors::throwExceptions,
                true
            ),
            of(
                "onlyMatching",
                (DecoratorCtor) wrapped -> FileVisitors.onlyMatching( alwaysTrue(), wrapped ),
                false
            )
        );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldDelegatePreVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        var dir = Paths.get( "some-dir" );
        var attrs = mock( BasicFileAttributes.class );
        decorator.preVisitDirectory( dir, attrs );
        verify( wrapped ).preVisitDirectory( dir, attrs );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateReturnValueFromPreVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        for ( var result : FileVisitResult.values() )
        {
            when( wrapped.preVisitDirectory( any(), any() ) ).thenReturn( result );
            assertThat( decorator.preVisitDirectory( null, null ) ).isEqualTo( result );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateExceptionsFromPreVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        when( wrapped.preVisitDirectory( any(), any() ) ).thenThrow( new IOException( "test" ) );
        assertThrows( IOException.class, () -> decorator.preVisitDirectory( null, null ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldDelegatePostVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        var dir = Paths.get( "some-dir" );
        var e = throwsExceptions ? null : new IOException( "test" );
        decorator.postVisitDirectory( dir, e );
        verify( wrapped ).postVisitDirectory( dir, e );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateReturnValueFromPostVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        for ( var result : FileVisitResult.values() )
        {
            when( wrapped.postVisitDirectory( any(), any() ) ).thenReturn( result );
            assertThat( decorator.postVisitDirectory( null, null ) ).isEqualTo( result );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateExceptionsFromPostVisitDirectory( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        when( wrapped.postVisitDirectory( any(), any() ) ).thenThrow( new IOException( "test" ) );
        assertThrows( IOException.class, () -> decorator.postVisitDirectory( null, null ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldDelegateVisitFile( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        var dir = Paths.get( "some-dir" );
        var attrs = mock( BasicFileAttributes.class );
        decorator.visitFile( dir, attrs );
        verify( wrapped ).visitFile( dir, attrs );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateReturnValueFromVisitFile( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        for ( var result : FileVisitResult.values() )
        {
            when( wrapped.visitFile( any(), any() ) ).thenReturn( result );
            assertThat( decorator.visitFile( null, null ) ).isEqualTo( result );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateExceptionsFromVisitFile( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        when( wrapped.visitFile( any(), any() ) ).thenThrow( new IOException( "test" ) );
        assertThrows( IOException.class, () -> decorator.visitFile( null, null ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldDelegateVisitFileFailed( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        var dir = Paths.get( "some-dir" );
        var e = throwsExceptions ? null : new IOException( "test" );
        decorator.visitFileFailed( dir, e );
        verify( wrapped ).visitFileFailed( dir, e );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateReturnValueFromVisitFileFailed( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        for ( var result : FileVisitResult.values() )
        {
            when( wrapped.visitFileFailed( any(), any() ) ).thenReturn( result );
            assertThat( decorator.visitFileFailed( null, null ) ).isEqualTo( result );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldPropagateExceptionsFromVisitFileFailed( String name, DecoratorCtor decoratorConstructor, boolean throwsExceptions ) throws IOException
    {
        var decorator = decoratorConstructor.apply( wrapped );
        when( wrapped.visitFileFailed( any(), any() ) ).thenThrow( new IOException( "test" ) );

        assertThrows( IOException.class, () -> decorator.visitFileFailed( null, null ) );
    }

    interface DecoratorCtor extends Function<FileVisitor<Path>, FileVisitor<Path>>
    {
        // alias
    }
}
