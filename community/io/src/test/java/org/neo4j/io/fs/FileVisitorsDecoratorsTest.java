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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.function.Function;

import org.neo4j.function.Predicates;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.function.ThrowingConsumer.noop;

@RunWith( Parameterized.class )
public class FileVisitorsDecoratorsTest
{
    @Parameterized.Parameter( 0 )
    public String name;

    @Parameterized.Parameter( 1 )
    public Function<FileVisitor<Path>, FileVisitor<Path>> decoratorConstrutor;

    @Parameterized.Parameter( 2 )
    public boolean throwsExceptions;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Object[]> formats()
    {
        return asList(
                new Object[]{"decorator",
                        (Function<FileVisitor<Path>, FileVisitor<Path>>) FileVisitors.Decorator::new,
                        false
                },
                new Object[]{"onFile",
                        (Function<FileVisitor<Path>, FileVisitor<Path>>) wrapped
                                -> FileVisitors.onFile( noop(), wrapped ),
                        false
                },
                new Object[]{"onDirectory",
                        (Function<FileVisitor<Path>, FileVisitor<Path>>) wrapped
                                -> FileVisitors.onDirectory( noop(), wrapped ),
                        false
                },
                new Object[]{"throwExceptions",
                        (Function<FileVisitor<Path>, FileVisitor<Path>>) FileVisitors::throwExceptions,
                        true
                },
                new Object[]{"onlyMatching", (Function<FileVisitor<Path>, FileVisitor<Path>>)
                        wrapped -> FileVisitors.onlyMatching( Predicates.alwaysTrue(), wrapped ),
                        false
                }
        );
    }

    @SuppressWarnings( "unchecked" )
    public FileVisitor<Path> wrapped = mock( FileVisitor.class );
    public FileVisitor<Path> decorator;

    @Before
    public void setup()
    {
        decorator = decoratorConstrutor.apply( wrapped );
    }

    @Test
    public void shouldDelegatePreVisitDirectory() throws IOException
    {
        Path dir = Paths.get( "some-dir" );
        BasicFileAttributes attrs = mock( BasicFileAttributes.class );
        decorator.preVisitDirectory( dir, attrs );
        verify( wrapped ).preVisitDirectory( dir, attrs );
    }

    @Test
    public void shouldPropagateReturnValueFromPreVisitDirectory() throws IOException
    {
        for ( FileVisitResult result : FileVisitResult.values() )
        {
            when( wrapped.preVisitDirectory( any(), any() ) ).thenReturn( result );
            assertThat( decorator.preVisitDirectory( null, null ), is( result ) );
        }
    }

    @Test
    public void shouldPropagateExceptionsFromPreVisitDirectory() throws IOException
    {
        when( wrapped.preVisitDirectory( any(), any() ) ).thenThrow( new IOException() );

        try
        {
            decorator.preVisitDirectory( null, null );
            fail( "expected exception" );
        }
        catch ( IOException ignored )
        {
        }
    }

    @Test
    public void shouldDelegatePostVisitDirectory() throws IOException
    {
        Path dir = Paths.get( "some-dir" );
        IOException e = throwsExceptions ? null : new IOException();
        decorator.postVisitDirectory( dir, e );
        verify( wrapped ).postVisitDirectory( dir, e );
    }

    @Test
    public void shouldPropagateReturnValueFromPostVisitDirectory() throws IOException
    {
        for ( FileVisitResult result : FileVisitResult.values() )
        {
            when( wrapped.postVisitDirectory( any(), any() ) ).thenReturn( result );
            assertThat( decorator.postVisitDirectory( null, null ), is( result ) );
        }
    }

    @Test
    public void shouldPropagateExceptionsFromPostVisitDirectory() throws IOException
    {
        when( wrapped.postVisitDirectory( any(), any() ) ).thenThrow( new IOException() );

        try
        {
            decorator.postVisitDirectory( null, null );
            fail( "expected exception" );
        }
        catch ( IOException ignored )
        {
        }
    }

    @Test
    public void shouldDelegateVisitFile() throws IOException
    {
        Path dir = Paths.get( "some-dir" );
        BasicFileAttributes attrs = mock( BasicFileAttributes.class );
        decorator.visitFile( dir, attrs );
        verify( wrapped ).visitFile( dir, attrs );
    }

    @Test
    public void shouldPropagateReturnValueFromVisitFile() throws IOException
    {
        for ( FileVisitResult result : FileVisitResult.values() )
        {
            when( wrapped.visitFile( any(), any() ) ).thenReturn( result );
            assertThat( decorator.visitFile( null, null ), is( result ) );
        }
    }

    @Test
    public void shouldPropagateExceptionsFromVisitFile() throws IOException
    {
        when( wrapped.visitFile( any(), any() ) ).thenThrow( new IOException() );

        try
        {
            decorator.visitFile( null, null );
            fail( "expected exception" );
        }
        catch ( IOException ignored )
        {
        }
    }

    @Test
    public void shouldDelegateVisitFileFailed() throws IOException
    {
        Path dir = Paths.get( "some-dir" );
        IOException e = throwsExceptions ? null : new IOException();
        decorator.visitFileFailed( dir, e );
        verify( wrapped ).visitFileFailed( dir, e );
    }

    @Test
    public void shouldPropagateReturnValueFromVisitFileFailed() throws IOException
    {
        for ( FileVisitResult result : FileVisitResult.values() )
        {
            when( wrapped.visitFileFailed( any(), any() ) ).thenReturn( result );
            assertThat( decorator.visitFileFailed( null, null ), is( result ) );
        }
    }

    @Test
    public void shouldPropagateExceptionsFromVisitFileFailed() throws IOException
    {
        when( wrapped.visitFileFailed( any(), any() ) ).thenThrow( new IOException() );

        try
        {
            decorator.visitFileFailed( null, null );
            fail( "expected exception" );
        }
        catch ( IOException ignored )
        {
        }
    }
}
