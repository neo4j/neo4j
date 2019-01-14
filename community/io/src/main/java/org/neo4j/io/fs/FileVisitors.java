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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Predicate;

import org.neo4j.function.ThrowingConsumer;

public class FileVisitors
{
    private FileVisitors()
    {
    }

    public static FileVisitor<Path> onlyMatching( Predicate<Path> predicate, FileVisitor<Path> wrapped )
    {
        return new FileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) throws IOException
            {
                return predicate.test( dir ) ? wrapped.preVisitDirectory( dir, attrs ) : FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
            {
                return predicate.test( file ) ? wrapped.visitFile( file, attrs ) : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed( Path file, IOException e ) throws IOException
            {
                return predicate.test( file ) ? wrapped.visitFileFailed( file, e ) : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException e ) throws IOException
            {
                return predicate.test( dir ) ? wrapped.postVisitDirectory( dir, e ) : FileVisitResult.CONTINUE;
            }
        };
    }

    public static FileVisitor<Path> throwExceptions( FileVisitor<Path> wrapped )
    {
        return new Decorator<Path>( wrapped )
        {
            @Override
            public FileVisitResult visitFileFailed( Path file, IOException e ) throws IOException
            {
                if ( e != null )
                {
                    throw e;
                }
                return super.visitFileFailed( file, null );
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException e ) throws IOException
            {
                if ( e != null )
                {
                    throw e;
                }
                return super.postVisitDirectory( dir, null );
            }
        };
    }

    public static FileVisitor<Path> onDirectory( ThrowingConsumer<Path, IOException> operation,
                                                 FileVisitor<Path> wrapped )
    {
        return new Decorator<Path>( wrapped )
        {
            @Override
            public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) throws IOException
            {
                operation.accept( dir );
                return super.preVisitDirectory( dir, attrs );
            }
        };
    }

    public static FileVisitor<Path> onFile( ThrowingConsumer<Path, IOException> operation, FileVisitor<Path> wrapped )
    {
        return new Decorator<Path>( wrapped )
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
            {
                operation.accept( file );
                return super.visitFile( file, attrs );
            }
        };
    }

    public static FileVisitor<Path> justContinue()
    {
        return new FileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs )
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed( Path file, IOException e )
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException e )
            {
                return FileVisitResult.CONTINUE;
            }
        };
    }

    public static class Decorator<T> implements FileVisitor<T>
    {
        private final FileVisitor<T> wrapped;

        public Decorator( FileVisitor<T> wrapped )
        {
            this.wrapped = wrapped;
        }

        @Override
        public FileVisitResult preVisitDirectory( T t, BasicFileAttributes attrs ) throws IOException
        {
            return wrapped.preVisitDirectory( t, attrs );
        }

        @Override
        public FileVisitResult visitFile( T t, BasicFileAttributes attrs ) throws IOException
        {
            return wrapped.visitFile( t, attrs );
        }

        @Override
        public FileVisitResult visitFileFailed( T t, IOException e ) throws IOException
        {
            return wrapped.visitFileFailed( t, e );
        }

        @Override
        public FileVisitResult postVisitDirectory( T t, IOException e ) throws IOException
        {
            return wrapped.postVisitDirectory( t, e );
        }
    }
}
