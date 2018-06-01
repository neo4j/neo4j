/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.FileVisitor;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.fs.FileVisitors.throwExceptions;

@ExtendWith( MockitoExtension.class )
class ThrowExceptionsFileVisitorTest
{
    @Mock
    FileVisitor<Path> wrapped;

    @Test
    void shouldThrowExceptionFromVisitFileFailed()
    {
        IOException exception = new IOException();
        assertThrows( IOException.class, () -> throwExceptions( wrapped ).visitFileFailed( null, exception ) );
    }

    @Test
    void shouldThrowExceptionFromPostVisitDirectory()
    {
        IOException exception = new IOException();
        assertThrows(IOException.class, () -> throwExceptions( wrapped ).postVisitDirectory( null, exception ) );
    }
}
