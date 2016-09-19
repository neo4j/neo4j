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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.file.FileVisitor;
import java.nio.file.Path;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.stub;

import static org.neo4j.io.fs.FileVisitors.throwExceptions;

@RunWith( MockitoJUnitRunner.class )
public class ThrowExceptionsFileVisitorTest
{
    @Mock
    public FileVisitor<Path> wrapped;

    @Test
    public void shouldThrowExceptionFromVisitFileFailed() throws IOException
    {
        IOException exception = new IOException();
        stub( wrapped.visitFileFailed( any(), any() ) ).toThrow( exception );
        try
        {
            throwExceptions( wrapped ).visitFileFailed( null, exception );
            fail( "Expected exception" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( exception ) );
        }
    }

    @Test
    public void shouldThrowExceptionFromPostVisitDirectory() throws IOException
    {
        IOException exception = new IOException();
        stub( wrapped.postVisitDirectory( any(), any() ) ).toThrow( exception );
        try
        {
            throwExceptions( wrapped ).postVisitDirectory( null, exception );
            fail( "Expected exception" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( exception ) );
        }
    }
}
