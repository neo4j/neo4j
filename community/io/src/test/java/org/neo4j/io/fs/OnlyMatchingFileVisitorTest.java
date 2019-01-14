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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.function.Predicates.alwaysFalse;
import static org.neo4j.io.fs.FileVisitors.onlyMatching;

@RunWith( MockitoJUnitRunner.class )
public class OnlyMatchingFileVisitorTest
{
    @Mock
    public FileVisitor<Path> wrapped;

    @Test
    public void shouldNotDelegatePreVisitDirectoryIfPredicateDoesntMatch() throws IOException
    {
        onlyMatching( alwaysFalse(), wrapped).preVisitDirectory( null, null );
        verify( wrapped, never() ).preVisitDirectory( any(), any() );
    }

    @Test
    public void shouldNotDelegatePostVisitDirectoryIfPredicateDoesntMatch() throws IOException
    {
        onlyMatching( alwaysFalse(), wrapped).postVisitDirectory( null, null );
        verify( wrapped, never() ).postVisitDirectory( any(), any() );
    }

    @Test
    public void shouldNotDelegateVisitFileIfPredicateDoesntMatch() throws IOException
    {
        onlyMatching( alwaysFalse(), wrapped).visitFile( null, null );
        verify( wrapped, never() ).visitFile( any(), any() );
    }

    @Test
    public void shouldNotDelegateVisitFileFailedIfPredicateDoesntMatch() throws IOException
    {
        onlyMatching( alwaysFalse(), wrapped).visitFileFailed( null, null );
        verify( wrapped, never() ).visitFileFailed( any(), any() );
    }

    @Test
    public void shouldNotSkipSubtreeFromPreVisitDirectoryIfPredicateDoesntMatch() throws IOException
    {
        assertThat( onlyMatching( alwaysFalse(), wrapped).preVisitDirectory( null, null ),
                is( FileVisitResult.SKIP_SUBTREE));
    }

    @Test
    public void shouldContinueAfterPostVisitDirectoryIfPredicateDoesntMatch() throws IOException
    {
        assertThat( onlyMatching( alwaysFalse(), wrapped).postVisitDirectory( null, null ),
                is( FileVisitResult.CONTINUE));
    }

    @Test
    public void shouldContinueAfterVisitFileIfPredicateDoesntMatch() throws IOException
    {
        assertThat( onlyMatching( alwaysFalse(), wrapped).visitFile( null, null ),
                is( FileVisitResult.CONTINUE));
    }

    @Test
    public void shouldContinueAfterVisitFileFailedIfPredicateDoesntMatch() throws IOException
    {
        assertThat( onlyMatching( alwaysFalse(), wrapped).visitFileFailed( null, null ),
                is( FileVisitResult.CONTINUE));
    }
}
