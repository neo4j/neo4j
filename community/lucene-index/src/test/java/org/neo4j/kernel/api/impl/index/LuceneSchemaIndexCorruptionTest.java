/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.CorruptIndexException;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;

import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class LuceneSchemaIndexCorruptionTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldRequestIndexPopulationIfTheIndexIsCorrupt() throws Exception
    {
        // Given
        DirectoryFactory dirFactory = mock(DirectoryFactory.class);

        // This isn't quite correct, but it will trigger the correct code paths in our code
        CorruptIndexException toThrow = new CorruptIndexException( "It's borken." );
        when(dirFactory.open( any(File.class) )).thenThrow( toThrow );

        LuceneSchemaIndexProvider p = getLuceneSchemaIndexProvider( dirFactory );

        // When
        InternalIndexState initialState = p.getInitialState( 1l );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( toThrow ) );
    }

    @Test
    public void shouldRequestIndexPopulationFailingWithFileNotFoundException() throws Exception
    {
        // Given
        DirectoryFactory dirFactory = mock(DirectoryFactory.class);

        // This isn't quite correct, but it will trigger the correct code paths in our code
        FileNotFoundException toThrow = new FileNotFoundException( "/some/path/somewhere" );
        when(dirFactory.open( any(File.class) )).thenThrow( toThrow );

        LuceneSchemaIndexProvider p = getLuceneSchemaIndexProvider( dirFactory );

        // When
        InternalIndexState initialState = p.getInitialState( 1l );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( toThrow ) );
    }

    @Test
    public void shouldRequestIndexPopulationWhenFailingWithEOFException() throws Exception
    {
        // Given
        DirectoryFactory dirFactory = mock(DirectoryFactory.class);

        // This isn't quite correct, but it will trigger the correct code paths in our code
        EOFException toThrow = new EOFException( "/some/path/somewhere" );
        when(dirFactory.open( any(File.class) )).thenThrow( toThrow );

        LuceneSchemaIndexProvider p = getLuceneSchemaIndexProvider( dirFactory );

        // When
        InternalIndexState initialState = p.getInitialState( 1l );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( toThrow ) );
    }

    private LuceneSchemaIndexProvider getLuceneSchemaIndexProvider( DirectoryFactory dirFactory )
    {
        return new LuceneSchemaIndexProvider( fs.get(), dirFactory, testDirectory.graphDbDir(),
                logProvider, new Config(), OperationalMode.single );
    }

    private static AssertableLogProvider.LogMatcher loggedException( Throwable exception )
    {
        return inLog( CoreMatchers.any( String.class ) )
                .error( CoreMatchers.any( String.class ), sameInstance( exception ) );
    }
}
