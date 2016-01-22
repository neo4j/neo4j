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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Rule;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneSchemaIndexCorruptionTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldMarkIndexAsFailedIfIndexIsCorrupt() throws Exception
    {
        // Given
        long faultyIndexId = 1;
        CorruptIndexException error = new CorruptIndexException( "It's broken.", "" );

        LuceneSchemaIndexProvider provider = newFaultySchemaIndexProvider( faultyIndexId, error );

        // When
        InternalIndexState initialState = provider.getInitialState( faultyIndexId );

        // Then
        assertEquals( InternalIndexState.FAILED, initialState );
    }

    @Test
    public void shouldMarkAsFailedAndReturnCorrectFailureMessageWhenFailingWithFileNotFoundException() throws Exception
    {
        // Given
        long faultyIndexId = 1;
        FileNotFoundException error = new FileNotFoundException( "/some/path/somewhere" );

        LuceneSchemaIndexProvider provider = newFaultySchemaIndexProvider( faultyIndexId, error );

        // When
        InternalIndexState initialState = provider.getInitialState( faultyIndexId );

        // Then
        assertEquals( InternalIndexState.FAILED, initialState );
        assertEquals( "File not found: " + error.getMessage(), provider.getPopulationFailure( faultyIndexId ) );
    }

    @Test
    public void shouldMarkAsFailedAndReturnCorrectFailureMessageWhenFailingWithEOFException() throws Exception
    {
        // Given
        long faultyIndexId = 1;
        EOFException error = new EOFException( "/some/path/somewhere" );

        LuceneSchemaIndexProvider provider = newFaultySchemaIndexProvider( faultyIndexId, error );

        // When
        InternalIndexState initialState = provider.getInitialState( faultyIndexId );

        // Then
        assertEquals( InternalIndexState.FAILED, initialState );
        assertEquals( "EOF encountered: " + error.getMessage(), provider.getPopulationFailure( faultyIndexId ) );
    }

    @Test
    public void shouldDenyFailureForNonFailedIndex() throws Exception
    {
        // Given
        long faultyIndexId = 1;
        long validIndexId = 2;
        EOFException error = new EOFException( "/some/path/somewhere" );

        LuceneSchemaIndexProvider provider = newFaultySchemaIndexProvider( faultyIndexId, error );

        // When
        InternalIndexState initialState = provider.getInitialState( faultyIndexId );

        // Then
        assertEquals( InternalIndexState.FAILED, initialState );
        try
        {
            provider.getPopulationFailure( validIndexId );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    private LuceneSchemaIndexProvider newFaultySchemaIndexProvider( long faultyIndexId, Exception error )
    {
        FaultyIndexStorageFactory storageFactory = new FaultyIndexStorageFactory( faultyIndexId, error );
        return new LuceneSchemaIndexProvider( storageFactory );
    }

    private class FaultyIndexStorageFactory extends IndexStorageFactory
    {
        final long faultyIndexId;
        final Exception error;

        FaultyIndexStorageFactory( long faultyIndexId, Exception error )
        {
            super( mock( DirectoryFactory.class ), fs.get(), testDirectory.graphDbDir() );
            this.faultyIndexId = faultyIndexId;
            this.error = error;
        }

        @Override
        public PartitionedIndexStorage indexStorageOf( long indexId )
        {
            return indexId == faultyIndexId ? newFaultyPartitionedIndexStorage() : super.indexStorageOf( indexId );
        }

        PartitionedIndexStorage newFaultyPartitionedIndexStorage()
        {
            try
            {
                PartitionedIndexStorage storage = mock( PartitionedIndexStorage.class );
                when( storage.listFolders() ).thenReturn( singletonList( new File( "/some/path/somewhere/1" ) ) );
                when( storage.openDirectory( any() ) ).thenThrow( error );
                return storage;
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
