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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.index.CorruptIndexException;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.defaultDirectoryStructure;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class LuceneSchemaIndexCorruptionTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final IndexProvider.Monitor monitor = new LoggingMonitor( logProvider.getLog( "test" ) );

    @Test
    public void shouldRequestIndexPopulationIfTheIndexIsCorrupt()
    {
        // Given
        long faultyIndexId = 1;
        CorruptIndexException error = new CorruptIndexException( "It's broken.", "" );

        LuceneIndexProvider provider = newFaultyIndexProvider( faultyIndexId, error );

        // When
        SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 1, 1 );
        InternalIndexState initialState = provider.getInitialState( faultyIndexId, descriptor );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( error ) );
    }

    @Test
    public void shouldRequestIndexPopulationFailingWithFileNotFoundException()
    {
        // Given
        long faultyIndexId = 1;
        FileNotFoundException error = new FileNotFoundException( "/some/path/somewhere" );

        LuceneIndexProvider provider = newFaultyIndexProvider( faultyIndexId, error );

        // When
        SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 1, 1 );
        InternalIndexState initialState = provider.getInitialState( faultyIndexId, descriptor );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( error ) );
    }

    @Test
    public void shouldRequestIndexPopulationWhenFailingWithEOFException()
    {
        // Given
        long faultyIndexId = 1;
        EOFException error = new EOFException( "/some/path/somewhere" );

        LuceneIndexProvider provider = newFaultyIndexProvider( faultyIndexId, error );

        // When
        SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 1, 1 );
        InternalIndexState initialState = provider.getInitialState( faultyIndexId, descriptor );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.POPULATING) );
        logProvider.assertAtLeastOnce( loggedException( error ) );
    }

    private LuceneIndexProvider newFaultyIndexProvider( long faultyIndexId, Exception error )
    {
        DirectoryFactory directoryFactory = mock( DirectoryFactory.class );
        File indexRootFolder = testDirectory.graphDbDir();
        AtomicReference<FaultyIndexStorageFactory> reference = new AtomicReference<>();
        return new LuceneIndexProvider( fs.get(), directoryFactory, defaultDirectoryStructure( indexRootFolder ), monitor,
                Config.defaults(), OperationalMode.single )
        {
            @Override
            protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
            {
                FaultyIndexStorageFactory storageFactory = new FaultyIndexStorageFactory( faultyIndexId, error,
                        directoryFactory, directoryStructure() );
                reference.set( storageFactory );
                return storageFactory;
            }
        };
    }

    private class FaultyIndexStorageFactory extends IndexStorageFactory
    {
        final long faultyIndexId;
        final Exception error;

        FaultyIndexStorageFactory( long faultyIndexId, Exception error, DirectoryFactory directoryFactory,
                IndexDirectoryStructure directoryStructure )
        {
            super( directoryFactory, fs.get(), directoryStructure );
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

    private static AssertableLogProvider.LogMatcher loggedException( Throwable exception )
    {
        return inLog( CoreMatchers.any( String.class ) )
                .error( CoreMatchers.any( String.class ), sameInstance( exception ) );
    }
}
