/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.PopulationProgress.DONE;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.DESCRIPTOR;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.stringValue;

@TestDirectoryExtension
class LuceneIndexProviderTest
{
    private static final IndexDescriptor descriptor = forSchema( forLabel( 1, 1 ), DESCRIPTOR ).withName( "index_1" ).materialise( 1 );

    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;
    private Path graphDbDir;

    @BeforeEach
    void setup()
    {
        graphDbDir = testDir.homePath();
    }

    @Test
    void shouldFailToInvokePopulatorInReadOnlyMode()
    {
        var config = Config.defaults();
        LuceneIndexProvider readOnlyIndexProvider =
                getLuceneIndexProvider( config, new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );
        assertThrows( UnsupportedOperationException.class,
                () -> readOnlyIndexProvider.getPopulator( descriptor, new IndexSamplingConfig( config ),
                        heapBufferFactory( 1024 ), INSTANCE, SIMPLE_TOKEN_LOOKUP ) );
    }

    @Test
    void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = Config.defaults( read_only_database_default, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                directoryFactory, fileSystem, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        assertThrows( UnsupportedOperationException.class, onlineAccessor::drop );
    }

    @Test
    void indexUpdateNotAllowedInReadOnlyMode()
    {
        Config readOnlyConfig = Config.defaults( read_only_database_default, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );

        assertThrows( UnsupportedOperationException.class,
                () -> getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).newUpdater( IndexUpdateMode.ONLINE, NULL ) );
    }

    @Test
    void indexForceMustBeAllowedInReadOnlyMode() throws Exception
    {
        // IndexAccessor.force is used in check-pointing, and must be allowed in read-only mode as it would otherwise
        // prevent backups from working.
        Config readOnlyConfig = Config.defaults( read_only_database_default, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );

        // We assert that 'force' does not throw an exception
        getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).force( NULL );
    }

    @Test
    void shouldHandleConcurrentUpdates() throws Throwable
    {
        // Given an active lucene index populator
        var config = Config.defaults();
        var provider = createIndexProvider( config );
        var samplingConfig = new IndexSamplingConfig( config );
        var bufferFactory = heapBufferFactory( (int) kibiBytes( 100 ) );
        var populator = provider.getPopulator( descriptor, samplingConfig, bufferFactory, INSTANCE, mock( TokenNameLookup.class ) );
        var race = new Race();

        // And the underlying index files are created
        populator.create();

        // When multiple threads are populating the index
        race.addContestants(2, throwing( () -> {
            for ( int value = 0; value < 3000; value++ )
            {
                populator.add( List.of( add( value, descriptor, stringValue( String.valueOf( value ) ) ) ), NULL );
            }
        } ) );

        // And updated concurrently
        race.addContestant(throwing( () ->
        {
            try ( var updater = populator.newPopulatingUpdater( NodePropertyAccessor.EMPTY, NULL ) )
            {
                for ( int value = 0; value < 1000; value++ )
                {
                    updater.process( change( value, descriptor, stringValue( String.valueOf( value ) ), stringValue( String.valueOf( value ) ) ) );
                }
            }
        } ) );
        race.go();

        // Then the index population completes
        assertThat( populator.progress( DONE ).getCompleted() ).isEqualTo( 1 );
        var sample = populator.sample( NULL );
        assertThat( sample.sampleSize() ).isEqualTo( 7000 );
        assertThat( sample.uniqueValues() ).isEqualTo( 3000L );
        assertThat( sample.indexSize() ).isGreaterThanOrEqualTo( 5000L );
        populator.close( true, NULL );
    }

    private LuceneIndexProvider createIndexProvider( Config config )
    {
        var directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        var directoryStructureFactory = directoriesByProvider( testDir.homePath() );
        return new LuceneIndexProvider( fileSystem, directoryFactory, directoryStructureFactory, new Monitors(), config, writable() );
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory ) throws IOException
    {
        Config config = Config.defaults();
        LuceneIndexProvider indexProvider = getLuceneIndexProvider( config, directoryFactory, fileSystem, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( config, indexProvider );
        onlineAccessor.close();
    }

    private static IndexAccessor getIndexAccessor( Config readOnlyConfig, LuceneIndexProvider indexProvider )
            throws IOException
    {
        return indexProvider.getOnlineAccessor( descriptor, new IndexSamplingConfig( readOnlyConfig ), SIMPLE_TOKEN_LOOKUP );
    }

    private static LuceneIndexProvider getLuceneIndexProvider( Config config, DirectoryFactory directoryFactory,
            FileSystemAbstraction fs, Path graphDbDir )
    {
        return new LuceneIndexProvider( fs, directoryFactory, directoriesByProvider( graphDbDir ),
                new Monitors(), config, readOnly() );
    }
}
