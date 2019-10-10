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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.DESCRIPTOR;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@TestDirectoryExtension
class LuceneIndexProviderTest
{
    private static final IndexDescriptor descriptor = forSchema( forLabel( 1, 1 ), DESCRIPTOR ).withName( "index_1" ).materialise( 1 );

    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;
    private File graphDbDir;

    @BeforeEach
    void setup()
    {
        graphDbDir = testDir.homeDir();
    }

    @Test
    void shouldFailToInvokePopulatorInReadOnlyMode()
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, true );
        LuceneIndexProvider readOnlyIndexProvider =
                getLuceneIndexProvider( readOnlyConfig, new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );
        assertThrows( UnsupportedOperationException.class,
                () -> readOnlyIndexProvider.getPopulator( descriptor, new IndexSamplingConfig( readOnlyConfig ), heapBufferFactory( 1024 ) ) );
    }

    @Test
    void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                directoryFactory, fileSystem, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        assertThrows( UnsupportedOperationException.class, onlineAccessor::drop );
    }

    @Test
    void indexUpdateNotAllowedInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );

        assertThrows( UnsupportedOperationException.class,
                () -> getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).newUpdater( IndexUpdateMode.ONLINE ) );
    }

    @Test
    void indexForceMustBeAllowedInReadOnlyMode() throws Exception
    {
        // IndexAccessor.force is used in check-pointing, and must be allowed in read-only mode as it would otherwise
        // prevent backups from working.
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, true );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );

        // We assert that 'force' does not throw an exception
        getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).force( IOLimiter.UNLIMITED );
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory ) throws IOException
    {
        Config config = Config.defaults();
        LuceneIndexProvider indexProvider = getLuceneIndexProvider( config, directoryFactory, fileSystem,
                graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( config, indexProvider );
        onlineAccessor.close();
    }

    private IndexAccessor getIndexAccessor( Config readOnlyConfig, LuceneIndexProvider indexProvider )
            throws IOException
    {
        return indexProvider.getOnlineAccessor( descriptor, new IndexSamplingConfig( readOnlyConfig ) );
    }

    private LuceneIndexProvider getLuceneIndexProvider( Config config, DirectoryFactory directoryFactory,
                                                        FileSystemAbstraction fs, File graphDbDir )
    {
        return new LuceneIndexProvider( fs, directoryFactory, directoriesByProvider( graphDbDir ),
                IndexProvider.Monitor.EMPTY, config, true );
    }
}
