/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.defaultDirectoryStructure;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class LuceneIndexProviderTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;

    private File graphDbDir;
    private final TokenNameLookup tokenNameLookup = simpleNameLookup;
    private static final StoreIndexDescriptor descriptor = forSchema( forLabel( 1, 1 ), PROVIDER_DESCRIPTOR ).withId( 1 );

    @BeforeEach
    void setup()
    {
        graphDbDir = testDir.databaseDir();
    }

    @Test
    void shouldFailToInvokePopulatorInReadOnlyMode()
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
        LuceneIndexProvider readOnlyIndexProvider =
                getLuceneIndexProvider( readOnlyConfig, new DirectoryFactory.InMemoryDirectoryFactory(), fileSystem, graphDbDir );
        assertThrows( UnsupportedOperationException.class,
                () -> readOnlyIndexProvider.getPopulator( descriptor, new IndexSamplingConfig( readOnlyConfig ), heapBufferFactory( 1024 ), tokenNameLookup ) );
    }

    @Test
    void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
        LuceneIndexProvider readOnlyIndexProvider = getLuceneIndexProvider( readOnlyConfig,
                directoryFactory, fileSystem, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        assertThrows( UnsupportedOperationException.class, onlineAccessor::drop );
    }

    @Test
    void indexUpdateNotAllowedInReadOnlyMode()
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
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
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
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
        return indexProvider.getOnlineAccessor( descriptor, new IndexSamplingConfig( readOnlyConfig ), tokenNameLookup );
    }

    private LuceneIndexProvider getLuceneIndexProvider( Config config, DirectoryFactory directoryFactory,
                                                        FileSystemAbstraction fs, File graphDbDir )
    {
        return new LuceneIndexProvider( fs, directoryFactory, defaultDirectoryStructure( graphDbDir ),
                IndexProvider.Monitor.EMPTY, config, OperationalMode.single );
    }
}
