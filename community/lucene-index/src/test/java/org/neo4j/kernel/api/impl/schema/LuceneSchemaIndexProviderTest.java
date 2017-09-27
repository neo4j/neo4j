/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProvider.defaultDirectoryStructure;

/**
 * Additional tests for stuff not already covered by {@link LuceneSchemaIndexProviderCompatibilitySuiteTest}
 */
public class LuceneSchemaIndexProviderTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( getClass() );

    private File graphDbDir;
    private FileSystemAbstraction fs;
    private static final IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 1, 1 );

    @Before
    public void setup()
    {
        fs = fileSystemRule.get();
        graphDbDir = testDir.graphDbDir();
    }

    @Test
    public void shouldFailToInvokePopulatorInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fs, graphDbDir );
        expectedException.expect( UnsupportedOperationException.class );

        readOnlyIndexProvider.getPopulator( 1L, descriptor, new IndexSamplingConfig(
                readOnlyConfig ) );
    }

    @Test
    public void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                directoryFactory, fs, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        expectedException.expect( UnsupportedOperationException.class );
        onlineAccessor.drop();
    }

    @Test
    public void indexUpdateNotAllowedInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fs, graphDbDir );

        expectedException.expect( UnsupportedOperationException.class );
        getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).newUpdater( IndexUpdateMode.ONLINE);
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory ) throws IOException
    {
        Config config = Config.defaults();
        LuceneSchemaIndexProvider indexProvider = getLuceneSchemaIndexProvider( config, directoryFactory, fs,
                graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( config, indexProvider );
        onlineAccessor.close();
    }

    private IndexAccessor getIndexAccessor( Config readOnlyConfig, LuceneSchemaIndexProvider indexProvider )
            throws IOException
    {
        return indexProvider.getOnlineAccessor( 1L, descriptor, new IndexSamplingConfig( readOnlyConfig ) );
    }

    private LuceneSchemaIndexProvider getLuceneSchemaIndexProvider( Config config, DirectoryFactory directoryFactory,
                                                                    FileSystemAbstraction fs, File graphDbDir )
    {
        return new LuceneSchemaIndexProvider( fs, directoryFactory, defaultDirectoryStructure( graphDbDir ),
                SchemaIndexProvider.Monitor.EMPTY, config, OperationalMode.single );
    }
}
