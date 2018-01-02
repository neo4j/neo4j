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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LuceneSchemaIndexProviderTest extends IndexProviderCompatibilityTestSuite
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected LuceneSchemaIndexProvider createIndexProvider()
    {
        return getLuceneSchemaIndexProvider( new Config(), new DirectoryFactory.InMemoryDirectoryFactory() );
    }

    @Test
    public void shouldFailToInvokePopulatorInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = new Config( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory() );
        expectedException.expect( UnsupportedOperationException.class );

        readOnlyIndexProvider.getPopulator( 1L, new IndexDescriptor( 1, 1 ), new IndexConfiguration( false ),
                new IndexSamplingConfig( readOnlyConfig ) );
    }

    @Test
    public void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = new Config( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                directoryFactory );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        expectedException.expect( UnsupportedOperationException.class );
        onlineAccessor.drop();
    }

    @Test
    public void indexCreationNotAllowedInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = new Config( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory() );

        expectedException.expect( IllegalStateException.class );
        getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory ) throws IOException
    {
        Config config = new Config();
        LuceneSchemaIndexProvider indexProvider = getLuceneSchemaIndexProvider( config, directoryFactory );
        IndexAccessor onlineAccessor = getIndexAccessor( config, indexProvider );
        onlineAccessor.flush();
        onlineAccessor.close();
    }

    private IndexAccessor getIndexAccessor( Config readOnlyConfig, LuceneSchemaIndexProvider indexProvider )
            throws IOException
    {
        return indexProvider.getOnlineAccessor( 1L, new IndexConfiguration( false ),
                new IndexSamplingConfig( readOnlyConfig ) );
    }

    private LuceneSchemaIndexProvider getLuceneSchemaIndexProvider( Config config, DirectoryFactory directoryFactory )
    {
        return new LuceneSchemaIndexProvider( fs, directoryFactory, graphDbDir, NullLogProvider.getInstance(), config, OperationalMode.single );
    }
}
