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
package org.neo4j.kernel.api.impl.schema.populator;

import org.junit.jupiter.api.Test;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProvider;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.NonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.DESCRIPTOR;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class NonUniqueLuceneIndexPopulatingUpdaterIT
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;
    private static final SchemaDescriptorSupplier SCHEMA_DESCRIPTOR = () -> SchemaDescriptors.forLabel( 1, 42 );
    private static final SchemaDescriptorSupplier COMPOSITE_SCHEMA_DESCRIPTOR = () -> SchemaDescriptors.forLabel( 1, 42, 43 );

    @Test
    void shouldSampleAdditions() throws Exception
    {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator( provider, SCHEMA_DESCRIPTOR );

        // When
        try ( var updater = populator.newPopulatingUpdater( mock( NodePropertyAccessor.class ), NULL ) )
        {
            updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
            updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
            updater.process( add( 3, SCHEMA_DESCRIPTOR, "baz" ) );
            updater.process( add( 4, SCHEMA_DESCRIPTOR, "bar" ) );
        }

        // Then
        assertThat( populator.sample( NULL ) ).isEqualTo( new IndexSample( 4, 3, 4 ) );
    }

    @Test
    void shouldSampleUpdates() throws Exception
    {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator( provider, SCHEMA_DESCRIPTOR );

        // When
        try ( var updater = populator.newPopulatingUpdater( mock( NodePropertyAccessor.class ), NULL ) )
        {
            updater.process( add( 1, SCHEMA_DESCRIPTOR, "initial1" ) );
            updater.process( add( 2, SCHEMA_DESCRIPTOR, "initial2" ) );
            updater.process( add( 3, SCHEMA_DESCRIPTOR, "new2" ) );
            updater.process( change( 1, SCHEMA_DESCRIPTOR, "initial1", "new1" ) );
            updater.process( change( 1, SCHEMA_DESCRIPTOR, "initial2", "new2" ) );
        }

        // Then samples calculated with documents pending merge
        assertThat( populator.sample( NULL ) ).isEqualTo( new IndexSample( 3, 4, 5 ) );
    }

    @Test
    void shouldSampleRemovals() throws Exception
    {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator( provider, SCHEMA_DESCRIPTOR );

        // When
        try ( var updater = populator.newPopulatingUpdater( mock( NodePropertyAccessor.class ), NULL ) )
        {
            updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
            updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
            updater.process( add( 3, SCHEMA_DESCRIPTOR, "baz" ) );
            updater.process( add( 4, SCHEMA_DESCRIPTOR, "qux" ) );
            updater.process( remove( 1, SCHEMA_DESCRIPTOR, "foo" ) );
            updater.process( remove( 2, SCHEMA_DESCRIPTOR, "bar" ) );
            updater.process( remove( 4, SCHEMA_DESCRIPTOR, "qux" ) );
        }

        // Then samples calculated with documents pending merge
        assertThat( populator.sample( NULL ) ).isEqualTo( new IndexSample( 1, 4, 4 ) );
    }

    @Test
    void shouldSampleCompositeIndex() throws Exception
    {
        // Given
        var provider = createIndexProvider();
        var populator = getPopulator( provider, COMPOSITE_SCHEMA_DESCRIPTOR );

        // When
        try ( var updater = populator.newPopulatingUpdater( mock( NodePropertyAccessor.class ), NULL ) )
        {
            updater.process( add( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
            updater.process( add( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
            updater.process( add( 3, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "baz" ) );
            updater.process( add( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "qux" ) );

            updater.process( remove( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
            updater.process( remove( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );

            updater.process( change( 3, COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "baz"}, new Object[]{"foo", "bar"} ) );
        }

        // Then samples calculated with documents pending merge
        assertThat( populator.sample( NULL ) ).isEqualTo( new IndexSample( 2, 5, 10 ) );
    }

    private IndexPopulator getPopulator( LuceneIndexProvider provider, SchemaDescriptorSupplier supplier ) throws Exception
    {
        var samplingConfig = new IndexSamplingConfig( Config.defaults() );
        var index = forSchema( supplier.schema(), DESCRIPTOR ).withName( "some_name" ).materialise( 1 );
        var bufferFactory = heapBufferFactory( (int) kibiBytes( 100 ) );
        var populator = provider.getPopulator( index, samplingConfig, bufferFactory, INSTANCE, mock( TokenNameLookup.class ) );
        populator.create();
        return populator;
    }

    private LuceneIndexProvider createIndexProvider()
    {
        var directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        var directoryStructureFactory = directoriesByProvider( testDir.homePath() );
        return new LuceneIndexProvider( fileSystem, directoryFactory, directoryStructureFactory, new Monitors(), Config.defaults(), writable() );
    }
}
