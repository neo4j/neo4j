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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.UpdateMode;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.HEAP_ALLOCATOR;

@ExtendWith( {TestDirectoryExtension.class, RandomExtension.class} )
class IndexUpdateStorageTest
{
    private static final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
    private static final SchemaDescriptorSupplier descriptor = SchemaDescriptorFactory.forLabel( 1, 1 );

    @Inject
    protected TestDirectory directory;

    @Inject
    protected RandomRule random;

    private final GenericLayout layout = new GenericLayout( 1, spatialSettings );

    @Test
    void shouldAddZeroEntries() throws IOException
    {
        // given
        try ( IndexUpdateStorage<GenericKey,NativeIndexValue> storage = new IndexUpdateStorage<>( directory.getFileSystem(), directory.file( "file" ),
                HEAP_ALLOCATOR, 1000, layout
        ) )
        {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates( 0 );
            storeAll( storage, expected );

            // then
            verify( expected, storage );
        }
    }

    @Test
    void shouldAddFewEntries() throws IOException
    {
        // given
        try ( IndexUpdateStorage<GenericKey,NativeIndexValue> storage = new IndexUpdateStorage<>( directory.getFileSystem(), directory.file( "file" ),
                HEAP_ALLOCATOR, 1000, layout
        ) )
        {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates( 5 );
            storeAll( storage, expected );

            // then
            verify( expected, storage );
        }
    }

    @Test
    void shouldAddManyEntries() throws IOException
    {
        // given
        try ( IndexUpdateStorage<GenericKey,NativeIndexValue> storage = new IndexUpdateStorage<>( directory.getFileSystem(), directory.file( "file" ),
                HEAP_ALLOCATOR, 10_000, layout
        ) )
        {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates( 1_000 );
            storeAll( storage, expected );

            // then
            verify( expected, storage );
        }
    }

    private static void storeAll( IndexUpdateStorage<GenericKey,NativeIndexValue> storage, List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected )
            throws IOException
    {
        for ( IndexEntryUpdate<SchemaDescriptorSupplier> update : expected )
        {
            storage.add( update );
        }
        storage.doneAdding();
    }

    private static void verify( List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected, IndexUpdateStorage<GenericKey,NativeIndexValue> storage )
            throws IOException
    {
        try ( IndexUpdateCursor<GenericKey,NativeIndexValue> reader = storage.reader() )
        {
            for ( IndexEntryUpdate<SchemaDescriptorSupplier> expectedUpdate : expected )
            {
                assertTrue( reader.next() );
                assertEquals( expectedUpdate, asUpdate( reader ) );
            }
            assertFalse( reader.next() );
        }
    }

    private static IndexEntryUpdate<SchemaDescriptorSupplier> asUpdate( IndexUpdateCursor<GenericKey,NativeIndexValue> reader )
    {
        switch ( reader.updateMode() )
        {
        case ADDED:
            return IndexEntryUpdate.add( reader.key().getEntityId(), descriptor, reader.key().asValue() );
        case CHANGED:
            return IndexEntryUpdate.change( reader.key().getEntityId(), descriptor, reader.key().asValue(), reader.key2().asValue() );
        case REMOVED:
            return IndexEntryUpdate.remove( reader.key().getEntityId(), descriptor, reader.key().asValue() );
        default:
            throw new IllegalArgumentException();
        }
    }

    private List<IndexEntryUpdate<SchemaDescriptorSupplier>> generateSomeUpdates( int count )
    {
        List<IndexEntryUpdate<SchemaDescriptorSupplier>> updates = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            long entityId = random.nextLong( 10_000_000 );
            switch ( random.among( UpdateMode.MODES ) )
            {
            case ADDED:
                updates.add( IndexEntryUpdate.add( entityId, descriptor, random.nextValue() ) );
                break;
            case REMOVED:
                updates.add( IndexEntryUpdate.remove( entityId, descriptor, random.nextValue() ) );
                break;
            case CHANGED:
                updates.add( IndexEntryUpdate.change( entityId, descriptor, random.nextValue(), random.nextValue() ) );
                break;
            default:
                throw new IllegalArgumentException();
            }
        }
        return updates;
    }
}
