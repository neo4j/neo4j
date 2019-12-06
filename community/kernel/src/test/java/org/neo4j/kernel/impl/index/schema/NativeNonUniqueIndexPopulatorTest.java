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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;

abstract class NativeNonUniqueIndexPopulatorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends NativeIndexPopulatorTests<KEY,VALUE>
{
    private final NativeIndexPopulatorTestCases.PopulatorFactory<KEY, VALUE> populatorFactory;
    private final ValueType[] typesOfGroup;
    private final IndexLayoutFactory<KEY, VALUE> indexLayoutFactory;

    NativeNonUniqueIndexPopulatorTest( NativeIndexPopulatorTestCases.PopulatorFactory<KEY, VALUE> populatorFactory, ValueType[] typesOfGroup,
        IndexLayoutFactory<KEY, VALUE> indexLayoutFactory )
    {
        this.populatorFactory = populatorFactory;
        this.typesOfGroup = typesOfGroup;
        this.indexLayoutFactory = indexLayoutFactory;
    }

    private static final IndexDescriptor nonUniqueDescriptor = TestIndexDescriptorFactory.forLabel( 42, 666 );

    private static Value[] asValues( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        Value[] values = new Value[updates.length];
        for ( int i = 0; i < updates.length; i++ )
        {
            values[i] = updates[i].values()[0];
        }
        return values;
    }

    @Override
    NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException
    {
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs ).withMonitor( monitor ).build();
        return populatorFactory.create( context, indexFiles, layout, indexDescriptor );
    }

    @Override
    ValueCreatorUtil<KEY,VALUE> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( nonUniqueDescriptor, typesOfGroup, ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<KEY,VALUE> createLayout()
    {
        return indexLayoutFactory.create();
    }

    @Test
    void addShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );

        // when
        populator.add( asList( updates ) );

        // then
        populator.scanCompleted( nullInstance, jobScheduler );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    void updaterShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }

        // then
        populator.scanCompleted( nullInstance, jobScheduler );
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    void shouldSampleUpdatesIfConfiguredForOnlineSampling() throws Exception
    {
        // GIVEN
        try
        {
            populator.create();
            IndexEntryUpdate<IndexDescriptor>[] scanUpdates = valueCreatorUtil.someUpdates( random );
            populator.add( asList( scanUpdates ) );
            Iterator<IndexEntryUpdate<IndexDescriptor>> generator = valueCreatorUtil.randomUpdateGenerator( random );
            Value[] updates = new Value[5];
            updates[0] = generator.next().values()[0];
            updates[1] = generator.next().values()[0];
            updates[2] = updates[1];
            updates[3] = generator.next().values()[0];
            updates[4] = updates[3];
            try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
            {
                long nodeId = 1000;
                for ( Value value : updates )
                {
                    IndexEntryUpdate<IndexDescriptor> update = valueCreatorUtil.add( nodeId++, value );
                    updater.process( update );
                }
            }

            // WHEN
            populator.scanCompleted( nullInstance, jobScheduler );
            IndexSample sample = populator.sampleResult();

            // THEN
            Value[] allValues = Arrays.copyOf( updates, updates.length + scanUpdates.length );
            System.arraycopy( asValues( scanUpdates ), 0, allValues, updates.length, scanUpdates.length );
            assertEquals( updates.length + scanUpdates.length, sample.sampleSize() );
            assertEquals( countUniqueValues( allValues ), sample.uniqueValues() );
            assertEquals( updates.length + scanUpdates.length, sample.indexSize() );
        }
        finally
        {
            populator.close( true );
        }
    }
}
