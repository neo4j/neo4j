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

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.ValueType;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.exception.ExceptionUtils.hasCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;

abstract class NativeUniqueIndexPopulatorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexPopulatorTests<KEY,VALUE>
{
    private static final IndexDescriptor uniqueDescriptor =
            IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 42, 666 ) ).withName( "constraint" ).materialise( 0 );

    private final NativeIndexPopulatorTestCases.PopulatorFactory<KEY, VALUE> populatorFactory;
    private final ValueType[] typesOfGroup;
    private final IndexLayoutFactory<KEY, VALUE> indexLayoutFactory;

    NativeUniqueIndexPopulatorTest( NativeIndexPopulatorTestCases.PopulatorFactory<KEY, VALUE> populatorFactory, ValueType[] typesOfGroup,
        IndexLayoutFactory<KEY, VALUE> indexLayoutFactory )
    {
        this.populatorFactory = populatorFactory;
        this.typesOfGroup = typesOfGroup;
        this.indexLayoutFactory = indexLayoutFactory;
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
        return new ValueCreatorUtil<>( uniqueDescriptor, typesOfGroup, ValueCreatorUtil.FRACTION_DUPLICATE_UNIQUE );
    }

    @Override
    IndexLayout<KEY,VALUE> createLayout()
    {
        return indexLayoutFactory.create();
    }

    @Test
    void addShouldThrowOnDuplicateValues()
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );

        assertThrows( IndexEntryConflictException.class, () ->
        {
            populator.add( asList( updates ) );
            populator.scanCompleted( nullInstance, jobScheduler );
        } );

        populator.close( true );
    }

    @Test
    void updaterShouldThrowOnDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues( random );
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );

        // when
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            updater.process( update );
        }
        var e = assertThrows( Exception.class, () ->
        {
            updater.close();
            populator.scanCompleted( nullInstance, jobScheduler );
        } );
        assertTrue( hasCause( e, IndexEntryConflictException.class ), e.getMessage() );

        populator.close( true );
    }

    @Test
    void shouldSampleUpdates() throws Exception
    {
        // GIVEN
        populator.create();
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdates( random );

        // WHEN
        populator.add( asList( updates ) );
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            populator.includeSample( update );
        }
        populator.scanCompleted( nullInstance, jobScheduler );
        IndexSample sample = populator.sampleResult();

        // THEN
        assertEquals( updates.length, sample.sampleSize() );
        assertEquals( updates.length, sample.uniqueValues() );
        assertEquals( updates.length, sample.indexSize() );
        populator.close( true );
    }
}
