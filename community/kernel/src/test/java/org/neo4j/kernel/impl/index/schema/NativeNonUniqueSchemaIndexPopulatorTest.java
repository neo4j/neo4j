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

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.index.schema.LayoutTestUtil.countUniqueValues;

public abstract class NativeNonUniqueSchemaIndexPopulatorTest<KEY extends NativeSchemaKey<KEY>,VALUE extends NativeSchemaValue>
        extends NativeSchemaIndexPopulatorTest<KEY,VALUE>
{
    @Test
    public void addShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesWithDuplicateValues();

        // when
        populator.add( Arrays.asList( updates ) );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesWithDuplicateValues();
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            // when
            for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void shouldSampleUpdatesIfConfiguredForOnlineSampling() throws Exception
    {
        // GIVEN
        populator.create();
        IndexEntryUpdate<SchemaIndexDescriptor>[] scanUpdates = layoutUtil.someUpdates();
        populator.add( Arrays.asList( scanUpdates ) );
        Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> generator = layoutUtil.randomUpdateGenerator( random );
        Object[] updates = new Object[5];
        updates[0] = generator.next().values()[0].asObject();
        updates[1] = generator.next().values()[0].asObject();
        updates[2] = updates[1];
        updates[3] = generator.next().values()[0].asObject();
        updates[4] = updates[3];
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            long nodeId = 1000;
            for ( Object value : updates )
            {
                IndexEntryUpdate<SchemaIndexDescriptor> update = layoutUtil.add( nodeId++, Values.of( value ) );
                updater.process( update );
                populator.includeSample( update );
            }
        }

        // WHEN
        IndexSample sample = populator.sampleResult();

        // THEN
        assertEquals( updates.length, sample.sampleSize() );
        assertEquals( countUniqueValues( updates ), sample.uniqueValues() );
        assertEquals( updates.length, sample.indexSize() );
        populator.close( true );
    }
}
