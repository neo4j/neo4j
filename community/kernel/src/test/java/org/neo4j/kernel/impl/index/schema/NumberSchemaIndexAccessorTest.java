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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;

public abstract class NumberSchemaIndexAccessorTest<KEY extends NumberSchemaKey, VALUE extends NativeSchemaValue>
        extends NativeSchemaIndexAccessorTest<KEY,VALUE>
{
    NativeSchemaIndexAccessor<KEY,VALUE> makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new NumberSchemaIndexAccessor<>( pageCache, fs, indexFile, layout, IMMEDIATE, monitor, indexDescriptor, indexId, samplingConfig );
    }

    @Test
    public void respectIndexOrder() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] someUpdates = layoutUtil.someUpdates();
        processAll( someUpdates );
        Value[] expectedValues = layoutUtil.extractValuesFromUpdates( someUpdates );

        // when
        IndexReader reader = accessor.newReader();
        IndexQuery.NumberRangePredicate supportedQuery =
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true );

        for ( IndexOrder supportedOrder : NumberIndexProvider.CAPABILITY.orderCapability( ValueGroup.NUMBER ) )
        {
            if ( supportedOrder == IndexOrder.ASCENDING )
            {
                Arrays.sort( expectedValues, Values.COMPARATOR );
            }
            if ( supportedOrder == IndexOrder.DESCENDING )
            {
                Arrays.sort( expectedValues, Values.COMPARATOR.reversed() );
            }

            SimpleNodeValueClient client = new SimpleNodeValueClient();
            reader.query( client, supportedOrder, supportedQuery );
            int i = 0;
            while ( client.next() )
            {
                assertEquals( "values in order", expectedValues[i++], client.values[0] );
            }
            assertTrue( "found all values", i == expectedValues.length );
        }
    }
}
