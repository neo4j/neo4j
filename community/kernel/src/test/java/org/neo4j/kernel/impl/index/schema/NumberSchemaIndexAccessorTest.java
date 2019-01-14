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

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

public abstract class NumberSchemaIndexAccessorTest extends NativeSchemaIndexAccessorTest<NumberSchemaKey,NativeSchemaValue>
{
    @Override
    NumberSchemaIndexAccessor makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new NumberSchemaIndexAccessor( pageCache, fs, getIndexFile(), layout, immediate(), monitor,
                schemaIndexDescriptor, indexId, samplingConfig );
    }

    @Test
    public void respectIndexOrder() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdates = layoutUtil.someUpdates();
        processAll( someUpdates );
        Value[] expectedValues = layoutUtil.extractValuesFromUpdates( someUpdates );

        // when
        IndexReader reader = accessor.newReader();
        IndexQuery.RangePredicate<?> supportedQuery =
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true );

        for ( IndexOrder supportedOrder : NumberIndexProvider.CAPABILITY.orderCapability( ValueCategory.NUMBER ) )
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
            assertEquals( "found all values", i, expectedValues.length );
        }
    }

    // <READER ordering>

    @Test
    public void throwForUnsupportedIndexOrder() throws Exception
    {
        // given
        // Unsupported index order for query
        IndexReader reader = accessor.newReader();
        IndexOrder unsupportedOrder = IndexOrder.DESCENDING;
        IndexQuery.ExactPredicate unsupportedQuery = IndexQuery.exact( 0, "Legolas" );

        // then
        expected.expect( UnsupportedOperationException.class );
        expected.expectMessage( CoreMatchers.allOf(
                CoreMatchers.containsString( "unsupported order" ),
                CoreMatchers.containsString( unsupportedOrder.toString() ),
                CoreMatchers.containsString( unsupportedQuery.toString() ) ) );

        // when
        reader.query( new SimpleNodeValueClient(), unsupportedOrder, unsupportedQuery );
    }

    // </READER ordering>
}
