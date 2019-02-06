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
package org.neo4j.storageengine.api.schema;

import org.junit.jupiter.api.Test;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultIndexReaderTest
{
    @Test
    void defaultQueryImplementationMustThrowForUnsupportedIndexOrder()
    {
        // Given
        IndexReader indexReader = stubIndexReader();

        // Then
        String expectedMessage = String.format( "This reader only have support for index order %s. Provided index order was %s.",
                IndexOrder.NONE, IndexOrder.ASCENDING );
        UnsupportedOperationException operationException = assertThrows( UnsupportedOperationException.class,
                () -> indexReader.query( new SimpleNodeValueClient(), IndexOrder.ASCENDING, false, IndexQuery.exists( 1 ) ) );
        assertEquals( expectedMessage, operationException.getMessage() );
    }

    private static IndexReader stubIndexReader()
    {
        return new AbstractIndexReader( null )
        {
            @Override
            public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
            {
                return 0;
            }

            @Override
            public IndexSampler createSampler()
            {
                return null;
            }

            @Override
            public PrimitiveLongResourceIterator query( IndexQuery... predicates )
            {
                return null;
            }

            @Override
            public boolean hasFullValuePrecision( IndexQuery... predicates )
            {
                return false;
            }

            @Override
            public void distinctValues( IndexProgressor.NodeValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
            {
            }

            @Override
            public void close()
            {
            }
        };
    }
}
