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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.Value;

public class DefaultIndexReaderTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void defaultQueryImplementationMustThrowForUnsupportedIndexOrder() throws Exception
    {
        // Given
        IndexReader indexReader = stubIndexReader();

        // Then
        expectedException.expect( UnsupportedOperationException.class );
        String expectedMessage = String.format( "This reader only have support for index order %s. Provided index order was %s.",
                IndexOrder.NONE, IndexOrder.ASCENDING );
        expectedException.expectMessage( Matchers.containsString( expectedMessage ) );
        indexReader.query( new SimpleNodeValueClient(), IndexOrder.ASCENDING, IndexQuery.exists( 1 ) );
    }

    private IndexReader stubIndexReader()
    {
        return new AbstractIndexReader( null )
        {
            @Override
            public long countIndexedNodes( long nodeId, Value... propertyValues )
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
            public void distinctValues( IndexProgressor.NodeValueClient client, PropertyAccessor propertyAccessor )
            {
            }

            @Override
            public void close()
            {
            }
        };
    }
}
