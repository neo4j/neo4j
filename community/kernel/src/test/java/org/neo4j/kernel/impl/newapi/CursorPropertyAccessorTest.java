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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.internal.kernel.api.helpers.StubRead;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class CursorPropertyAccessorTest
{
    @Test
    public void shouldLookupProperty() throws EntityNotFoundException
    {
        // given
        long nodeId = 10;
        Value value = Values.of( "abc" );
        int propertyKeyId = 0;
        StubNodeCursor nodeCursor = new StubNodeCursor().withNode( nodeId, new long[]{}, genericMap( 999, Values.of( 12345 ), propertyKeyId, value ) );
        CursorPropertyAccessor accessor = new CursorPropertyAccessor( nodeCursor, new StubPropertyCursor(), new StubRead() );

        // when
        Value readValue = accessor.getPropertyValue( nodeId, propertyKeyId );

        // then
        assertEquals( value, readValue );
    }

    @Test
    public void shouldReturnNoValueOnMissingProperty() throws EntityNotFoundException
    {
        // given
        long nodeId = 10;
        StubNodeCursor nodeCursor = new StubNodeCursor().withNode( nodeId, new long[]{}, genericMap( 999, Values.of( 12345 ) ) );
        CursorPropertyAccessor accessor = new CursorPropertyAccessor( nodeCursor, new StubPropertyCursor(), new StubRead() );

        // when
        Value readValue = accessor.getPropertyValue( nodeId, 0 );

        // then
        assertEquals( NO_VALUE, readValue );
    }

    @Test
    public void shouldThrowOnEntityNotFound()
    {
        // given
        long nodeId = 10;
        Value value = Values.of( "abc" );
        int propertyKeyId = 0;
        StubNodeCursor nodeCursor = new StubNodeCursor().withNode( nodeId, new long[]{}, genericMap( 999, Values.of( 12345 ), propertyKeyId, value ) );
        CursorPropertyAccessor accessor = new CursorPropertyAccessor( nodeCursor, new StubPropertyCursor(), new StubRead() );

        // when
        try
        {
            accessor.getPropertyValue( nodeId + 1, propertyKeyId );
            fail();
        }
        catch ( EntityNotFoundException e )
        {
            // then good
        }
    }
}
