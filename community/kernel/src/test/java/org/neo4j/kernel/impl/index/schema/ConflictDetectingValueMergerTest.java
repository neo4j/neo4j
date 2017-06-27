/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConflictDetectingValueMergerTest
{
    private final ConflictDetectingValueMerger<SchemaNumberKey,SchemaNumberValue> detector = new ConflictDetectingValueMerger<>();

    @Test
    public void shouldReportConflictOnSameValueAndDifferentEntityIds() throws Exception
    {
        // given
        Value value = Values.of( 123);
        long entityId1 = 10;
        long entityId2 = 20;

        // when
        SchemaNumberValue merged = detector.merge(
                key( entityId1, value ),
                key( entityId2, value ),
                SchemaNumberValue.INSTANCE,
                SchemaNumberValue.INSTANCE );

        // then
        assertNull( merged );
        assertTrue( detector.wasConflict() );
        assertEquals( entityId1, detector.existingNodeId() );
        assertEquals( entityId2, detector.addedNodeId() );
    }

    @Test
    public void shouldNotReportConflictOnSameValueSameEntityId() throws Exception
    {
        // given
        Value value = Values.of( 123);
        long entityId = 10;

        // when
        SchemaNumberValue merged = detector.merge(
                key( entityId, value ),
                key( entityId, value ),
                SchemaNumberValue.INSTANCE,
                SchemaNumberValue.INSTANCE );

        // then
        assertNull( merged );
        assertFalse( detector.wasConflict() );
    }

    private static SchemaNumberKey key( long entityId, Value... value )
    {
        SchemaNumberKey key = new SchemaNumberKey();
        key.from( entityId, value );
        return key;
    }
}
