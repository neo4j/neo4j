/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.List;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.nioneo.store.NodeStore.readOwnerFromDynamicLabelsRecord;

public class NodeStoreTest
{
    @Test
    public void shouldReadFirstFromSingleRecordDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = 12l;
        long[] ids = new long[] { expectedId, 23l, 42l };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord );
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    public void shouldReadFirstAsNullFromEmptyDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = null;
        long[] ids = new long[] { };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord );
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    public void shouldReadFirstFromTwoRecordDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = 12l;
        long[] ids = new long[] { expectedId, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord, new DynamicRecord( 1l ) );
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 8 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }
}
