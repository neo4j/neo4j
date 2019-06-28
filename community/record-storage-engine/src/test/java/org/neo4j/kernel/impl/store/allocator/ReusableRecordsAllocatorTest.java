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
package org.neo4j.kernel.impl.store.allocator;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReusableRecordsAllocatorTest
{
    @Test
    void allocatePreviouslyNotUsedRecord()
    {
        DynamicRecord dynamicRecord = new DynamicRecord( 1 );
        dynamicRecord.setInUse( false );

        ReusableRecordsAllocator recordsAllocator = new ReusableRecordsAllocator( 10, dynamicRecord );
        DynamicRecord allocatedRecord = recordsAllocator.nextRecord();

        assertSame( allocatedRecord, dynamicRecord, "Records should be the same." );
        assertTrue( allocatedRecord.inUse(), "Record should be marked as used." );
        assertTrue( allocatedRecord.isCreated(), "Record should be marked as created." );
    }

    @Test
    void allocatePreviouslyUsedRecord()
    {
        DynamicRecord dynamicRecord = new DynamicRecord( 1 );
        dynamicRecord.setInUse( true );

        ReusableRecordsAllocator recordsAllocator = new ReusableRecordsAllocator( 10, dynamicRecord );
        DynamicRecord allocatedRecord = recordsAllocator.nextRecord();

        assertSame( allocatedRecord, dynamicRecord, "Records should be the same." );
        assertTrue( allocatedRecord.inUse(), "Record should be marked as used." );
        assertFalse( allocatedRecord.isCreated(), "Record should be marked as created." );
    }

    @Test
    void trackRecordsAvailability()
    {
        DynamicRecord dynamicRecord1 = new DynamicRecord( 1 );
        DynamicRecord dynamicRecord2 = new DynamicRecord( 1 );

        ReusableRecordsAllocator recordsAllocator = new ReusableRecordsAllocator( 10, dynamicRecord1, dynamicRecord2 );
        assertSame( dynamicRecord1, recordsAllocator.nextRecord(), "Should be the same as first available record." );
        assertTrue( recordsAllocator.hasNext(), "Should have second record." );
        assertSame( dynamicRecord2, recordsAllocator.nextRecord(), "Should be the same as second available record." );
        assertFalse( recordsAllocator.hasNext(), "Should be out of available records" );
    }
}
