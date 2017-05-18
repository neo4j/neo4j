/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.internal.dragons;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class AllocationRecordsTest
{

    @Test
    public void addRecordToEmptyList() throws Exception
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        UnsafeUtil.AllocationRecord allocationRecord = newRecord( 1, 100 );
        allocationRecords.add( allocationRecord );

        assertEquals( "We should have 1 record in the list of allocations.", 1, allocationRecords.size() );
        assertSame( "We should see our record in the beginning of the list.", allocationRecord, allocationRecords.get( 0 ) );
    }

    @Test
    public void addRecordsToTheEndOfTheList()
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        allocationRecords.add( newRecord( 1, 2 ) );
        allocationRecords.add( newRecord( 5, 1 ) );
        allocationRecords.add( newRecord( 15, 10 ) );
        allocationRecords.add( newRecord( 100, 10 ) );
        allocationRecords.add( newRecord( 217, 10 ) );
        allocationRecords.add( newRecord( 428, 10 ) );

        assertEquals( "List should contain 6 allocation records.", 6, allocationRecords.size() );
        assertEquals( "Records should be ordered by address.", 1, allocationRecords.get( 0 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 5, allocationRecords.get( 1 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 15, allocationRecords.get( 2 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 100, allocationRecords.get( 3 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 217, allocationRecords.get( 4 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 428, allocationRecords.get( 5 ).getAddress() );
    }

    @Test
    public void addRecordsToTheAllocationList()
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        allocationRecords.add( newRecord( 10, 5 ) );
        allocationRecords.add( newRecord( 100, 10 ) );
        allocationRecords.add( newRecord( 17, 1 ) );
        allocationRecords.add( newRecord( 7, 1 ) );
        allocationRecords.add( newRecord( 58, 1 ) );
        allocationRecords.add( newRecord( 18, 1 ) );
        allocationRecords.add( newRecord( 5, 1 ) );
        allocationRecords.add( newRecord( 1, 1 ) );
        allocationRecords.add( newRecord( 89, 1 ) );
        allocationRecords.add( newRecord( 67, 1 ) );

        assertEquals( "List should contain 10 records.", 10, allocationRecords.size() );
        assertEquals( "Records should be ordered by address.", 1, allocationRecords.get( 0 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 5, allocationRecords.get( 1 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 7, allocationRecords.get( 2 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 10, allocationRecords.get( 3 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 17, allocationRecords.get( 4 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 18, allocationRecords.get( 5 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 58, allocationRecords.get( 6 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 67, allocationRecords.get( 7 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 89, allocationRecords.get( 8 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 100, allocationRecords.get( 9 ).getAddress() );
    }

    @Test
    public void floorRecord() throws Exception
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        assertNull( allocationRecords.floorRecord( 100 ) );
        allocationRecords.add( newRecord( 10, 1 ) );
        allocationRecords.add( newRecord( 25, 1 ) );
        allocationRecords.add( newRecord( 40, 1 ) );
        allocationRecords.add( newRecord( 50, 1 ) );

        assertNull( "Floor record address should be closest lower or equal allocation.", allocationRecords.floorRecord( 5 ) );
        assertEquals("Floor record address should be closest lower or equal allocation.", 10,  allocationRecords.floorRecord( 10 ).getAddress() );
        assertEquals("Floor record address should be closest lower or equal allocation.", 25,  allocationRecords.floorRecord( 26 ).getAddress() );
        assertEquals("Floor record address should be closest lower or equal allocation.", 25,  allocationRecords.floorRecord( 30 ).getAddress() );
        assertEquals("Floor record address should be closest lower or equal allocation.", 40,  allocationRecords.floorRecord( 47 ).getAddress() );
        assertEquals("Floor record address should be closest lower or equal allocation.", 50,  allocationRecords.floorRecord( 50 ).getAddress() );
        assertEquals("Floor record address should be closest lower or equal allocation.", 50,  allocationRecords.floorRecord( 100 ).getAddress() );
    }

    @Test
    public void ceilRecord() throws Exception
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        assertNull( allocationRecords.ceilingRecord( 100 ) );
        allocationRecords.add( newRecord( 10, 1 ) );
        allocationRecords.add( newRecord( 25, 1 ) );
        allocationRecords.add( newRecord( 40, 1 ) );
        allocationRecords.add( newRecord( 67, 1 ) );
        allocationRecords.add( newRecord( 150, 1 ) );
        allocationRecords.add( newRecord( 250, 1 ) );

        assertNull( "Ceil record address should be closest higher or equal allocation.", allocationRecords.ceilingRecord( 300 ) );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 10,  allocationRecords.ceilingRecord( 10 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 40,  allocationRecords.ceilingRecord( 26 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 40,  allocationRecords.ceilingRecord( 30 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 67,  allocationRecords.ceilingRecord( 47 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 150,  allocationRecords.ceilingRecord( 120 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 250,  allocationRecords.ceilingRecord( 204 ).getAddress() );
        assertEquals("Ceil record address should be closest higher or equal allocation.", 250,  allocationRecords.ceilingRecord( 250 ).getAddress() );
    }

    @Test
    public void removeAllocationRecord()
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();

        allocationRecords.add( newRecord( 10, 1 ) );
        allocationRecords.add( newRecord( 100, 2 ) );
        allocationRecords.add( newRecord( 17, 3 ) );
        allocationRecords.add( newRecord( 7, 4 ) );
        allocationRecords.add( newRecord( 58, 5 ) );
        allocationRecords.add( newRecord( 18, 6 ) );
        allocationRecords.add( newRecord( 5, 7 ) );
        allocationRecords.add( newRecord( 1, 8 ) );
        allocationRecords.add( newRecord( 89, 9 ) );
        allocationRecords.add( newRecord( 67, 10 ) );

        assertEquals( 10, allocationRecords.size() );

        assertEquals( 4, allocationRecords.remove( 7 ).getSizeInBytes() );
        assertEquals( 9, allocationRecords.size() );
        assertEquals( "Records should be ordered by address.", 1, allocationRecords.get( 0 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 5, allocationRecords.get( 1 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 10, allocationRecords.get( 2 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 17, allocationRecords.get( 3 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 18, allocationRecords.get( 4 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 58, allocationRecords.get( 5 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 67, allocationRecords.get( 6 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 89, allocationRecords.get( 7 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 100, allocationRecords.get( 8 ).getAddress() );

        assertEquals( 8, allocationRecords.remove( 1 ).getSizeInBytes() );
        assertEquals( 8, allocationRecords.size() );
        assertEquals( "Records should be ordered by address.", 5, allocationRecords.get( 0 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 10, allocationRecords.get( 1 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 17, allocationRecords.get( 2 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 18, allocationRecords.get( 3 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 58, allocationRecords.get( 4 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 67, allocationRecords.get( 5 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 89, allocationRecords.get( 6 ).getAddress() );
        assertEquals( "Records should be ordered by address.", 100, allocationRecords.get( 7 ).getAddress() );
    }

    @Test
    public void removeNonExistentRecord()
    {
        UnsafeUtil.AllocationRecords allocationRecords = createAllocationRecords();
        assertNull( "Removal of non existent record should not fail and return null.", allocationRecords.remove( 110 ));
    }

    private UnsafeUtil.AllocationRecord newRecord( long address, int size )
    {
        return new UnsafeUtil.AllocationRecord( address, size );
    }

    private UnsafeUtil.AllocationRecords createAllocationRecords()
    {
        return new UnsafeUtil.AllocationRecords();
    }
}
