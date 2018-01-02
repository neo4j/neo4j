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
package org.neo4j.kernel.impl.store;

import java.util.Iterator;

import org.neo4j.kernel.impl.store.DynamicBlockSize;
import org.neo4j.kernel.impl.store.ExistingThenNewRecordAllocator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExistingThenNewRecordAllocatorTest
{
    @Test
    public void shouldUseExistingRecordsThenAllocateNewOnes() throws Exception
    {
        // given
        IdSequence mock = mock( IdSequence.class );
        when( mock.nextId() ).thenReturn( 3L ).thenReturn( 4L );

        ExistingThenNewRecordAllocator allocator = new ExistingThenNewRecordAllocator(
                mock( DynamicBlockSize.class ), mock );
        Iterator<DynamicRecord> existing = asList( new DynamicRecord( 1 ), new DynamicRecord( 2 ) ).iterator();

        // when
        DynamicRecord record1 = allocator.nextUsedRecordOrNew( existing );
        DynamicRecord record2 = allocator.nextUsedRecordOrNew( existing );
        DynamicRecord record3 = allocator.nextUsedRecordOrNew( existing );
        DynamicRecord record4 = allocator.nextUsedRecordOrNew( existing );

        // then
        assertEquals( 1, record1.getId() );
        assertEquals( 2, record2.getId() );
        assertEquals( 3, record3.getId() );
        assertEquals( 4, record4.getId() );
    }
}
