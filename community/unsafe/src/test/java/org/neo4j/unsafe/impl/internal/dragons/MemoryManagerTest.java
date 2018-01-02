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
package org.neo4j.unsafe.impl.internal.dragons;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class MemoryManagerTest
{
    @Test
    public void allocatedPointerMustNotBeNull() throws Exception
    {
        MemoryManager mman = new MemoryManager( 16 * 4096, 8 );
        long address = mman.allocateAligned( 8192 );
        assertThat( address, is( not( 0L ) ) );
    }

    @Test
    public void allocatedPointerMustBePageAligned() throws Exception
    {
        MemoryManager mman = new MemoryManager( 16 * 4096, UnsafeUtil.pageSize() );
        long address = mman.allocateAligned( 8192 );
        assertThat( address % UnsafeUtil.pageSize(), is( 0L ) );
    }

    @Test
    public void mustBeAbleToAllocatePastMemoryLimit() throws Exception
    {
        MemoryManager mman = new MemoryManager( 8192, 2 );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1 ) % 2, is( 0L ) );
        }
        // Also asserts that no OutOfMemoryError is thrown.
    }
}
