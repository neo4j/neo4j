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
package org.neo4j.kernel.impl.util.collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator.MemoryBlock;
import org.neo4j.memory.MemoryAllocationTracker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CapacityLimitingBlockAllocatorDecoratorTest
{
    @Test
    void maxMemoryLimit()
    {
        final MemoryAllocationTracker tracker = mock( MemoryAllocationTracker.class );
        final OffHeapBlockAllocator allocator = mock( OffHeapBlockAllocator.class );
        when( allocator.allocate( anyLong(), any( MemoryAllocationTracker.class ) ) ).then( invocation ->
        {
            final long size = invocation.<Long>getArgument( 0 );
            return new MemoryBlock( 0, size, 0, size );
        } );
        final CapacityLimitingBlockAllocatorDecorator decorator = new CapacityLimitingBlockAllocatorDecorator( allocator, 1024 );

        final List<MemoryBlock> blocks = new ArrayList<>();
        for ( int i = 0; i < 8; i++ )
        {
            final MemoryBlock block = decorator.allocate( 128, tracker );
            blocks.add( block );
        }

        assertThrows( RuntimeException.class, () -> decorator.allocate( 128, tracker ) );

        decorator.free( blocks.remove( 0 ), tracker );
        assertDoesNotThrow( () -> decorator.allocate( 128, tracker ) );

        assertThrows( RuntimeException.class, () -> decorator.allocate( 256, tracker ) );
        decorator.free( blocks.remove( 0 ), tracker );
        assertThrows( RuntimeException.class, () -> decorator.allocate( 256, tracker ) );

        decorator.free( blocks.remove( 0 ), tracker );
        assertDoesNotThrow( () -> decorator.allocate( 256, tracker ) );
    }
}
