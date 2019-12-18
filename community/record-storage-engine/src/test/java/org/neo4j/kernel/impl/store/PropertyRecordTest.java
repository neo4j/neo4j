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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertyRecordTest
{
    @Test
    void addingDuplicatePropertyBlockShouldOverwriteExisting()
    {
        // Given these things...
        PropertyRecord record = new PropertyRecord( 1 );
        PropertyBlock blockA = new PropertyBlock();
        blockA.setValueBlocks( new long[1] );
        blockA.setKeyIndexId( 2 );
        PropertyBlock blockB = new PropertyBlock();
        blockB.setValueBlocks( new long[1] );
        blockB.setKeyIndexId( 2 ); // also 2, thus a duplicate

        // When we set the property block twice that have the same key
        record.setPropertyBlock( blockA );
        record.setPropertyBlock( blockB );

        // Then the record should only contain a single block, because blockB overwrote blockA
        List<PropertyBlock> propertyBlocks = Iterables.asList( record );
        assertThat( propertyBlocks ).contains( blockB );
        assertThat( propertyBlocks ).hasSize( 1 );
    }

    @Test
    void shouldIterateOverBlocks()
    {
        // GIVEN
        PropertyRecord record = new PropertyRecord( 0 );
        PropertyBlock[] blocks = new PropertyBlock[3];
        for ( int i = 0; i < blocks.length; i++ )
        {
            blocks[i] = new PropertyBlock();
            record.addPropertyBlock( blocks[i] );
        }

        // WHEN
        Iterator<PropertyBlock> iterator = record.iterator();

        // THEN
        for ( PropertyBlock block : blocks )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( block, iterator.next() );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldBeAbleToRemoveBlocksDuringIteration()
    {
        // GIVEN
        PropertyRecord record = new PropertyRecord( 0 );
        Set<PropertyBlock> blocks = new HashSet<>();
        for ( int i = 0; i < 4; i++ )
        {
            PropertyBlock block = new PropertyBlock();
            block.setValueBlocks( new long[]{i} );
            record.addPropertyBlock( block );
            blocks.add( block );
        }

        // WHEN
        Iterator<PropertyBlock> iterator = record.iterator();
        assertThrows( IllegalStateException.class, iterator::remove );

        // THEN
        int size = blocks.size();
        for ( int i = 0; i < size; i++ )
        {
            assertTrue( iterator.hasNext() );
            PropertyBlock block = iterator.next();
            if ( i % 2 == 1 )
            {
                iterator.remove();
                assertThrows( IllegalStateException.class, iterator::remove );
                blocks.remove( block );
            }
        }
        assertFalse( iterator.hasNext() );

        // and THEN there should only be the non-removed blocks left
        assertEquals( blocks, Iterables.asSet( record ) );
    }

    @Test
    void addLoadedBlock()
    {
        PropertyRecord record = new PropertyRecord( 42 );

        addBlock( record, 1, 2 );
        addBlock( record, 3, 4 );

        List<PropertyBlock> blocks = Iterables.asList( record );
        assertEquals( 2, blocks.size() );
        assertEquals( 1, blocks.get( 0 ).getKeyIndexId() );
        assertEquals( 2, blocks.get( 0 ).getSingleValueInt() );
        assertEquals( 3, blocks.get( 1 ).getKeyIndexId() );
        assertEquals( 4, blocks.get( 1 ).getSingleValueInt() );
    }

    @Test
    void addLoadedBlockFailsWhenTooManyBlocksAdded()
    {
        PropertyRecord record = new PropertyRecord( 42 );

        addBlock( record, 1, 2 );
        addBlock( record, 3, 4 );
        addBlock( record, 5, 6 );
        addBlock( record, 7, 8 );

        assertThrows( AssertionError.class, () -> addBlock( record, 9, 10 ) );
    }

    private static void addBlock( PropertyRecord record, int key, int value )
    {
        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, key, Values.of( value ), null, null, true );
        for ( long valueBlock : block.getValueBlocks() )
        {
            record.addLoadedBlock( valueBlock );
        }
    }
}
