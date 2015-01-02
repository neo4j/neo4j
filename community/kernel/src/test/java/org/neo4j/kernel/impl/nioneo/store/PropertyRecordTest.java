/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class PropertyRecordTest
{
    @Test
    public void addingDuplicatePropertyBlockShouldOverwriteExisting()
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
        List<PropertyBlock> propertyBlocks = record.getPropertyBlocks();
        assertThat( propertyBlocks, hasItem( blockB ));
        assertThat( propertyBlocks, hasSize( 1 ) );
    }
}
