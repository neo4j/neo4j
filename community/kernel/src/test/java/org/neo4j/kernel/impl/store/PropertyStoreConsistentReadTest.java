/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opentest4j.TestAbortedException;

import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PropertyStoreConsistentReadTest extends RecordStoreConsistentReadTest<PropertyRecord, PropertyStore>
{
    @Override
    protected PropertyStore getStore( NeoStores neoStores )
    {
        return neoStores.getPropertyStore();
    }

    @Override
    protected PropertyRecord createNullRecord( long id )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setNextProp( 0 );
        record.setPrevProp( 0 );
        return record;
    }

    @Override
    protected PropertyRecord createExistingRecord( boolean light )
    {
        PropertyRecord record = new PropertyRecord( ID );
        record.setId( ID );
        record.setNextProp( 2 );
        record.setPrevProp( 4 );
        record.setInUse( true );
        PropertyBlock block = new PropertyBlock();
        DynamicRecordAllocator stringAllocator = new ReusableRecordsAllocator( 64, new DynamicRecord( 7 ) );
        Value value = Values.of( "a string too large to fit in the property block itself" );
        PropertyStore.encodeValue( block, 6, value, stringAllocator, null, true );
        if ( light )
        {
            block.getValueRecords().clear();
        }
        record.setPropertyBlock( block );
        return record;
    }

    @Override
    protected PropertyRecord getLight( long id, PropertyStore store )
    {
        throw new TestAbortedException( "Getting a light non-existing property record will throw." );
    }

    @Override
    protected PropertyRecord getHeavy( PropertyStore store, int id )
    {
        PropertyRecord record = super.getHeavy( store, id );
        ensureHeavy( store, record );
        return record;
    }

    private static void ensureHeavy( PropertyStore store, PropertyRecord record )
    {
        for ( PropertyBlock propertyBlock : record )
        {
            store.ensureHeavy( propertyBlock );
        }
    }

    @Override
    protected void assertRecordsEqual( PropertyRecord actualRecord, PropertyRecord expectedRecord )
    {
        assertNotNull( actualRecord, "actualRecord" );
        assertNotNull( expectedRecord, "expectedRecord" );
        assertThat( "getDeletedRecords", actualRecord.getDeletedRecords(), is( expectedRecord.getDeletedRecords() ) );
        assertThat( "getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
        assertThat( "getEntityId", actualRecord.getNodeId(), is( expectedRecord.getNodeId() ) );
        assertThat( "getPrevProp", actualRecord.getPrevProp(), is( expectedRecord.getPrevProp() ) );
        assertThat( "getRelId", actualRecord.getRelId(), is( expectedRecord.getRelId() ) );
        assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "getLongId", actualRecord.getId(), is( expectedRecord.getId() ) );

        List<PropertyBlock> actualBlocks = Iterables.asList( actualRecord );
        List<PropertyBlock> expectedBlocks = Iterables.asList( expectedRecord );
        assertThat( "getPropertyBlocks().size", actualBlocks.size(), is( expectedBlocks.size() ) );
        for ( int i = 0; i < actualBlocks.size(); i++ )
        {
            PropertyBlock actualBlock = actualBlocks.get( i );
            PropertyBlock expectedBlock = expectedBlocks.get( i );
            assertPropertyBlocksEqual( i, actualBlock, expectedBlock );
        }
    }

    private static void assertPropertyBlocksEqual( int index, PropertyBlock actualBlock, PropertyBlock expectedBlock )
    {
        assertThat( "[" + index + "]getKeyIndexId", actualBlock.getKeyIndexId(),
                is( expectedBlock.getKeyIndexId() ) );
        assertThat( "[" + index + "]getSingleValueBlock", actualBlock.getSingleValueBlock(),
                is( expectedBlock.getSingleValueBlock() ) );
        assertThat( "[" + index + "]getSingleValueByte", actualBlock.getSingleValueByte(),
                is( expectedBlock.getSingleValueByte() ) );
        assertThat( "[" + index + "]getSingleValueInt", actualBlock.getSingleValueInt(),
                is( expectedBlock.getSingleValueInt() ) );
        assertThat( "[" + index + "]getSingleValueLong", actualBlock.getSingleValueLong(),
                is( expectedBlock.getSingleValueLong() ) );
        assertThat( "[" + index + "]getSingleValueShort", actualBlock.getSingleValueShort(),
                is( expectedBlock.getSingleValueShort() ) );
        assertThat( "[" + index + "]getSize", actualBlock.getSize(), is( expectedBlock.getSize() ) );
        assertThat( "[" + index + "]getType", actualBlock.getType(), is( expectedBlock.getType() ) );
        assertThat( "[" + index + "]isLight", actualBlock.isLight(), is( expectedBlock.isLight() ) );

        List<DynamicRecord> actualValueRecords = actualBlock.getValueRecords();
        List<DynamicRecord> expectedValueRecords = expectedBlock.getValueRecords();
        assertThat( "[" + index + "]getValueRecords.size",
                actualValueRecords.size(), is( expectedValueRecords.size() ) );

        for ( int i = 0; i < actualValueRecords.size(); i++ )
        {
            DynamicRecord actualValueRecord = actualValueRecords.get( i );
            DynamicRecord expectedValueRecord = expectedValueRecords.get( i );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getData", actualValueRecord.getData(),
                    is( expectedValueRecord.getData() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getLength", actualValueRecord.getLength(),
                    is( expectedValueRecord.getLength() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getNextBlock", actualValueRecord.getNextBlock(),
                    is( expectedValueRecord.getNextBlock() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getType", actualValueRecord.getType(),
                    is( expectedValueRecord.getType() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getId", actualValueRecord.getId(),
                    is( expectedValueRecord.getId() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]getLongId", actualValueRecord.getId(),
                    is( expectedValueRecord.getId() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]isStartRecord", actualValueRecord.isStartRecord(),
                    is( expectedValueRecord.isStartRecord() ) );
            assertThat( "[" + index + "]getValueRecords[" + i + "]inUse", actualValueRecord.inUse(),
                    is( expectedValueRecord.inUse() ) );
        }
    }
}
