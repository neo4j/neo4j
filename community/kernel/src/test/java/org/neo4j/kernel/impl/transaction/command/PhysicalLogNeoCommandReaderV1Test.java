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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.store.PropertyType.STRING;

public class PhysicalLogNeoCommandReaderV1Test
{
    @Test
    public void shouldReadPropertyCommandWithDeletedDynamicRecords() throws Exception
    {
        // GIVEN
        PhysicalLogCommandReaderV2_1 reader = new PhysicalLogCommandReaderV2_1();
        InMemoryLogChannel data = new InMemoryLogChannel();
        CommandWriter writer = new CommandWriter( data );
        long id = 5;
        int keyId = 6;
        byte[] data1 = new byte[] {1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {6, 7, 8, 9, 10};
        long value = 1234;
        PropertyCommand command = new PropertyCommand();
        PropertyRecord property = new PropertyRecord( id );
        property.setInUse( true );
        property.addPropertyBlock( propertyBlockWithSomeDynamicRecords( keyId, STRING, value, data1, data2 ) );
        property.addDeletedRecord( dynamicRecord( false, null, STRING.intValue() ) );
        property.addDeletedRecord( dynamicRecord( false, null, STRING.intValue() ) );
        command.init( new PropertyRecord( id ), property );

        // WHEN
        writer.visitPropertyCommand( command );

        // THEN
        PropertyCommand readCommand = (PropertyCommand) reader.read( data );
        PropertyRecord readRecord = readCommand.getAfter();
        assertEquals( id, readRecord.getId() );
        PropertyBlock readBlock = single( (Iterable<PropertyBlock>) readRecord );
        assertArrayEquals( data1, readBlock.getValueRecords().get( 0 ).getData() );
        assertArrayEquals( data2, readBlock.getValueRecords().get( 1 ).getData() );
        assertEquals( 2, readRecord.getDeletedRecords().size() );
    }

    private long dynamicRecordId = 1;

    private DynamicRecord dynamicRecord( boolean inUse, byte[] data, int type )
    {
        DynamicRecord record = new DynamicRecord( dynamicRecordId++ );
        record.setInUse( inUse, type );
        if ( data != null )
        {
            record.setData( data );
            record.setLength( data.length );
        }
        return record;
    }

    private PropertyBlock propertyBlockWithSomeDynamicRecords( int keyId, PropertyType type, long value,
            byte[]... dynamicRecordData )
    {
        PropertyBlock block = new PropertyBlock();
        block.setSingleBlock( PropertyStore.singleBlockLongValue( keyId, type, value ) );
        for ( byte[] bytes : dynamicRecordData )
        {
            block.addValueRecord( dynamicRecord( true, bytes, type.intValue() ) );
        }
        return block;
    }
}
