/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static org.neo4j.util.Bits.bitFlag;
import static org.neo4j.util.Bits.bitFlags;

class LogCommandSerializationV5_0 extends LogCommandSerializationV4_4
{
    static final LogCommandSerializationV5_0 INSTANCE = new LogCommandSerializationV5_0();

    @Override
    KernelVersion version()
    {
        return KernelVersion.V5_0;
    }

    @Override
    protected Command readPropertyKeyTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        var before = readPropertyKeyTokenRecord( id, channel );
        var after = readPropertyKeyTokenRecord( id, channel );

        return new Command.PropertyKeyTokenCommand( this, before, after );
    }

    private static PropertyKeyTokenRecord readPropertyKeyTokenRecord( int id, ReadableChannel channel ) throws IOException
    {
        // flags(byte)+propertyCount(int)+nameId(int)+nr_key_records(int)
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean createdInTx = bitFlag( flags, Record.CREATED_IN_TX );
        boolean internal = bitFlag( flags, Record.ADDITIONAL_FLAG_1 );

        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setInternal( internal );
        if ( createdInTx )
        {
            record.setCreated();
        }
        record.setPropertyCount( channel.getInt() );
        record.setNameId( channel.getInt() );
        readDynamicRecords( channel, record::addNameRecord );
        return record;
    }

    @Override
    public void writePropertyKeyTokenCommand( WritableChannel channel, Command.PropertyKeyTokenCommand command ) throws IOException
    {
        channel.put( NeoCommandType.PROP_INDEX_COMMAND );
        channel.putInt( command.getAfter().getIntId() );
        writePropertyKeyTokenRecord( channel, command.getBefore() );
        writePropertyKeyTokenRecord( channel, command.getAfter() );
    }

    private static void writePropertyKeyTokenRecord( WritableChannel channel, PropertyKeyTokenRecord record ) throws IOException
    {
        // flags(byte)+propertyCount(int)+nameId(int)+nr_key_records(int)
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                               bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                               bitFlag( record.isInternal(), Record.ADDITIONAL_FLAG_1 ) );
        channel.put( flags );
        channel.putInt( record.getPropertyCount() );
        channel.putInt( record.getNameId() );
        if ( record.isLight() )
        {
            channel.putInt( 0 );
        }
        else
        {
            writeDynamicRecords( channel, record.getNameRecords() );
        }
    }

    @Override
    protected Command readSchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        byte schemaRulePresence = channel.get();
        boolean hasSchemaRule = schemaRulePresence == SchemaRecord.COMMAND_HAS_SCHEMA_RULE;
        SchemaRecord before = readSchemaRecord( id, channel );
        SchemaRecord after = readSchemaRecord( id, channel );
        SchemaRule schemaRule = null;
        if ( hasSchemaRule )
        {
            schemaRule = readSchemaRule( id, channel );
        }
        return new Command.SchemaRuleCommand( this, before, after, schemaRule );
    }

    private static SchemaRecord readSchemaRecord( long id, ReadableChannel channel ) throws IOException
    {
        SchemaRecord schemaRecord = new SchemaRecord( id );
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean createdInTx = bitFlag( flags, Record.CREATED_IN_TX );
        schemaRecord.setInUse( inUse );
        if ( inUse )
        {
            byte schemaFlags = channel.get();
            schemaRecord.setConstraint( bitFlag( schemaFlags, SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT ) );
            schemaRecord.setNextProp( channel.getLong() );
        }
        if ( createdInTx )
        {
            schemaRecord.setCreated();
        }

        return schemaRecord;
    }

    @Override
    public void writeSchemaRuleCommand( WritableChannel channel, Command.SchemaRuleCommand command ) throws IOException
    {
        channel.put( NeoCommandType.SCHEMA_RULE_COMMAND );
        channel.putLong( command.getBefore().getId() );
        SchemaRule schemaRule = command.getSchemaRule();
        boolean hasSchemaRule = schemaRule != null;
        channel.put( hasSchemaRule ? SchemaRecord.COMMAND_HAS_SCHEMA_RULE : SchemaRecord.COMMAND_HAS_NO_SCHEMA_RULE );
        writeSchemaRecord( channel, command.getBefore() );
        writeSchemaRecord( channel, command.getAfter() );
        if ( hasSchemaRule )
        {
            writeSchemaRule( channel, schemaRule );
        }
    }

    private static void writeSchemaRecord( WritableChannel channel, SchemaRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                               bitFlag( record.isCreated(), Record.CREATED_IN_TX ) );
        channel.put( flags );
        if ( record.inUse() )
        {
            byte schemaFlags = bitFlag( record.isConstraint(), SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT );
            channel.put( schemaFlags );
            channel.putLong( record.getNextProp() );
        }
    }

    @Override
    protected Command readPropertyCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong(); // 8
        var before = readPropertyRecord( id, channel );
        var after = readPropertyRecord( id, channel );
        return new Command.PropertyCommand( this, before, after );
    }

    private static PropertyRecord readPropertyRecord( long id, ReadableChannel channel ) throws IOException
    {
        var record = new PropertyRecord( id );
        byte flags = channel.get(); // 1

        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean isCreated = bitFlag( flags, Record.CREATED_IN_TX );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean secondaryUnitCreated = bitFlag( flags, Record.SECONDARY_UNIT_CREATED_IN_TX );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );
        boolean nodeProperty = bitFlag( flags, Record.PROPERTY_OWNED_BY_NODE );
        boolean relProperty = bitFlag( flags, Record.PROPERTY_OWNED_BY_RELATIONSHIP );

        record.setInUse( inUse );
        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        record.setUseFixedReferences( usesFixedReferenceFormat );
        if ( isCreated )
        {
            record.setCreated();
        }

        long nextProp = channel.getLong(); // 8
        long prevProp = channel.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );

        long primitiveId = channel.getLong(); // 8
        setPropertyRecordOwner( record, nodeProperty, relProperty, primitiveId );

        if ( hasSecondaryUnit )
        {
            var secondaryUnitId = channel.getLong(); // 8
            record.setSecondaryUnitIdOnLoad( secondaryUnitId );
            record.setSecondaryUnitCreated( secondaryUnitCreated );
        }

        int nrPropBlocks = channel.get(); // 1
        assert nrPropBlocks >= 0;
        while ( nrPropBlocks-- > 0 )
        {
            var block = readPropertyBlock( channel );
            record.addPropertyBlock( block );
        }
        readDynamicRecords( channel, record::addDeletedRecord );
        if ( record.inUse() != ( record.numberOfProperties() > 0 ) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as " + inUse + " but the record is " + record );
        }
        return record;
    }

    private static void setPropertyRecordOwner( PropertyRecord record, boolean nodeProperty, boolean relProperty, long primitiveId )
    {
        assert !(nodeProperty && relProperty); // either node or rel or none of them
        if ( primitiveId != -1 ) // unused properties has no owner
        {
            if ( nodeProperty )
            {
                record.setNodeId( primitiveId );
            }
            else if ( relProperty )
            {
                record.setRelId( primitiveId );
            }
            else
            {
                record.setSchemaRuleId( primitiveId );
            }
        }
    }

    private static PropertyBlock readPropertyBlock( ReadableChannel channel ) throws IOException
    {
        var toReturn = new PropertyBlock();
        byte blockSize = channel.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize + " is not a valid block size value";
        // Read in blocks
        long[] blocks = readLongs( channel, blockSize / 8 );
        assert blocks.length == blockSize / 8 : blocks.length
                                                + " longs were read in while i asked for what corresponds to " + blockSize;

        assert PropertyType.getPropertyTypeOrThrow( blocks[0] ).calculateNumberOfBlocksUsed(
                blocks[0] ) == blocks.length : blocks.length + " is not a valid number of blocks for type "
                                               + PropertyType.getPropertyTypeOrThrow( blocks[0] );
        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks( blocks );
        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        readDynamicRecords( channel, toReturn::addValueRecord );
        return toReturn;
    }

    @Override
    public void writePropertyCommand( WritableChannel channel, Command.PropertyCommand command ) throws IOException
    {
        channel.put( NeoCommandType.PROP_COMMAND );
        channel.putLong( command.getAfter().getId() );
        writePropertyRecord( channel, command.getBefore() );
        writePropertyRecord( channel, command.getAfter() );
    }

    private static void writePropertyRecord( WritableChannel channel, PropertyRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                               bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                               bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                               bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                               bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ),
                               bitFlag( record.isSecondaryUnitCreated(), Record.SECONDARY_UNIT_CREATED_IN_TX ),
                               bitFlag( record.isNodeSet(), Record.PROPERTY_OWNED_BY_NODE ),
                               bitFlag( record.isRelSet(), Record.PROPERTY_OWNED_BY_RELATIONSHIP ) );

        channel.put( flags ); // 1
        channel.putLong( record.getNextProp() ).putLong( record.getPrevProp() ); // 8 + 8

        long entityId = record.getEntityId();
        channel.putLong( entityId ); // 8

        if ( record.hasSecondaryUnitId() )
        {
            channel.putLong( record.getSecondaryUnitId() );
        }
        int numberOfProperties = record.numberOfProperties();
        channel.put( (byte) numberOfProperties ); // 1
        PropertyBlock[] blocks = record.getPropertyBlocks();
        for ( int i = 0; i < numberOfProperties; i++ )
        {
            PropertyBlock block = blocks[i];
            assert block.getSize() > 0 : record + " has incorrect size";
            writePropertyBlock( channel, block );
        }
        writeDynamicRecords( channel, record.getDeletedRecords() );
    }

    private static void writePropertyBlock( WritableChannel channel, PropertyBlock block ) throws IOException
    {
        byte blockSize = (byte) block.getSize();
        assert blockSize > 0 : blockSize + " is not a valid block size value";
        channel.put( blockSize ); // 1
        long[] propBlockValues = block.getValueBlocks();
        for ( long propBlockValue : propBlockValues )
        {
            channel.putLong( propBlockValue );
        }
        /*
         * For each block we need to keep its dynamic record chain if
         * it is just created. Deleted dynamic records are in the property
         * record and dynamic records are never modified. Also, they are
         * assigned as a whole, so just checking the first should be enough.
         */
        if ( block.isLight() )
        {
            /*
             *  This has to be int. If this record is not light
             *  then we have the number of DynamicRecords that follow,
             *  which is an int. We do not currently want/have a flag bit so
             *  we simplify by putting an int here always
             */
            channel.putInt( 0 ); // 4 or
        }
        else
        {
            writeDynamicRecords( channel, block.getValueRecords() );
        }
    }

    private static void writeDynamicRecords( WritableChannel channel, List<DynamicRecord> records ) throws IOException
    {
        var size = records.size();
        channel.putInt( size ); // 4
        for ( int i = 0; i < size; i++ )
        {
            writeDynamicRecord( channel, records.get( i ) );
        }
    }

    private static void writeDynamicRecord( WritableChannel channel, DynamicRecord record ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            if ( record.isCreated() )
            {
                inUse |= Record.CREATED_IN_TX;
            }
            if ( record.isStartRecord() )
            {
                inUse |= Record.ADDITIONAL_FLAG_1;
            }
            channel.putLong( record.getId() )
                   .putInt( record.getTypeAsInt() )
                   .put( inUse )
                   .putInt( record.getLength() )
                   .putLong( record.getNextBlock() );
            byte[] data = record.getData();
            assert data != null;
            channel.put( data, data.length );
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            channel.putLong( record.getId() )
                   .putInt( record.getTypeAsInt() )
                   .put( inUse );
        }
    }

    private static DynamicRecord readDynamicRecord( ReadableChannel channel ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        long id = channel.getLong();
        assert id >= 0 && id <= (1L << 36) - 1 : id + " is not a valid dynamic record id";
        int type = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = (inUseFlag & Record.IN_USE.byteValue()) != 0;
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            record.setStartRecord( (inUseFlag & Record.ADDITIONAL_FLAG_1) != 0 );
            if ( (inUseFlag & Record.CREATED_IN_TX) != 0 )
            {
                record.setCreated();
            }
            int nrOfBytes = channel.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ((1 << 24) - 1) : nrOfBytes
                                                                   + " is not valid for a number of bytes field of " + "a dynamic record";
            long nextBlock = channel.getLong();
            assert (nextBlock >= 0 && nextBlock <= (1L << 36 - 1))
                   || (nextBlock == Record.NO_NEXT_BLOCK.intValue()) : nextBlock
                                                                       + " is not valid for a next record field of " + "a dynamic record";
            record.setNextBlock( nextBlock );
            byte[] data = new byte[nrOfBytes];
            channel.get( data, nrOfBytes );
            record.setData( data );
        }
        return record;
    }

    private static void readDynamicRecords( ReadableChannel channel, Consumer<DynamicRecord> recordConsumer )
            throws IOException
    {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            recordConsumer.accept( read );
            numberOfRecords--;
        }
    }

    private static long[] readLongs( ReadableChannel channel, int count ) throws IOException
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = channel.getLong();
        }
        return result;
    }
}
