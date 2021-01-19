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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.helpers.Numbers.unsignedShortToInt;
import static org.neo4j.internal.recordstorage.CommandReading.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_DELETED_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_INDEX_DYNAMIC_RECORD_ADDER;
import static org.neo4j.util.Bits.bitFlag;

class LogCommandSerializationV4_0 extends LogCommandSerialization
{
    static final LogCommandSerializationV4_0 INSTANCE = new LogCommandSerializationV4_0();

    @Override
    protected Command readNodeCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        NodeRecord before = readNodeRecord( id, channel );
        NodeRecord after = readNodeRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        // DynamicRecord has the created flag stored inside them because it's much harder to tell by looking at the command whether or not they are created
        return new Command.NodeCommand( this, before, after );
    }

    @Override
    protected Command readRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();

        RelationshipRecord before = readRelationshipRecord( id, channel );
        RelationshipRecord after = readRelationshipRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        return new Command.RelationshipCommand( this, before, after );
    }

    @Override
    protected Command readPropertyCommand( ReadableChannel channel ) throws IOException
    {
        // ID
        long id = channel.getLong(); // 8
        // BEFORE
        PropertyRecord before = readPropertyRecord( id, channel );
        if ( before == null )
        {
            return null;
        }
        // AFTER
        PropertyRecord after = readPropertyRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        // DynamicRecord has the created flag stored inside them because it's much harder to tell by looking at the command whether or not they are created
        return new Command.PropertyCommand( this, before, after );
    }

    @Override
    protected Command readRelationshipGroupCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        RelationshipGroupRecord before = readRelationshipGroupRecord( id, channel );
        RelationshipGroupRecord after = readRelationshipGroupRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        return new Command.RelationshipGroupCommand( this, before, after );
    }

    private RelationshipGroupRecord readRelationshipGroupRecord( long id, ReadableChannel channel )
            throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        int type = unsignedShortToInt( channel.getShort() );
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        RelationshipGroupRecord record = new RelationshipGroupRecord( id ).initialize( inUse, type, firstOut, firstIn, firstLoop, owningNode, next );
        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        if ( hasSecondaryUnit )
        {
            record.setSecondaryUnitIdOnLoad( channel.getLong() );
        }
        record.setUseFixedReferences( usesFixedReferenceFormat );
        return record;
    }

    @Override
    protected Command readRelationshipTypeTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        RelationshipTypeTokenRecord before = readRelationshipTypeTokenRecord( id, channel );
        RelationshipTypeTokenRecord after = readRelationshipTypeTokenRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        // DynamicRecord has the created flag stored inside them because it's much harder to tell by looking at the command whether or not they are created
        return new Command.RelationshipTypeTokenCommand( this, before, after );
    }

    private RelationshipTypeTokenRecord readRelationshipTypeTokenRecord( int id, ReadableChannel channel )
            throws IOException
    {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte headerByte = channel.get();
        boolean inUse = false;
        boolean internal = false;
        if ( (headerByte & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
            internal = (headerByte & Record.ADDITIONAL_FLAG_1) == Record.ADDITIONAL_FLAG_1;
        }
        else if ( headerByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + headerByte );
        }
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        record.setInternal( internal );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            record.addNameRecord( dr );
        }
        return record;
    }

    @Override
    protected Command readLabelTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        LabelTokenRecord before = readLabelTokenRecord( id, channel );
        LabelTokenRecord after = readLabelTokenRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        // DynamicRecord has the created flag stored inside them because it's much harder to tell by looking at the command whether or not they are created
        return new Command.LabelTokenCommand( this, before, after );
    }

    private LabelTokenRecord readLabelTokenRecord( int id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte headerByte = channel.get();
        boolean inUse = false;
        boolean internal = false;
        if ( (headerByte & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
            internal = (headerByte & Record.ADDITIONAL_FLAG_1) == Record.ADDITIONAL_FLAG_1;
        }
        else if ( headerByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + headerByte );
        }
        LabelTokenRecord record = new LabelTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        record.setInternal( internal );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            record.addNameRecord( dr );
        }
        return record;
    }

    @Override
    protected Command readPropertyKeyTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        PropertyKeyTokenRecord before = readPropertyKeyTokenRecord( id, channel );
        if ( before == null )
        {
            return null;
        }

        PropertyKeyTokenRecord after = readPropertyKeyTokenRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        // DynamicRecord has the created flag stored inside them because it's much harder to tell by looking at the command whether or not they are created
        return new Command.PropertyKeyTokenCommand( this, before, after );
    }

    private PropertyKeyTokenRecord readPropertyKeyTokenRecord( int id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+count(int)+key_blockId(int)
        byte headerByte = channel.get();
        boolean inUse = false;
        boolean internal = false;
        if ( (headerByte & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
            internal = (headerByte & Record.ADDITIONAL_FLAG_1) == Record.ADDITIONAL_FLAG_1;
        }
        else if ( headerByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + headerByte );
        }
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( channel.getInt() );
        record.setNameId( channel.getInt() );
        record.setInternal( internal );
        if ( readDynamicRecords( channel, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        return record;
    }

    @Override
    protected Command readSchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        byte schemaRulePresence = channel.get();
        boolean hasSchemaRule = schemaRulePresence == SchemaRecord.COMMAND_HAS_SCHEMA_RULE;
        SchemaRecord before = readSchemaRecord( id, channel );
        SchemaRecord after = readSchemaRecord( id, channel );
        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );

        SchemaRule schemaRule = null;
        if ( hasSchemaRule )
        {
            schemaRule = readSchemaRule( id, channel );
        }
        return new Command.SchemaRuleCommand( this, before, after, schemaRule );
    }

    private SchemaRecord readSchemaRecord( long id, ReadableChannel channel ) throws IOException
    {
        SchemaRecord schemaRecord = new SchemaRecord( id );
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        if ( inUse )
        {
            schemaRecord.setInUse( inUse );
            if ( bitFlag( flags, Record.CREATED_IN_TX ) )
            {
                schemaRecord.setCreated();
            }
            schemaRecord.setUseFixedReferences( bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT ) );

            byte schemaFlags = channel.get();
            schemaRecord.setConstraint( bitFlag( schemaFlags, SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT ) );
            schemaRecord.setNextProp( channel.getLong() );
            if ( bitFlag( flags, Record.HAS_SECONDARY_UNIT ) )
            {
                schemaRecord.setSecondaryUnitIdOnLoad( channel.getLong() );
            }
        }
        else
        {
            schemaRecord.clear();
        }
        return schemaRecord;
    }

    private SchemaRule readSchemaRule( long id, ReadableChannel channel ) throws IOException
    {
        Map<String,Value> ruleMap = readStringValueMap( channel );
        try
        {
            return SchemaStore.unmapifySchemaRule( id, ruleMap );
        }
        catch ( MalformedSchemaRuleException e )
        {
            throw new IOException( "Failed to create a schema rule from string-value map: " + ruleMap, e );
        }
    }

    Map<String,Value> readStringValueMap( ReadableChannel channel ) throws IOException
    {
        Map<String,Value> map = new HashMap<>();
        int size = channel.getInt();
        for ( int i = 0; i < size; i++ )
        {
            byte[] keyBytes = readMapKeyByteArray( channel );
            String key = UTF8.decode( keyBytes );
            Value value = readMapValue( channel );
            map.put( key, value );
        }
        return map;
    }

    private byte[] readMapKeyByteArray( ReadableChannel channel ) throws IOException
    {
        int size = channel.getInt();
        byte[] bytes = new byte[size];
        channel.get( bytes, size );
        return bytes;
    }

    private Value readMapValue( ReadableChannel channel ) throws IOException
    {
        SchemaMapValueType type = SchemaMapValueType.map( channel.get() );
        switch ( type )
        {
        case BOOL_LITERAL_TRUE:
            return Values.booleanValue( true );
        case BOOL_LITERAL_FALSE:
            return Values.booleanValue( false );
        case BOOL_ARRAY_ELEMENT:
            throw new IOException( "Cannot read schema rule map value of type boolean array element as a top-level type." );
        case BYTE:
            return Values.byteValue( channel.get() );
        case SHORT:
            return Values.shortValue( channel.getShort() );
        case INT:
            return Values.intValue( channel.getInt() );
        case LONG:
            return Values.longValue( channel.getLong() );
        case FLOAT:
            return Values.floatValue( channel.getFloat() );
        case DOUBLE:
            return Values.doubleValue( channel.getDouble() );
        case STRING:
        {
            int size = channel.getInt();
            byte[] bytes = new byte[size];
            channel.get( bytes, size );
            return Values.utf8Value( bytes );
        }
        case CHAR:
            return Values.charValue( (char) channel.getInt() );
        case ARRAY:
        {
            int arraySize = channel.getInt();
            SchemaMapValueType elementType = SchemaMapValueType.map( channel.get() );
            switch ( elementType )
            {
            case BOOL_LITERAL_TRUE:
                throw new IOException( "BOOL_LITERAL_TRUE cannot be a schema rule map value array element type." );
            case BOOL_LITERAL_FALSE:
                throw new IOException( "BOOL_LITERAL_FALSE cannot be a schema rule map value array element type." );
            case BOOL_ARRAY_ELEMENT:
            {
                boolean[] array = new boolean[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.get() == SchemaMapValueType.BOOL_LITERAL_TRUE.type();
                }
                return Values.booleanArray( array );
            }
            case BYTE:
            {
                byte[] array = new byte[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.get();
                }
                return Values.byteArray( array );
            }
            case SHORT:
            {
                short[] array = new short[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.getShort();
                }
                return Values.shortArray( array );
            }
            case INT:
            {
                int[] array = new int[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.getInt();
                }
                return Values.intArray( array );
            }
            case LONG:
            {
                long[] array = new long[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.getLong();
                }
                return Values.longArray( array );
            }
            case FLOAT:
            {
                float[] array = new float[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.getFloat();
                }
                return Values.floatArray( array );
            }
            case DOUBLE:
            {
                double[] array = new double[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = channel.getDouble();
                }
                return Values.doubleArray( array );
            }
            case STRING:
            {
                String[] array = new String[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    int size = channel.getInt();
                    byte[] bytes = new byte[size];
                    channel.get( bytes, size );
                    array[i] = UTF8.decode( bytes );
                }
                return Values.stringArray( array );
            }
            case CHAR:
            {
                char[] array = new char[arraySize];
                for ( int i = 0; i < arraySize; i++ )
                {
                    array[i] = (char) channel.getInt();
                }
                return Values.charArray( array );
            }
            case ARRAY:
                throw new IOException( "Nested arrays are not support for schema rule map values." );
            default:
                throw new IOException( "Unknown array element type: " + elementType );
            }
        } // array case
        default:
            throw new IOException( "Unknown schema map value type: " + type );
        } // switch clause
    }

    private NodeRecord readNodeRecord( long id, ReadableChannel channel ) throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean isCreated = bitFlag( flags, Record.CREATED_IN_TX );
        boolean requiresSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        NodeRecord record;
        Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
        long labelField = Record.NO_LABELS_FIELD.intValue();
        if ( inUse )
        {
            boolean dense = channel.get() == 1;
            long nextRel = channel.getLong();
            long nextProp = channel.getLong();
            record = new NodeRecord( id ).initialize( false, nextProp, dense, nextRel, 0 );
            // labels
            labelField = channel.getLong();
            record.setRequiresSecondaryUnit( requiresSecondaryUnit );
            if ( hasSecondaryUnit )
            {
                record.setSecondaryUnitIdOnLoad( channel.getLong() );
            }
            record.setUseFixedReferences( usesFixedReferenceFormat );
        }
        else
        {
            record = new NodeRecord( id );
        }
        readDynamicRecords( channel, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
        record.setLabelField( labelField, dynamicLabelRecords );
        record.setInUse( inUse );
        if ( isCreated )
        {
            record.setCreated();
        }
        return record;
    }

    private RelationshipRecord readRelationshipRecord( long id, ReadableChannel channel ) throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean requiresSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        RelationshipRecord record;
        if ( inUse )
        {
            record = new RelationshipRecord( id );
            record.setLinks( channel.getLong(), channel.getLong(), channel.getInt() );
            record.setInUse( true );
            record.setRequiresSecondaryUnit( requiresSecondaryUnit );
            record.setFirstPrevRel( channel.getLong() );
            record.setFirstNextRel( channel.getLong() );
            record.setSecondPrevRel( channel.getLong() );
            record.setSecondNextRel( channel.getLong() );
            record.setNextProp( channel.getLong() );
            byte extraByte = channel.get();
            record.setFirstInFirstChain( (extraByte & 0x1) > 0 );
            record.setFirstInSecondChain( (extraByte & 0x2) > 0 );
            if ( hasSecondaryUnit )
            {
                record.setSecondaryUnitIdOnLoad( channel.getLong() );
            }
            record.setUseFixedReferences( usesFixedReferenceFormat );
        }
        else
        {
            record = new RelationshipRecord( id );
            record.setLinks( -1, -1, channel.getInt() );
            record.setInUse( false );
        }
        if ( bitFlag( flags, Record.CREATED_IN_TX ) )
        {
            record.setCreated();
        }

        return record;
    }

    private DynamicRecord readDynamicRecord( ReadableChannel channel ) throws IOException
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

    private <T> int readDynamicRecords( ReadableChannel channel, T target, CommandReading.DynamicRecordAdder<T> adder )
            throws IOException
    {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            adder.add( target, read );
            numberOfRecords--;
        }
        return numberOfRecords;
    }

    private PropertyRecord readPropertyRecord( long id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(long)+next_prop_id(long)
        PropertyRecord record = new PropertyRecord( id );
        byte flags = channel.get(); // 1

        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean nodeProperty = !bitFlag( flags, Record.REL_PROPERTY.byteValue() );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        record.setUseFixedReferences( usesFixedReferenceFormat );

        long nextProp = channel.getLong(); // 8
        long prevProp = channel.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );

        long primitiveId = channel.getLong(); // 8
        if ( primitiveId != -1 && nodeProperty )
        {
            record.setNodeId( primitiveId );
        }
        else if ( primitiveId != -1 )
        {
            record.setRelId( primitiveId );
        }
        if ( hasSecondaryUnit )
        {
            record.setSecondaryUnitIdOnLoad( channel.getLong() );
        }
        int nrPropBlocks = channel.get();
        assert nrPropBlocks >= 0;
        if ( nrPropBlocks > 0 )
        {
            record.setInUse( true );
        }
        while ( nrPropBlocks-- > 0 )
        {
            PropertyBlock block = readPropertyBlock( channel );
            if ( block == null )
            {
                return null;
            }
            record.addPropertyBlock( block );
        }
        int deletedRecords = readDynamicRecords( channel, record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER );
        if ( deletedRecords == -1 )
        {
            return null;
        }
        assert deletedRecords >= 0;
        while ( deletedRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            record.addDeletedRecord( read );
        }
        if ( (inUse && !record.inUse()) || (!inUse && record.inUse()) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as " + inUse + " but the record is " + record );
        }
        return record;
    }

    private PropertyBlock readPropertyBlock( ReadableChannel channel ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
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
        if ( readDynamicRecords( channel, toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        return toReturn;
    }

    private long[] readLongs( ReadableChannel channel, int count ) throws IOException
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = channel.getLong();
        }
        return result;
    }

    @Override
    protected Command readNodeCountsCommand( ReadableChannel channel ) throws IOException
    {
        int labelId = channel.getInt();
        long delta = channel.getLong();
        return new Command.NodeCountsCommand( this, labelId, delta );
    }

    @Override
    protected Command readRelationshipCountsCommand( ReadableChannel channel ) throws IOException
    {
        int startLabelId = channel.getInt();
        int typeId = channel.getInt();
        int endLabelId = channel.getInt();
        long delta = channel.getLong();
        return new Command.RelationshipCountsCommand( this, startLabelId, typeId, endLabelId, delta );
    }

    @Override
    protected Command readNeoStoreCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    static void markAfterRecordAsCreatedIfCommandLooksCreated( AbstractBaseRecord before, AbstractBaseRecord after )
    {
        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }
        if ( !before.hasSecondaryUnitId() && after.hasSecondaryUnitId() )
        {
            // Override the "load" of the secondary unit to be a create since the before state didn't have it and the after does
            after.setSecondaryUnitIdOnCreate( after.getSecondaryUnitId() );
        }
    }

    private enum SchemaMapValueType
    {
        // NOTE: Enum order (specifically, the enum ordinal) is part of the binary format!
        BOOL_LITERAL_TRUE,
        BOOL_LITERAL_FALSE,
        BOOL_ARRAY_ELEMENT,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        CHAR,
        ARRAY;

        private static final SchemaMapValueType[] TYPE_ID_TO_ENUM = values(); // This works because 'type' is equal to ordinal.

        public static SchemaMapValueType map( byte type )
        {
            return TYPE_ID_TO_ENUM[type];
        }

        public static SchemaMapValueType map( ValueWriter.ArrayType arrayType ) throws IOException
        {
            switch ( arrayType )
            {
            case BYTE:
                return BYTE;
            case SHORT:
                return SHORT;
            case INT:
                return INT;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case BOOLEAN:
                return BOOL_ARRAY_ELEMENT;
            case STRING:
                return STRING;
            case CHAR:
                return CHAR;
            case POINT:
            case ZONED_DATE_TIME:
            case LOCAL_DATE_TIME:
            case DATE:
            case ZONED_TIME:
            case LOCAL_TIME:
            case DURATION:
            default:
                throw new IOException( "Unsupported schema record map value type: " + arrayType );
            }
        }

        public byte type()
        {
            return (byte) ordinal();
        }
    }

    @Override
    protected Command readLegacySchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexDefineCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexAddNodeCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexAddRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexCreateCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexDeleteCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    @Override
    protected Command readIndexRemoveCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }
}
