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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.SchemaStore;
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
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;

import static org.neo4j.util.Bits.bitFlag;
import static org.neo4j.util.Bits.bitFlags;

class LogCommandSerializationV4_2 extends LogCommandSerializationV4_0
{
    static final LogCommandSerializationV4_2 INSTANCE = new LogCommandSerializationV4_2();

    @Override
    KernelVersion version()
    {
        return KernelVersion.V4_2;
    }

    @Override
    public void writeNodeCommand( WritableChannel channel, Command.NodeCommand command ) throws IOException
    {
        channel.put( NeoCommandType.NODE_COMMAND );
        channel.putLong( command.getAfter().getId() );
        writeNodeRecord( channel, command.getBefore() );
        writeNodeRecord( channel, command.getAfter() );
    }

    private static void writeNodeRecord( WritableChannel channel, NodeRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
        channel.put( flags );
        if ( record.inUse() )
        {
            channel.put( record.isDense() ? (byte) 1 : (byte) 0 );
            channel.putLong( record.getNextRel() ).putLong( record.getNextProp() );
            channel.putLong( record.getLabelField() );
            if ( record.hasSecondaryUnitId() )
            {
                channel.putLong( record.getSecondaryUnitId() );
            }
        }
        // Always write dynamic label records because we want to know which ones have been deleted
        // especially if the node has been deleted.
        writeDynamicRecords( channel, record.getDynamicLabelRecords() );
    }

    @Override
    public void writeRelationshipCommand( WritableChannel channel, Command.RelationshipCommand command ) throws IOException
    {
        channel.put( NeoCommandType.REL_COMMAND );
        channel.putLong( command.getAfter().getId() );
        writeRelationshipRecord( channel, command.getBefore() );
        writeRelationshipRecord( channel, command.getAfter() );
    }

    private static void writeRelationshipRecord( WritableChannel channel, RelationshipRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
        channel.put( flags );
        if ( record.inUse() )
        {
            channel.putLong( record.getFirstNode() ).putLong( record.getSecondNode() ).putInt( record.getType() )
                    .putLong( record.getFirstPrevRel() ).putLong( record.getFirstNextRel() )
                    .putLong( record.getSecondPrevRel() ).putLong( record.getSecondNextRel() )
                    .putLong( record.getNextProp() )
                    .put( (byte) ((record.isFirstInFirstChain() ? 1 : 0) | (record.isFirstInSecondChain() ? 2 : 0)) );
            if ( record.hasSecondaryUnitId() )
            {
                channel.putLong( record.getSecondaryUnitId() );
            }
        }
        else
        {
            channel.putInt( record.getType() );
        }
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
                bitFlag( record.getRelId() != -1, Record.REL_PROPERTY.byteValue() ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );

        channel.put( flags ); // 1
        channel.putLong( record.getNextProp() ).putLong( record.getPrevProp() ); // 8 + 8
        long nodeId = record.getNodeId();
        long relId = record.getRelId();
        if ( nodeId != -1 )
        {
            channel.putLong( nodeId ); // 8 or
        }
        else if ( relId != -1 )
        {
            channel.putLong( relId ); // 8 or
        }
        else
        {
            // means this records value has not changed, only place in
            // prop chain
            channel.putLong( -1 ); // 8
        }
        if ( record.hasSecondaryUnitId() )
        {
            channel.putLong( record.getSecondaryUnitId() );
        }
        channel.put( (byte) record.numberOfProperties() ); // 1
        for ( PropertyBlock block : record )
        {
            assert block.getSize() > 0 : record + " seems kinda broken";
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

    /**
     * This command can be serialized into two different command types, depending on if the relationship type will fit in 2 or 3 bytes.
     * High-limit format might require more bytes, this was unfortunately not tested properly, so this is an afterthought.
     * This approach was chosen to minimize impact of introducing a new command in a patch release, that could prevent
     * users from upgrading.
     */
    @Override
    public void writeRelationshipGroupCommand( WritableChannel channel, Command.RelationshipGroupCommand command ) throws IOException
    {
        int relType = Math.max( command.getBefore().getType(), command.getAfter().getType() );
        if ( relType == Record.NULL_REFERENCE.intValue() || relType >>> Short.SIZE == 0 )
        {
            // relType will fit in a short
            channel.put( NeoCommandType.REL_GROUP_COMMAND );
            channel.putLong( command.getAfter().getId() );
            writeRelationshipGroupRecord( channel, command.getBefore() );
            writeRelationshipGroupRecord( channel, command.getAfter() );
        }
        else
        {
            // here we need 3 bytes to store the relType
            channel.put( NeoCommandType.REL_GROUP_EXTENDED_COMMAND );
            channel.putLong( command.getAfter().getId() );
            writeRelationshipGroupExtendedRecord( channel, command.getBefore() );
            writeRelationshipGroupExtendedRecord( channel, command.getAfter() );
        }
    }

    private static void writeRelationshipGroupRecord( WritableChannel channel, RelationshipGroupRecord record )
            throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
        channel.put( flags );
        channel.putShort( (short) record.getType() );
        channel.putLong( record.getNext() );
        channel.putLong( record.getFirstOut() );
        channel.putLong( record.getFirstIn() );
        channel.putLong( record.getFirstLoop() );
        channel.putLong( record.getOwningNode() );
        if ( record.hasSecondaryUnitId() )
        {
            channel.putLong( record.getSecondaryUnitId() );
        }
    }

    private void writeRelationshipGroupExtendedRecord( WritableChannel channel, RelationshipGroupRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
        channel.put( flags );
        channel.putShort( (short) record.getType() );
        channel.put( (byte) (record.getType() >>> Short.SIZE) );
        channel.putLong( record.getNext() );
        channel.putLong( record.getFirstOut() );
        channel.putLong( record.getFirstIn() );
        channel.putLong( record.getFirstLoop() );
        channel.putLong( record.getOwningNode() );
        if ( record.hasSecondaryUnitId() )
        {
            channel.putLong( record.getSecondaryUnitId() );
        }
    }

    @Override
    public void writeRelationshipTypeTokenCommand( WritableChannel channel, Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        channel.put( NeoCommandType.REL_TYPE_COMMAND );
        channel.putInt( command.getAfter().getIntId() );
        writeRelationshipTypeTokenRecord( channel, command.getBefore() );
        writeRelationshipTypeTokenRecord( channel, command.getAfter() );
    }

    private static void writeRelationshipTypeTokenRecord( WritableChannel channel, RelationshipTypeTokenRecord record ) throws IOException
    {
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        byte headerByte = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        headerByte += record.isInternal() ? Record.ADDITIONAL_FLAG_1 : 0;
        channel.put( headerByte );
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
    public void writeLabelTokenCommand( WritableChannel channel, Command.LabelTokenCommand command ) throws IOException
    {
        channel.put( NeoCommandType.LABEL_KEY_COMMAND );
        channel.putInt( command.getAfter().getIntId() );
        writeLabelTokenRecord( channel, command.getBefore() );
        writeLabelTokenRecord( channel, command.getAfter() );
    }

    private static void writeLabelTokenRecord( WritableChannel channel, LabelTokenRecord record ) throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte headerByte = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        headerByte += record.isInternal() ? Record.ADDITIONAL_FLAG_1 : 0;
        channel.put( headerByte ).putInt( record.getNameId() );
        writeDynamicRecords( channel, record.getNameRecords() );
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
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        byte headerByte = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        headerByte += record.isInternal() ? Record.ADDITIONAL_FLAG_1 : 0;
        channel.put( headerByte );
        channel.putInt( record.getPropertyCount() ).putInt( record.getNameId() );
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
                bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ) );
        channel.put( flags );
        if ( record.inUse() )
        {
            byte schemaFlags = bitFlags( bitFlag( record.isConstraint(), SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT ) );
            channel.put( schemaFlags );
            channel.putLong( record.getNextProp() );
            if ( record.hasSecondaryUnitId() )
            {
                channel.putLong( record.getSecondaryUnitId() );
            }
        }
    }

    private static void writeSchemaRule( WritableChannel channel, SchemaRule schemaRule ) throws IOException
    {
        Map<String,Value> ruleMap = SchemaStore.mapifySchemaRule( schemaRule );
        writeStringValueMap( channel, ruleMap );
    }

    /**
     * @see LogCommandSerializationV4_0#readStringValueMap(ReadableChannel)
     */
    static void writeStringValueMap( WritableChannel channel, Map<String,Value> ruleMap ) throws IOException
    {
        channel.putInt( ruleMap.size() );
        for ( Map.Entry<String,Value> entry : ruleMap.entrySet() )
        {
            writeMapKeyByteArray( channel, UTF8.encode( entry.getKey() ) );
            writeMapValue( channel, entry.getValue() );
        }
    }

    private static void writeMapKeyByteArray( WritableChannel channel, byte[] bytes ) throws IOException
    {
        channel.putInt( bytes.length );
        channel.put( bytes, bytes.length );
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

    private static void writeMapValue( WritableChannel channel, Value value ) throws IOException
    {
        value.writeTo( new ValueWriter<IOException>()
        {
            private boolean arrayContext;

            @Override
            public void writeNull() throws IOException
            {
                throw new IOException( "Cannot write null entry value in schema record map representation." );
            }

            @Override
            public void writeBoolean( boolean value ) throws IOException
            {
                if ( value )
                {
                    channel.put( SchemaMapValueType.BOOL_LITERAL_TRUE.type() );
                }
                else
                {
                    channel.put( SchemaMapValueType.BOOL_LITERAL_FALSE.type() );
                }
            }

            @Override
            public void writeInteger( byte value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.BYTE.type() );
                }
                channel.put( value );
            }

            @Override
            public void writeInteger( short value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.SHORT.type() );
                }
                channel.putShort( value );
            }

            @Override
            public void writeInteger( int value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.INT.type() );
                }
                channel.putInt( value );
            }

            @Override
            public void writeInteger( long value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.LONG.type() );
                }
                channel.putLong( value );
            }

            @Override
            public void writeFloatingPoint( float value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.FLOAT.type() );
                }
                channel.putFloat( value );
            }

            @Override
            public void writeFloatingPoint( double value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.DOUBLE.type() );
                }
                channel.putDouble( value );
            }

            @Override
            public void writeString( String value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.STRING.type() );
                }
                byte[] bytes = UTF8.encode( value );
                channel.putInt( bytes.length );
                channel.put( bytes, bytes.length );
            }

            @Override
            public void writeString( char value ) throws IOException
            {
                if ( !arrayContext )
                {
                    channel.put( SchemaMapValueType.CHAR.type() );
                }
                channel.putInt( value );
            }

            @Override
            public void beginArray( int size, ValueWriter.ArrayType arrayType ) throws IOException
            {
                arrayContext = true;
                channel.put( SchemaMapValueType.ARRAY.type() );
                channel.putInt( size );
                channel.put( SchemaMapValueType.map( arrayType ).type() );
            }

            @Override
            public void endArray()
            {
                arrayContext = false;
            }

            @Override
            public void writeByteArray( byte[] value ) throws IOException
            {
                beginArray( value.length, ArrayType.BYTE );
                for ( byte b : value )
                {
                    writeInteger( b );
                }
                endArray();
            }

            @Override
            public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws IOException
            {
                throw new IOException( "Point is not a supported schema map value type." );
            }

            @Override
            public void writeDuration( long months, long days, long seconds, int nanos ) throws IOException
            {
                throw new IOException( "Duration is not a supported schema map value type." );
            }

            @Override
            public void writeDate( LocalDate localDate ) throws IOException
            {
                throw new IOException( "Date is not a supported schema map value type." );
            }

            @Override
            public void writeLocalTime( LocalTime localTime ) throws IOException
            {
                throw new IOException( "LocalTime is not a supported schema map value type." );
            }

            @Override
            public void writeTime( OffsetTime offsetTime ) throws IOException
            {
                throw new IOException( "OffsetTime is not a supported schema map value type." );
            }

            @Override
            public void writeLocalDateTime( LocalDateTime localDateTime ) throws IOException
            {
                throw new IOException( "LocalDateTime is not a supported schema map value type." );
            }

            @Override
            public void writeDateTime( ZonedDateTime zonedDateTime ) throws IOException
            {
                throw new IOException( "DateTime is not a supported schema map value type." );
            }
        } );
    }

    @Override
    public void writeNodeCountsCommand( WritableChannel channel, Command.NodeCountsCommand command ) throws IOException
    {
        channel.put( NeoCommandType.UPDATE_NODE_COUNTS_COMMAND );
        channel.putInt( command.labelId() )
               .putLong( command.delta() );
    }

    @Override
    public void writeRelationshipCountsCommand( WritableChannel channel, Command.RelationshipCountsCommand command ) throws IOException
    {
        channel.put( NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND );
        channel.putInt( command.startLabelId() )
               .putInt( command.typeId() )
               .putInt( command.endLabelId() )
               .putLong( command.delta() );
    }

    static void writeDynamicRecords( WritableChannel channel, Collection<DynamicRecord> records ) throws IOException
    {
        writeDynamicRecords( channel, records, records.size() );
    }

    static void writeDynamicRecords( WritableChannel channel, Iterable<DynamicRecord> records, int size ) throws IOException
    {
        channel.putInt( size ); // 4
        for ( DynamicRecord record : records )
        {
            writeDynamicRecord( channel, record );
        }
    }

    static void writeDynamicRecord( WritableChannel channel, DynamicRecord record ) throws IOException
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
}
