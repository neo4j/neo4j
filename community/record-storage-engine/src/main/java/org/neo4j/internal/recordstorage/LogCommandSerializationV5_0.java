/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import static org.neo4j.internal.schema.SchemaRuleMapifier.unmapifySchemaRule;
import static org.neo4j.util.BitUtils.bitFlag;
import static org.neo4j.util.BitUtils.bitFlags;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaRuleMapifier;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
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

class LogCommandSerializationV5_0 extends LogCommandSerializationV4_4 {
    static final LogCommandSerializationV5_0 INSTANCE = new LogCommandSerializationV5_0();

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.V5_0;
    }

    @Override
    protected Command readPropertyKeyTokenCommand(ReadableChannel channel) throws IOException {
        int id = channel.getInt();
        var before = readPropertyKeyTokenRecord(id, channel);
        var after = readPropertyKeyTokenRecord(id, channel);

        return new Command.PropertyKeyTokenCommand(this, before, after);
    }

    private static PropertyKeyTokenRecord readPropertyKeyTokenRecord(int id, ReadableChannel channel)
            throws IOException {
        // flags(byte)+propertyCount(int)+nameId(int)+nr_key_records(int)
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean createdInTx = bitFlag(flags, Record.CREATED_IN_TX);
        boolean internal = bitFlag(flags, Record.TOKEN_INTERNAL);

        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord(id);
        record.setInUse(inUse);
        record.setInternal(internal);
        record.setCreated(createdInTx);
        record.setPropertyCount(channel.getInt());
        record.setNameId(channel.getInt());
        readDynamicRecords(channel, record::addNameRecord);
        return record;
    }

    @Override
    public void writePropertyKeyTokenCommand(WritableChannel channel, Command.PropertyKeyTokenCommand command)
            throws IOException {
        channel.put(NeoCommandType.PROP_INDEX_COMMAND);
        channel.putInt(command.getAfter().getIntId());
        writePropertyKeyTokenRecord(channel, command.getBefore());
        writePropertyKeyTokenRecord(channel, command.getAfter());
    }

    private static void writePropertyKeyTokenRecord(WritableChannel channel, PropertyKeyTokenRecord record)
            throws IOException {
        // flags(byte)+propertyCount(int)+nameId(int)+nr_key_records(int)
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.isInternal(), Record.TOKEN_INTERNAL));
        channel.put(flags);
        channel.putInt(record.getPropertyCount());
        channel.putInt(record.getNameId());
        if (record.isLight()) {
            channel.putInt(0);
        } else {
            writeDynamicRecords(channel, record.getNameRecords());
        }
    }

    @Override
    protected Command readLabelTokenCommand(ReadableChannel channel) throws IOException {
        int id = channel.getInt();
        LabelTokenRecord before = readLabelTokenRecord(id, channel);
        LabelTokenRecord after = readLabelTokenRecord(id, channel);
        return new Command.LabelTokenCommand(this, before, after);
    }

    private static LabelTokenRecord readLabelTokenRecord(int id, ReadableChannel channel) throws IOException {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean createdInTx = bitFlag(flags, Record.CREATED_IN_TX);
        boolean internal = bitFlag(flags, Record.TOKEN_INTERNAL);

        LabelTokenRecord record = new LabelTokenRecord(id);
        record.setInUse(inUse);
        record.setInternal(internal);
        record.setCreated(createdInTx);

        record.setNameId(channel.getInt());
        readDynamicRecords(channel, record::addNameRecord);
        return record;
    }

    @Override
    public void writeLabelTokenCommand(WritableChannel channel, Command.LabelTokenCommand command) throws IOException {
        channel.put(NeoCommandType.LABEL_KEY_COMMAND);
        channel.putInt(command.getAfter().getIntId());
        writeLabelTokenRecord(channel, command.getBefore());
        writeLabelTokenRecord(channel, command.getAfter());
    }

    private static void writeLabelTokenRecord(WritableChannel channel, LabelTokenRecord record) throws IOException {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.isInternal(), Record.TOKEN_INTERNAL));

        channel.put(flags).putInt(record.getNameId());
        writeDynamicRecords(channel, record.getNameRecords());
    }

    @Override
    protected Command readRelationshipTypeTokenCommand(ReadableChannel channel) throws IOException {
        int id = channel.getInt();
        var before = readRelationshipTypeTokenRecord(id, channel);
        var after = readRelationshipTypeTokenRecord(id, channel);
        return new Command.RelationshipTypeTokenCommand(this, before, after);
    }

    private static RelationshipTypeTokenRecord readRelationshipTypeTokenRecord(int id, ReadableChannel channel)
            throws IOException {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean createdInTx = bitFlag(flags, Record.CREATED_IN_TX);
        boolean internal = bitFlag(flags, Record.TOKEN_INTERNAL);

        var record = new RelationshipTypeTokenRecord(id);
        record.setInUse(inUse);
        record.setInternal(internal);
        record.setCreated(createdInTx);

        record.setNameId(channel.getInt());
        readDynamicRecords(channel, record::addNameRecord);
        return record;
    }

    @Override
    public void writeRelationshipTypeTokenCommand(WritableChannel channel, Command.RelationshipTypeTokenCommand command)
            throws IOException {
        channel.put(NeoCommandType.REL_TYPE_COMMAND);
        channel.putInt(command.getAfter().getIntId());
        writeRelationshipTypeTokenRecord(channel, command.getBefore());
        writeRelationshipTypeTokenRecord(channel, command.getAfter());
    }

    private static void writeRelationshipTypeTokenRecord(WritableChannel channel, RelationshipTypeTokenRecord record)
            throws IOException {
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.isInternal(), Record.TOKEN_INTERNAL));
        channel.put(flags);
        channel.putInt(record.getNameId());
        writeDynamicRecords(channel, record.getNameRecords());
    }

    @Override
    protected Command readSchemaRuleCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        byte schemaRulePresence = channel.get();
        boolean hasSchemaRule = schemaRulePresence == SchemaRecord.COMMAND_HAS_SCHEMA_RULE;
        SchemaRecord before = readSchemaRecord(id, channel);
        SchemaRecord after = readSchemaRecord(id, channel);
        SchemaRule schemaRule = null;
        if (hasSchemaRule) {
            schemaRule = readSchemaRule(id, channel);
        }
        return new Command.SchemaRuleCommand(this, before, after, schemaRule);
    }

    private static SchemaRecord readSchemaRecord(long id, ReadableChannel channel) throws IOException {
        SchemaRecord schemaRecord = new SchemaRecord(id);
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean createdInTx = bitFlag(flags, Record.CREATED_IN_TX);
        schemaRecord.setInUse(inUse);
        if (inUse) {
            byte schemaFlags = channel.get();
            schemaRecord.setConstraint(bitFlag(schemaFlags, SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT));
            schemaRecord.setNextProp(channel.getLong());
        }
        schemaRecord.setCreated(createdInTx);

        return schemaRecord;
    }

    static SchemaRule readSchemaRule(long id, ReadableChannel channel) throws IOException {
        Map<String, Value> ruleMap = readStringValueMap(channel);
        try {
            return unmapifySchemaRule(id, ruleMap);
        } catch (MalformedSchemaRuleException e) {
            throw new IOException("Failed to create a schema rule from string-value map: " + ruleMap, e);
        }
    }

    @Override
    public void writeSchemaRuleCommand(WritableChannel channel, Command.SchemaRuleCommand command) throws IOException {
        channel.put(NeoCommandType.SCHEMA_RULE_COMMAND);
        channel.putLong(command.getBefore().getId());
        SchemaRule schemaRule = command.getSchemaRule();
        boolean hasSchemaRule = schemaRule != null;
        channel.put(hasSchemaRule ? SchemaRecord.COMMAND_HAS_SCHEMA_RULE : SchemaRecord.COMMAND_HAS_NO_SCHEMA_RULE);
        writeSchemaRecord(channel, command.getBefore());
        writeSchemaRecord(channel, command.getAfter());
        if (hasSchemaRule) {
            writeSchemaRule(channel, schemaRule);
        }
    }

    private static void writeSchemaRecord(WritableChannel channel, SchemaRecord record) throws IOException {
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()), bitFlag(record.isCreated(), Record.CREATED_IN_TX));
        channel.put(flags);
        if (record.inUse()) {
            byte schemaFlags = bitFlag(record.isConstraint(), SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT);
            channel.put(schemaFlags);
            channel.putLong(record.getNextProp());
        }
    }

    private static void writeSchemaRule(WritableChannel channel, SchemaRule schemaRule) throws IOException {
        Map<String, Value> ruleMap = SchemaRuleMapifier.mapifySchemaRule(schemaRule);
        writeStringValueMap(channel, ruleMap);
    }

    /**
     * @see LogCommandSerializationV4_2#readStringValueMap(ReadableChannel)
     */
    private static void writeStringValueMap(WritableChannel channel, Map<String, Value> ruleMap) throws IOException {
        channel.putInt(ruleMap.size());
        for (Map.Entry<String, Value> entry : ruleMap.entrySet()) {
            writeMapKeyByteArray(channel, UTF8.encode(entry.getKey()));
            writeMapValue(channel, entry.getValue());
        }
    }

    private static void writeMapKeyByteArray(WritableChannel channel, byte[] bytes) throws IOException {
        channel.putInt(bytes.length);
        channel.put(bytes, bytes.length);
    }

    private static void writeMapValue(WritableChannel channel, Value value) throws IOException {
        value.writeTo(new ValueWriter<IOException>() {
            private boolean arrayContext;

            @Override
            public void writeNull() throws IOException {
                throw new IOException("Cannot write null entry value in schema record map representation.");
            }

            @Override
            public void writeBoolean(boolean value) throws IOException {
                if (value) {
                    channel.put(SchemaMapValueType.BOOL_LITERAL_TRUE.type());
                } else {
                    channel.put(SchemaMapValueType.BOOL_LITERAL_FALSE.type());
                }
            }

            @Override
            public void writeInteger(byte value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.BYTE.type());
                }
                channel.put(value);
            }

            @Override
            public void writeInteger(short value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.SHORT.type());
                }
                channel.putShort(value);
            }

            @Override
            public void writeInteger(int value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.INT.type());
                }
                channel.putInt(value);
            }

            @Override
            public void writeInteger(long value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.LONG.type());
                }
                channel.putLong(value);
            }

            @Override
            public void writeFloatingPoint(float value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.FLOAT.type());
                }
                channel.putFloat(value);
            }

            @Override
            public void writeFloatingPoint(double value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.DOUBLE.type());
                }
                channel.putDouble(value);
            }

            @Override
            public void writeString(String value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.STRING.type());
                }
                byte[] bytes = UTF8.encode(value);
                channel.putInt(bytes.length);
                channel.put(bytes, bytes.length);
            }

            @Override
            public void writeString(char value) throws IOException {
                if (!arrayContext) {
                    channel.put(SchemaMapValueType.CHAR.type());
                }
                channel.putInt(value);
            }

            @Override
            public void beginArray(int size, ValueWriter.ArrayType arrayType) throws IOException {
                arrayContext = true;
                channel.put(SchemaMapValueType.ARRAY.type());
                channel.putInt(size);
                channel.put(SchemaMapValueType.map(arrayType).type());
            }

            @Override
            public void endArray() {
                arrayContext = false;
            }

            @Override
            public void writeByteArray(byte[] value) throws IOException {
                beginArray(value.length, ArrayType.BYTE);
                for (byte b : value) {
                    writeInteger(b);
                }
                endArray();
            }

            @Override
            public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) throws IOException {
                throw new IOException("Point is not a supported schema map value type.");
            }

            @Override
            public void writeDuration(long months, long days, long seconds, int nanos) throws IOException {
                throw new IOException("Duration is not a supported schema map value type.");
            }

            @Override
            public void writeDate(LocalDate localDate) throws IOException {
                throw new IOException("Date is not a supported schema map value type.");
            }

            @Override
            public void writeLocalTime(LocalTime localTime) throws IOException {
                throw new IOException("LocalTime is not a supported schema map value type.");
            }

            @Override
            public void writeTime(OffsetTime offsetTime) throws IOException {
                throw new IOException("OffsetTime is not a supported schema map value type.");
            }

            @Override
            public void writeLocalDateTime(LocalDateTime localDateTime) throws IOException {
                throw new IOException("LocalDateTime is not a supported schema map value type.");
            }

            @Override
            public void writeDateTime(ZonedDateTime zonedDateTime) throws IOException {
                throw new IOException("DateTime is not a supported schema map value type.");
            }
        });
    }

    @Override
    protected Command readPropertyCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong(); // 8
        var before = readPropertyRecord(id, channel);
        var after = readPropertyRecord(id, channel);
        return new Command.PropertyCommand(this, before, after);
    }

    static PropertyRecord readPropertyRecord(long id, ReadableChannel channel) throws IOException {
        var record = new PropertyRecord(id);
        byte flags = channel.get(); // 1

        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean isCreated = bitFlag(flags, Record.CREATED_IN_TX);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean nodeProperty = bitFlag(flags, Record.PROPERTY_OWNED_BY_NODE);
        boolean relProperty = bitFlag(flags, Record.PROPERTY_OWNED_BY_RELATIONSHIP);

        record.setInUse(inUse);
        record.setUseFixedReferences(usesFixedReferenceFormat);
        record.setCreated(isCreated);

        long nextProp = channel.getLong(); // 8
        long prevProp = channel.getLong(); // 8
        record.setNextProp(nextProp);
        record.setPrevProp(prevProp);

        long primitiveId = channel.getLong(); // 8
        setPropertyRecordOwner(record, nodeProperty, relProperty, primitiveId);

        int nrPropBlocks = channel.get(); // 1
        assert nrPropBlocks >= 0;
        while (nrPropBlocks-- > 0) {
            var block = readPropertyBlock(channel);
            record.addPropertyBlock(block);
        }
        readDynamicRecords(channel, record::addDeletedRecord);
        if (record.inUse() != (record.numberOfProperties() > 0)) {
            throw new IllegalStateException("Weird, inUse was read in as " + inUse + " but the record is " + record);
        }
        return record;
    }

    private static void setPropertyRecordOwner(
            PropertyRecord record, boolean nodeProperty, boolean relProperty, long primitiveId) {
        assert !(nodeProperty && relProperty); // either node or rel or none of them
        if (primitiveId != -1) // unused properties has no owner
        {
            if (nodeProperty) {
                record.setNodeId(primitiveId);
            } else if (relProperty) {
                record.setRelId(primitiveId);
            } else {
                record.setSchemaRuleId(primitiveId);
            }
        }
    }

    private static PropertyBlock readPropertyBlock(ReadableChannel channel) throws IOException {
        var toReturn = new PropertyBlock();
        byte blockSize = channel.get(); // the size is stored in bytes // 1
        // Read in blocks
        long[] blocks = readLongs(channel, blockSize / 8);
        assert blocks.length == blockSize / 8
                : blocks.length + " longs were read in while i asked for what corresponds to " + blockSize;

        assert blocks.length == 0
                        || PropertyType.getPropertyTypeOrThrow(blocks[0]).calculateNumberOfBlocksUsed(blocks[0])
                                == blocks.length
                : blocks.length + " is not a valid number of blocks for type "
                        + PropertyType.getPropertyTypeOrThrow(blocks[0]);
        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks(blocks);
        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        readDynamicRecordList(channel, toReturn::setValueRecords);
        return toReturn;
    }

    @Override
    public void writePropertyCommand(WritableChannel channel, Command.PropertyCommand command) throws IOException {
        channel.put(NeoCommandType.PROP_COMMAND);
        channel.putLong(command.getAfter().getId());
        writePropertyRecord(channel, command.getBefore());
        writePropertyRecord(channel, command.getAfter());
    }

    static void writePropertyRecord(WritableChannel channel, PropertyRecord record) throws IOException {
        assert !record.hasSecondaryUnitId() : "secondary units are not supported for property records";
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT),
                bitFlag(record.isNodeSet(), Record.PROPERTY_OWNED_BY_NODE),
                bitFlag(record.isRelSet(), Record.PROPERTY_OWNED_BY_RELATIONSHIP));

        channel.put(flags); // 1
        channel.putLong(record.getNextProp()).putLong(record.getPrevProp()); // 8 + 8

        long entityId = record.getEntityId();
        channel.putLong(entityId); // 8

        int numberOfProperties = record.numberOfProperties();
        channel.put((byte) numberOfProperties); // 1
        PropertyBlock[] blocks = record.getPropertyBlocks();
        for (int i = 0; i < numberOfProperties; i++) {
            PropertyBlock block = blocks[i];
            assert block.getSize() > 0 : record + " has incorrect size";
            writePropertyBlock(channel, block);
        }
        writeDynamicRecords(channel, record.getDeletedRecords());
    }

    private static void writePropertyBlock(WritableChannel channel, PropertyBlock block) throws IOException {
        byte blockSize = (byte) block.getSize();
        assert blockSize > 0 : blockSize + " is not a valid block size value";
        channel.put(blockSize); // 1
        long[] propBlockValues = block.getValueBlocks();
        for (long propBlockValue : propBlockValues) {
            channel.putLong(propBlockValue);
        }
        /*
         * For each block we need to keep its dynamic record chain if
         * it is just created. Deleted dynamic records are in the property
         * record and dynamic records are never modified. Also, they are
         * assigned as a whole, so just checking the first should be enough.
         */
        if (block.isLight()) {
            /*
             *  This has to be int. If this record is not light
             *  then we have the number of DynamicRecords that follow,
             *  which is an int. We do not currently want/have a flag bit so
             *  we simplify by putting an int here always
             */
            channel.putInt(0); // 4 or
        } else {
            writeDynamicRecords(channel, block.getValueRecords());
        }
    }

    @Override
    public void writeNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        channel.put(NeoCommandType.NODE_COMMAND);
        channel.putLong(command.getAfter().getId());
        writeNodeRecord(channel, command.getBefore());
        writeNodeRecord(channel, command.getAfter());
    }

    static void writeNodeRecord(WritableChannel channel, NodeRecord record) throws IOException {
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT),
                bitFlag(record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT),
                bitFlag(record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT),
                bitFlag(record.isSecondaryUnitCreated(), Record.SECONDARY_UNIT_CREATED_IN_TX));
        channel.put(flags);
        if (record.inUse()) {
            channel.put(record.isDense() ? (byte) 1 : (byte) 0);
            channel.putLong(record.getNextRel()).putLong(record.getNextProp());
            channel.putLong(record.getLabelField());
        }
        if (record.hasSecondaryUnitId()) {
            channel.putLong(record.getSecondaryUnitId());
        }
        // Always write dynamic label records because we want to know which ones have been deleted
        // especially if the node has been deleted.
        writeDynamicRecords(channel, record.getDynamicLabelRecords());
    }

    @Override
    protected Command readNodeCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        NodeRecord before = readNodeRecord(id, channel);
        NodeRecord after = readNodeRecord(id, channel);
        return new Command.NodeCommand(this, before, after);
    }

    static NodeRecord readNodeRecord(long id, ReadableChannel channel) throws IOException {
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean isCreated = bitFlag(flags, Record.CREATED_IN_TX);
        boolean requiresSecondaryUnit = bitFlag(flags, Record.REQUIRE_SECONDARY_UNIT);
        boolean hasSecondaryUnit = bitFlag(flags, Record.HAS_SECONDARY_UNIT);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean secondaryUnitCreated = bitFlag(flags, Record.SECONDARY_UNIT_CREATED_IN_TX);

        var record = new NodeRecord(id);
        if (inUse) {
            boolean dense = channel.get() == 1;
            long nextRel = channel.getLong();
            long nextProp = channel.getLong();
            long labelField = channel.getLong();
            record.initialize(true, nextProp, dense, nextRel, labelField);
        }
        if (hasSecondaryUnit) {
            record.setSecondaryUnitIdOnLoad(channel.getLong());
        }
        record.setRequiresSecondaryUnit(requiresSecondaryUnit);
        record.setUseFixedReferences(usesFixedReferenceFormat);
        record.setSecondaryUnitCreated(secondaryUnitCreated);

        readDynamicRecordList(channel, dlr -> record.setLabelField(record.getLabelField(), dlr));

        record.setCreated(isCreated);
        return record;
    }

    @Override
    public void writeRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        channel.put(NeoCommandType.REL_COMMAND);
        channel.putLong(command.getAfter().getId());
        writeRelationshipRecord(channel, command.getBefore());
        writeRelationshipRecord(channel, command.getAfter());
    }

    static void writeRelationshipRecord(WritableChannel channel, RelationshipRecord record) throws IOException {
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT),
                bitFlag(record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT),
                bitFlag(record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT),
                bitFlag(record.isSecondaryUnitCreated(), Record.SECONDARY_UNIT_CREATED_IN_TX));
        channel.put(flags);
        if (record.inUse()) {
            channel.putLong(record.getFirstNode())
                    .putLong(record.getSecondNode())
                    .putInt(record.getType())
                    .putLong(record.getFirstPrevRel())
                    .putLong(record.getFirstNextRel())
                    .putLong(record.getSecondPrevRel())
                    .putLong(record.getSecondNextRel())
                    .putLong(record.getNextProp());
            var extraByte = bitFlags(
                    bitFlag(record.isFirstInFirstChain(), Record.RELATIONSHIP_FIRST_IN_FIRST_CHAIN),
                    bitFlag(record.isFirstInSecondChain(), Record.RELATIONSHIP_FIRST_IN_SECOND_CHAIN));
            channel.put(extraByte);
        } else {
            channel.putInt(record.getType());
        }
        if (record.hasSecondaryUnitId()) {
            channel.putLong(record.getSecondaryUnitId());
        }
    }

    @Override
    protected Command readRelationshipCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        RelationshipRecord before = readRelationshipRecord(id, channel);
        RelationshipRecord after = readRelationshipRecord(id, channel);
        return new Command.RelationshipCommand(this, before, after);
    }

    static RelationshipRecord readRelationshipRecord(long id, ReadableChannel channel) throws IOException {
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean isCreated = bitFlag(flags, Record.CREATED_IN_TX);
        boolean requiresSecondaryUnit = bitFlag(flags, Record.REQUIRE_SECONDARY_UNIT);
        boolean hasSecondaryUnit = bitFlag(flags, Record.HAS_SECONDARY_UNIT);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean secondaryUnitCreated = bitFlag(flags, Record.SECONDARY_UNIT_CREATED_IN_TX);

        var record = new RelationshipRecord(id);
        if (inUse) {
            record.setInUse(true);
            record.setLinks(channel.getLong(), channel.getLong(), channel.getInt());
            record.setFirstPrevRel(channel.getLong());
            record.setFirstNextRel(channel.getLong());
            record.setSecondPrevRel(channel.getLong());
            record.setSecondNextRel(channel.getLong());
            record.setNextProp(channel.getLong());
            byte extraByte = channel.get();
            record.setFirstInFirstChain(bitFlag(extraByte, Record.RELATIONSHIP_FIRST_IN_FIRST_CHAIN));
            record.setFirstInSecondChain(bitFlag(extraByte, Record.RELATIONSHIP_FIRST_IN_SECOND_CHAIN));
        } else {
            record.setLinks(-1, -1, channel.getInt());
            record.setInUse(false);
        }
        if (hasSecondaryUnit) {
            record.setSecondaryUnitIdOnLoad(channel.getLong());
        }
        record.setRequiresSecondaryUnit(requiresSecondaryUnit);
        record.setUseFixedReferences(usesFixedReferenceFormat);
        record.setSecondaryUnitCreated(secondaryUnitCreated);

        record.setCreated(isCreated);

        return record;
    }

    @Override
    public void writeRelationshipGroupCommand(WritableChannel channel, Command.RelationshipGroupCommand command)
            throws IOException {
        channel.put(NeoCommandType.REL_GROUP_COMMAND);
        channel.putLong(command.getAfter().getId());
        writeRelationshipGroupRecord(channel, command.getBefore());
        writeRelationshipGroupRecord(channel, command.getAfter());
    }

    private static void writeRelationshipGroupRecord(WritableChannel channel, RelationshipGroupRecord record)
            throws IOException {
        byte flags = bitFlags(
                bitFlag(record.inUse(), Record.IN_USE.byteValue()),
                bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                bitFlag(record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT),
                bitFlag(record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT),
                bitFlag(record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT),
                bitFlag(record.isSecondaryUnitCreated(), Record.SECONDARY_UNIT_CREATED_IN_TX));

        channel.put(flags);
        channel.putInt(record.getType());
        channel.putLong(record.getNext());
        channel.putLong(record.getFirstOut());
        channel.putLong(record.getFirstIn());
        channel.putLong(record.getFirstLoop());
        channel.putLong(record.getOwningNode());
        byte externalDegreesFlags = bitFlags(
                bitFlag(record.hasExternalDegreesOut(), Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_OUT),
                bitFlag(record.hasExternalDegreesIn(), Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_IN),
                bitFlag(record.hasExternalDegreesLoop(), Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_LOOP));
        channel.put(externalDegreesFlags);
        if (record.hasSecondaryUnitId()) {
            channel.putLong(record.getSecondaryUnitId());
        }
    }

    @Override
    protected Command readRelationshipGroupCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        var before = readRelationshipGroupRecord(id, channel);
        var after = readRelationshipGroupRecord(id, channel);

        return new Command.RelationshipGroupCommand(this, before, after);
    }

    private static RelationshipGroupRecord readRelationshipGroupRecord(long id, ReadableChannel channel)
            throws IOException {
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean isCreated = bitFlag(flags, Record.CREATED_IN_TX);
        boolean requireSecondaryUnit = bitFlag(flags, Record.REQUIRE_SECONDARY_UNIT);
        boolean hasSecondaryUnit = bitFlag(flags, Record.HAS_SECONDARY_UNIT);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean secondaryUnitCreated = bitFlag(flags, Record.SECONDARY_UNIT_CREATED_IN_TX);

        int type = channel.getInt();
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        var record =
                new RelationshipGroupRecord(id).initialize(inUse, type, firstOut, firstIn, firstLoop, owningNode, next);

        byte externalDegreesFlags = channel.get();
        boolean hasExternalDegreesOut = bitFlag(externalDegreesFlags, Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_OUT);
        boolean hasExternalDegreesIn = bitFlag(externalDegreesFlags, Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_IN);
        boolean hasExternalDegreesLoop = bitFlag(externalDegreesFlags, Record.RELATIONSHIP_GROUP_EXTERNAL_DEGREES_LOOP);
        record.setHasExternalDegreesOut(hasExternalDegreesOut);
        record.setHasExternalDegreesIn(hasExternalDegreesIn);
        record.setHasExternalDegreesLoop(hasExternalDegreesLoop);

        if (hasSecondaryUnit) {
            record.setSecondaryUnitIdOnLoad(channel.getLong());
        }

        record.setRequiresSecondaryUnit(requireSecondaryUnit);
        record.setUseFixedReferences(usesFixedReferenceFormat);
        record.setSecondaryUnitCreated(secondaryUnitCreated);
        record.setCreated(isCreated);
        return record;
    }

    @Override
    protected Command readRelationshipGroupExtendedCommand(ReadableChannel channel) throws IOException {
        // 5.0 serialization doesn't write "extended" group command and therefore doesn't support reading it
        throw unsupportedInThisVersionException();
    }

    @Override
    public void writeMetaDataCommand(WritableChannel channel, Command.MetaDataCommand command) throws IOException {
        channel.put(NeoCommandType.META_DATA_COMMAND);
        channel.putLong(command.getKey());
        writeMetaDataRecord(channel, command.getBefore());
        writeMetaDataRecord(channel, command.getAfter());
    }

    private static void writeMetaDataRecord(WritableChannel channel, MetaDataRecord record) throws IOException {
        byte flags = bitFlag(record.inUse(), Record.IN_USE.byteValue());
        channel.put(flags);
        channel.putLong(record.getValue());
    }

    @Override
    public void writeGroupDegreeCommand(WritableChannel channel, Command.GroupDegreeCommand command)
            throws IOException {
        channel.put(NeoCommandType.UPDATE_GROUP_DEGREE_COMMAND);
        channel.putLong(command.getKey());
        channel.putLong(command.delta());
    }

    @Override
    public void writeNodeCountsCommand(WritableChannel channel, Command.NodeCountsCommand command) throws IOException {
        channel.put(NeoCommandType.UPDATE_NODE_COUNTS_COMMAND);
        channel.putInt(command.labelId()).putLong(command.delta());
    }

    @Override
    public void writeRelationshipCountsCommand(WritableChannel channel, Command.RelationshipCountsCommand command)
            throws IOException {
        channel.put(NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND);
        channel.putInt(command.startLabelId())
                .putInt(command.typeId())
                .putInt(command.endLabelId())
                .putLong(command.delta());
    }

    private static void writeDynamicRecords(WritableChannel channel, List<DynamicRecord> records) throws IOException {
        var size = records.size();
        channel.putInt(size); // 4
        for (int i = 0; i < size; i++) {
            writeDynamicRecord(channel, records.get(i));
        }
    }

    private static void writeDynamicRecord(WritableChannel channel, DynamicRecord record) throws IOException {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if (record.inUse()) {
            byte flags = bitFlags(
                    bitFlag(true, Record.IN_USE.byteValue()),
                    bitFlag(record.isCreated(), Record.CREATED_IN_TX),
                    bitFlag(record.isStartRecord(), Record.DYNAMIC_RECORD_START_RECORD));
            channel.putLong(record.getId())
                    .putInt(record.getTypeAsInt())
                    .put(flags)
                    .putInt(record.getLength())
                    .putLong(record.getNextBlock());
            byte[] data = record.getData();
            assert data != null;
            channel.put(data, data.length);
        } else {
            channel.putLong(record.getId()).putInt(record.getTypeAsInt()).put(Record.NOT_IN_USE.byteValue());
        }
    }

    private static DynamicRecord readDynamicRecord(ReadableChannel channel) throws IOException {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        long id = channel.getLong();
        assert id >= 0 : id + " is not a valid dynamic record id";
        int type = channel.getInt();
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean created = bitFlag(flags, Record.CREATED_IN_TX);
        DynamicRecord record = new DynamicRecord(id);
        record.setInUse(inUse, type);
        if (inUse) {
            record.setStartRecord(bitFlag(flags, Record.DYNAMIC_RECORD_START_RECORD));
            record.setCreated(created);
            int nrOfBytes = channel.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ((1 << 24) - 1)
                    : nrOfBytes + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = channel.getLong();
            assert (nextBlock >= 0) || (nextBlock == Record.NO_NEXT_BLOCK.intValue())
                    : nextBlock + " is not valid for a next record field of a dynamic record";
            record.setNextBlock(nextBlock);
            byte[] data = new byte[nrOfBytes];
            channel.get(data, nrOfBytes);
            record.setData(data);
        }
        return record;
    }

    private static void readDynamicRecords(ReadableChannel channel, Consumer<DynamicRecord> recordConsumer)
            throws IOException {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        while (numberOfRecords > 0) {
            DynamicRecord read = readDynamicRecord(channel);
            recordConsumer.accept(read);
            numberOfRecords--;
        }
    }

    private static void readDynamicRecordList(ReadableChannel channel, Consumer<List<DynamicRecord>> recordConsumer)
            throws IOException {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        if (numberOfRecords > 0) {
            var records = new ArrayList<DynamicRecord>(numberOfRecords);
            while (numberOfRecords > 0) {
                records.add(readDynamicRecord(channel));
                numberOfRecords--;
            }
            recordConsumer.accept(records);
        }
    }

    private static long[] readLongs(ReadableChannel channel, int count) throws IOException {
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            result[i] = channel.getLong();
        }
        return result;
    }

    @Override
    public void writeDeletedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        writeNodeCommand(channel, command);
    }

    @Override
    public void writeCreatedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        writeNodeCommand(channel, command);
    }

    @Override
    public void writeCreatedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        writeRelationshipCommand(channel, command);
    }

    @Override
    public void writeDeletedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        writeRelationshipCommand(channel, command);
    }

    @Override
    public void writeCreatedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        writePropertyCommand(channel, command);
    }

    @Override
    public void writeDeletedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        writePropertyCommand(channel, command);
    }
}
