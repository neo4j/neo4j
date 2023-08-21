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

import static org.neo4j.internal.helpers.Numbers.unsignedByteToInt;
import static org.neo4j.internal.helpers.Numbers.unsignedShortToInt;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.directionFromCombinedKey;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.groupIdFromCombinedKey;
import static org.neo4j.util.BitUtils.bitFlag;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

class LogCommandSerializationV4_3_D3 extends LogCommandSerializationV4_2 {
    static final LogCommandSerializationV4_3_D3 INSTANCE = new LogCommandSerializationV4_3_D3();

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.V4_3_D4;
    }

    @Override
    protected Command readMetaDataCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        MetaDataRecord before = readMetaDataRecord(id, channel);
        MetaDataRecord after = readMetaDataRecord(id, channel);
        return new Command.MetaDataCommand(this, before, after);
    }

    private static MetaDataRecord readMetaDataRecord(long id, ReadableChannel channel) throws IOException {
        byte flags = channel.get();
        long value = channel.getLong();
        MetaDataRecord record = new MetaDataRecord();
        record.setId(id);
        if (bitFlag(flags, Record.IN_USE.byteValue())) {
            record.initialize(true, value);
        }
        return record;
    }

    @Override
    protected Command readGroupDegreeCommand(ReadableChannel channel) throws IOException {
        long key = channel.getLong();
        long delta = channel.getLong();
        return new Command.GroupDegreeCommand(this, groupIdFromCombinedKey(key), directionFromCombinedKey(key), delta);
    }

    @Override
    protected Command readRelationshipGroupCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        RelationshipGroupRecord before = readRelationshipGroupRecord(id, channel);
        RelationshipGroupRecord after = readRelationshipGroupRecord(id, channel);

        markAfterRecordAsCreatedIfCommandLooksCreated(before, after);
        return new Command.RelationshipGroupCommand(this, before, after);
    }

    private static RelationshipGroupRecord readRelationshipGroupRecord(long id, ReadableChannel channel)
            throws IOException {
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean requireSecondaryUnit = bitFlag(flags, Record.REQUIRE_SECONDARY_UNIT);
        boolean hasSecondaryUnit = bitFlag(flags, Record.HAS_SECONDARY_UNIT);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean hasExternalDegreesOut = bitFlag(flags, Record.ADDITIONAL_FLAG_1);
        boolean hasExternalDegreesIn = bitFlag(flags, Record.ADDITIONAL_FLAG_2);
        boolean hasExternalDegreesLoop = bitFlag(flags, Record.ADDITIONAL_FLAG_3);

        int type = unsignedShortToInt(channel.getShort());
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        RelationshipGroupRecord record =
                new RelationshipGroupRecord(id).initialize(inUse, type, firstOut, firstIn, firstLoop, owningNode, next);
        record.setHasExternalDegreesOut(hasExternalDegreesOut);
        record.setHasExternalDegreesIn(hasExternalDegreesIn);
        record.setHasExternalDegreesLoop(hasExternalDegreesLoop);
        record.setRequiresSecondaryUnit(requireSecondaryUnit);
        if (hasSecondaryUnit) {
            record.setSecondaryUnitIdOnLoad(channel.getLong());
        }
        record.setUseFixedReferences(usesFixedReferenceFormat);
        return record;
    }

    @Override
    protected Command readRelationshipGroupExtendedCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        RelationshipGroupRecord before = readRelationshipGroupExtendedRecord(id, channel);
        RelationshipGroupRecord after = readRelationshipGroupExtendedRecord(id, channel);

        markAfterRecordAsCreatedIfCommandLooksCreated(before, after);
        return new Command.RelationshipGroupCommand(this, before, after);
    }

    private static RelationshipGroupRecord readRelationshipGroupExtendedRecord(long id, ReadableChannel channel)
            throws IOException {
        byte flags = channel.get();
        boolean inUse = bitFlag(flags, Record.IN_USE.byteValue());
        boolean requireSecondaryUnit = bitFlag(flags, Record.REQUIRE_SECONDARY_UNIT);
        boolean hasSecondaryUnit = bitFlag(flags, Record.HAS_SECONDARY_UNIT);
        boolean usesFixedReferenceFormat = bitFlag(flags, Record.USES_FIXED_REFERENCE_FORMAT);
        boolean hasExternalDegreesOut = bitFlag(flags, Record.ADDITIONAL_FLAG_1);
        boolean hasExternalDegreesIn = bitFlag(flags, Record.ADDITIONAL_FLAG_2);
        boolean hasExternalDegreesLoop = bitFlag(flags, Record.ADDITIONAL_FLAG_3);

        int type = unsignedShortToInt(channel.getShort());
        type |= unsignedByteToInt(channel.get()) << Short.SIZE;
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        RelationshipGroupRecord record =
                new RelationshipGroupRecord(id).initialize(inUse, type, firstOut, firstIn, firstLoop, owningNode, next);
        record.setHasExternalDegreesOut(hasExternalDegreesOut);
        record.setHasExternalDegreesIn(hasExternalDegreesIn);
        record.setHasExternalDegreesLoop(hasExternalDegreesLoop);
        record.setRequiresSecondaryUnit(requireSecondaryUnit);
        if (hasSecondaryUnit) {
            record.setSecondaryUnitIdOnLoad(channel.getLong());
        }
        record.setUseFixedReferences(usesFixedReferenceFormat);
        return record;
    }
}
