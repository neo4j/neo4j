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

import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;

import java.io.IOException;
import java.util.ArrayList;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

class LogCommandSerializationV5_11 extends LogCommandSerializationV5_10 {
    static final LogCommandSerializationV5_11 INSTANCE = new LogCommandSerializationV5_11();

    @Override
    public void writeCreatedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        channel.put(NeoCommandType.CREATE_NODE_COMMAND);
        channel.putLong(command.getAfter().getId());
        writeNodeRecord(channel, command.getAfter());
    }

    @Override
    public void writeDeletedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        channel.put(NeoCommandType.DELETE_NODE_COMMAND);
        channel.putLong(command.getBefore().getId());
        writeNodeRecord(channel, command.getBefore());
    }

    @Override
    public void writeDeletedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        channel.put(NeoCommandType.DELETE_PROP_COMMAND);
        channel.putLong(command.getBefore().getId());
        writePropertyRecord(channel, command.getBefore());
    }

    @Override
    public void writeCreatedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        channel.put(NeoCommandType.CREATE_PROP_COMMAND);
        channel.putLong(command.getAfter().getId());
        writePropertyRecord(channel, command.getAfter());
    }

    @Override
    public void writeCreatedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        channel.put(NeoCommandType.CREATE_REL_COMMAND);
        channel.putLong(command.getAfter().getId());
        writeRelationshipRecord(channel, command.getAfter());
    }

    @Override
    public void writeDeletedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        channel.put(NeoCommandType.DELETE_REL_COMMAND);
        channel.putLong(command.getBefore().getId());
        writeRelationshipRecord(channel, command.getBefore());
    }

    @Override
    protected Command readDeletedNodeCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        NodeRecord before = readNodeRecord(id, channel);
        NodeRecord after = new NodeRecord(before.getId());

        var beforeRecords = before.getDynamicLabelRecords();
        var afterRecords = new ArrayList<DynamicRecord>(beforeRecords.size());
        for (DynamicRecord beforeRecord : beforeRecords) {
            DynamicRecord dynamicRecord = new DynamicRecord(beforeRecord);
            dynamicRecord.setInUse(false);
            afterRecords.add(dynamicRecord);
        }
        after.setLabelField(NO_LABELS_FIELD.longValue(), afterRecords);

        return new Command.NodeCommand(this, before, after);
    }

    @Override
    protected Command readCreatedNodeCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        NodeRecord after = readNodeRecord(id, channel);
        NodeRecord before = new NodeRecord(after.getId());
        before.setInUse(false);

        return new Command.NodeCommand(this, before, after);
    }

    @Override
    protected Command readCreatedPropertyCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        var after = readPropertyRecord(id, channel);

        var before = PropertyRecord.lightCopy(after);
        before.setInUse(false);
        before.setCreated(false);

        return new Command.PropertyCommand(this, before, after);
    }

    @Override
    protected Command readDeletedPropertyCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        var before = readPropertyRecord(id, channel);

        var after = new PropertyRecord(before);
        for (PropertyBlock block : after.propertyBlocks()) {
            for (DynamicRecord valueRecord : block.getValueRecords()) {
                assert valueRecord.inUse();
                valueRecord.setInUse(false);
                after.addDeletedRecord(valueRecord);
            }
        }
        after.clearPropertyBlocks();
        after.setInUse(false);

        return new Command.PropertyCommand(this, before, after);
    }

    @Override
    protected Command readCreatedRelationshipCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        var after = readRelationshipRecord(id, channel);

        var before = new RelationshipRecord(id);
        before.clear();

        return new Command.RelationshipCommand(this, before, after);
    }

    @Override
    protected Command readDeletedRelationshipCommand(ReadableChannel channel) throws IOException {
        long id = channel.getLong();
        var before = readRelationshipRecord(id, channel);

        var after = new RelationshipRecord(id);
        after.clear();

        return new Command.RelationshipCommand(this, before, after);
    }

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.V5_11;
    }
}
