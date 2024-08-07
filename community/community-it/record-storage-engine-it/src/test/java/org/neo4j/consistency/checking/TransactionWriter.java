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
package org.neo4j.consistency.checking;

import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.TokenStore.NAME_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_PREV_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.LatestVersions;

public class TransactionWriter {
    private final NeoStores neoStores;

    private final List<Command.NodeCommand> nodeCommands = new ArrayList<>();
    private final List<Command.RelationshipCommand> relationshipCommands = new ArrayList<>();
    private final List<Command.RelationshipGroupCommand> relationshipGroupCommands = new ArrayList<>();
    private final List<Command> otherCommands = new ArrayList<>();
    private final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    public TransactionWriter(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    public CommandBatch representation(long consensusIndex, long startTime, long lastCommittedTx, long committedTime) {
        prepareForCommit();
        return new CompleteCommandBatch(
                allCommands(),
                consensusIndex,
                startTime,
                lastCommittedTx,
                committedTime,
                -1,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
    }

    public void propertyKey(int id, String key, boolean internal, int... dynamicIds) {
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(id);
        PropertyKeyTokenRecord after = withName(new PropertyKeyTokenRecord(id), dynamicIds, key);
        after.setInternal(internal);
        otherCommands.add(new Command.PropertyKeyTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void label(int id, String name, boolean internal, int... dynamicIds) {
        LabelTokenRecord before = new LabelTokenRecord(id);
        LabelTokenRecord after = withName(new LabelTokenRecord(id), dynamicIds, name);
        after.setInternal(internal);
        otherCommands.add(new Command.LabelTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void relationshipType(int id, String relType, boolean internal, int... dynamicIds) {
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(id);
        RelationshipTypeTokenRecord after = withName(new RelationshipTypeTokenRecord(id), dynamicIds, relType);
        after.setInternal(internal);
        otherCommands.add(new Command.RelationshipTypeTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void update(LabelTokenRecord before, LabelTokenRecord after) {
        otherCommands.add(new Command.LabelTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void create(NodeRecord node) {
        node.setCreated();
        update(
                new NodeRecord(node.getId())
                        .initialize(false, NO_NEXT_PROPERTY.intValue(), false, NO_PREV_RELATIONSHIP.intValue(), 0),
                node);
    }

    public void create(LabelTokenRecord labelToken) {
        labelToken.setCreated();
        update(new LabelTokenRecord(labelToken.getIntId()), labelToken);
    }

    public void create(PropertyKeyTokenRecord token) {
        token.setCreated();
        update(new PropertyKeyTokenRecord(token.getIntId()), token);
    }

    public void create(RelationshipGroupRecord group) {
        group.setCreated();
        update(
                new RelationshipGroupRecord(group.getId())
                        .initialize(
                                false,
                                group.getType(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue()),
                group);
    }

    public void update(NodeRecord before, NodeRecord node) {
        node.setInUse(true);
        add(before, node);
    }

    public void update(PropertyKeyTokenRecord before, PropertyKeyTokenRecord after) {
        after.setInUse(true);
        add(before, after);
    }

    public void delete(NodeRecord node) {
        node.setInUse(false);
        add(
                node,
                new NodeRecord(node.getId())
                        .initialize(false, NO_NEXT_PROPERTY.intValue(), false, NO_PREV_RELATIONSHIP.intValue(), 0));
    }

    public void create(RelationshipRecord record) {
        record.setCreated();
        update(new RelationshipRecord(record.getId()), record);
    }

    public void delete(RelationshipGroupRecord group) {
        group.setInUse(false);
        add(
                group,
                new RelationshipGroupRecord(group.getId())
                        .initialize(
                                false,
                                group.getType(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue()));
    }

    public void createSchema(SchemaRecord before, SchemaRecord after, SchemaRule rule) {
        after.setCreated();
        updateSchema(before, after, rule);
    }

    public void updateSchema(SchemaRecord before, SchemaRecord after, SchemaRule rule) {
        after.setInUse(true);
        addSchema(before, after, rule);
    }

    public void update(RelationshipRecord before, RelationshipRecord after) {
        after.setInUse(true);
        add(before, after);
    }

    public void update(RelationshipGroupRecord before, RelationshipGroupRecord after) {
        after.setInUse(true);
        add(before, after);
    }

    public void delete(RelationshipRecord record) {
        record.setInUse(false);
        add(record, new RelationshipRecord(record.getId()));
    }

    public void create(PropertyRecord property) {
        property.setCreated();
        PropertyRecord before = new PropertyRecord(property.getId());
        if (property.isNodeSet()) {
            before.setNodeId(property.getNodeId());
        }
        if (property.isRelSet()) {
            before.setRelId(property.getRelId());
        }
        if (property.isSchemaSet()) {
            before.setSchemaRuleId(property.getSchemaRuleId());
        }
        update(before, property);
    }

    public void update(PropertyRecord before, PropertyRecord after) {
        after.setInUse(true);
        add(before, after);
    }

    public void delete(PropertyRecord before, PropertyRecord after) {
        after.setInUse(false);
        add(before, after);
    }

    // Internals

    private void addSchema(SchemaRecord before, SchemaRecord after, SchemaRule rule) {
        otherCommands.add(new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule));
    }

    public void add(NodeRecord before, NodeRecord after) {
        nodeCommands.add(new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void add(RelationshipRecord before, RelationshipRecord after) {
        relationshipCommands.add(new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void add(RelationshipGroupRecord before, RelationshipGroupRecord after) {
        relationshipGroupCommands.add(new Command.RelationshipGroupCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void add(PropertyRecord before, PropertyRecord property) {
        otherCommands.add(new Command.PropertyCommand(LATEST_LOG_SERIALIZATION, before, property));
    }

    public void add(RelationshipTypeTokenRecord before, RelationshipTypeTokenRecord after) {
        otherCommands.add(new Command.RelationshipTypeTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void add(PropertyKeyTokenRecord before, PropertyKeyTokenRecord after) {
        otherCommands.add(new Command.PropertyKeyTokenCommand(LATEST_LOG_SERIALIZATION, before, after));
    }

    public void incrementNodeCount(int labelId, long delta) {
        otherCommands.add(new Command.NodeCountsCommand(LATEST_LOG_SERIALIZATION, labelId, delta));
    }

    public void incrementRelationshipCount(int startLabelId, int typeId, int endLabelId, long delta) {
        otherCommands.add(new Command.RelationshipCountsCommand(
                LATEST_LOG_SERIALIZATION, startLabelId, typeId, endLabelId, delta));
    }

    private static <T extends TokenRecord> T withName(T record, int[] dynamicIds, String name) {
        if (dynamicIds == null || dynamicIds.length == 0) {
            throw new IllegalArgumentException("No dynamic records for storing the name.");
        }
        record.setInUse(true);
        byte[] data = PropertyStore.encodeString(name);
        if (data.length > dynamicIds.length * NAME_STORE_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                    String.format("[%s] is too long to fit in %d blocks", name, dynamicIds.length));
        } else if (data.length <= (dynamicIds.length - 1) * NAME_STORE_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                    String.format("[%s] is to short to fill %d blocks", name, dynamicIds.length));
        }

        for (int i = 0; i < dynamicIds.length; i++) {
            byte[] part = new byte[Math.min(NAME_STORE_BLOCK_SIZE, data.length - i * NAME_STORE_BLOCK_SIZE)];
            System.arraycopy(data, i * NAME_STORE_BLOCK_SIZE, part, 0, part.length);

            DynamicRecord dynamicRecord = new DynamicRecord(dynamicIds[i]);
            dynamicRecord.setInUse(true);
            dynamicRecord.setData(part);
            dynamicRecord.setCreated();
            // Link the next block
            if (i + 1 < dynamicIds.length) {
                dynamicRecord.setNextBlock(dynamicIds[i + 1]);
            }
            record.addNameRecord(dynamicRecord);
        }
        record.setNameId(dynamicIds[0]);
        return record;
    }

    private void prepareForCommit() {
        var nodeStore = neoStores.getNodeStore();
        for (Command.NodeCommand command : nodeCommands) {
            nodeStore.prepareForCommit(command.getAfter(), nodeStore.getIdGenerator(), NULL_CONTEXT);
        }

        var relationshipStore = neoStores.getRelationshipStore();
        for (Command.RelationshipCommand command : relationshipCommands) {
            relationshipStore.prepareForCommit(command.getAfter(), relationshipStore.getIdGenerator(), NULL_CONTEXT);
        }

        var relationshipGroupStore = neoStores.getRelationshipGroupStore();
        for (Command.RelationshipGroupCommand command : relationshipGroupCommands) {
            relationshipGroupStore.prepareForCommit(
                    command.getAfter(), relationshipGroupStore.getIdGenerator(), NULL_CONTEXT);
        }
    }

    private List<StorageCommand> allCommands() {
        List<StorageCommand> allCommands = new ArrayList<>();
        allCommands.addAll(nodeCommands);
        allCommands.addAll(relationshipCommands);
        allCommands.addAll(relationshipGroupCommands);
        allCommands.addAll(otherCommands);
        return allCommands;
    }
}
