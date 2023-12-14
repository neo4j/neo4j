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

import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.combinedKeyOnGroupAndDirection;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;
import static org.neo4j.lock.ResourceType.RELATIONSHIP_GROUP;
import static org.neo4j.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command.Mode;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.recordstorage.id.IdSequenceProvider;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

/**
 * Transaction containing {@link Command commands} reflecting the operations performed in the transaction.
 * <p>
 * This class currently has a symbiotic relationship with a transaction, with which it always has a 1-1 relationship.
 * <p>
 * The idea here is that KernelTransaction will eventually take on the responsibilities of WriteTransaction, such as
 * keeping track of transaction state, serialization and deserialization to and from logical log, and applying things
 * to store. It would most likely do this by keeping a component derived from the current WriteTransaction
 * implementation as a sub-component, responsible for handling logical log commands.
 */
public class TransactionRecordState implements RecordState {
    private static final CommandComparator COMMAND_COMPARATOR = new CommandComparator();
    private static final Command[] EMPTY_COMMANDS = new Command[0];
    private static final Function<Mode, List<Command>> MODE_TO_ARRAY_LIST = mode -> new ArrayList<>();

    private final KernelVersionProvider kernelVersionProvider;
    private final NeoStores neoStores;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final RecordAccessSet recordChangeSet;
    private final ResourceLocker locks;
    private final LockTracer lockTracer;
    private final RelationshipModifier relationshipModifier;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final MemoryTracker memoryTracker;
    private final LogCommandSerialization commandSerialization;
    private final DynamicAllocatorProvider dynamicAllocators;
    private final IdSequenceProvider transactionIdSequenceProvider;
    private final DegreesUpdater groupDegreesUpdater = new DegreesUpdater();

    private boolean prepared;
    private final RelationshipGroupGetter.DirectGroupLookup directGroupLookup;
    private Upgrade.KernelUpgrade upgrade;

    TransactionRecordState(
            KernelVersionProvider kernelVersionProvider,
            RecordChangeSet recordChangeSet,
            NeoStores neoStores,
            ResourceLocker locks,
            LockTracer lockTracer,
            RelationshipModifier relationshipModifier,
            PropertyCreator propertyCreator,
            PropertyDeleter propertyDeleter,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker,
            LogCommandSerialization commandSerialization,
            DynamicAllocatorProvider dynamicAllocators,
            IdSequenceProvider transactionIdSequenceProvider) {
        this.kernelVersionProvider = kernelVersionProvider;
        this.neoStores = neoStores;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.recordChangeSet = recordChangeSet;
        this.locks = locks;
        this.lockTracer = lockTracer;
        this.relationshipModifier = relationshipModifier;
        this.propertyCreator = propertyCreator;
        this.propertyDeleter = propertyDeleter;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
        this.commandSerialization = commandSerialization;
        this.dynamicAllocators = dynamicAllocators;
        this.transactionIdSequenceProvider = transactionIdSequenceProvider;
        this.directGroupLookup = new RelationshipGroupGetter.DirectGroupLookup(recordChangeSet, cursorContext);
    }

    @Override
    public void extractCommands(Collection<StorageCommand> commands, MemoryTracker memoryTracker)
            throws TransactionFailureException {
        assert !prepared : "Transaction has already been prepared";

        int noOfCommands = recordChangeSet.changeSize();

        var labelTokenChanges = recordChangeSet.getLabelTokenChanges().changes();
        memoryTracker.allocateHeap(labelTokenChanges.size() * Command.LabelTokenCommand.HEAP_SIZE);
        for (RecordProxy<LabelTokenRecord, Void> record : labelTokenChanges) {
            commands.add(new Command.LabelTokenCommand(
                    commandSerialization, record.getBefore(), record.forReadingLinkage()));
        }

        var relationshipTypeTokenChanges =
                recordChangeSet.getRelationshipTypeTokenChanges().changes();
        memoryTracker.allocateHeap(
                relationshipTypeTokenChanges.size() * Command.RelationshipTypeTokenCommand.HEAP_SIZE);
        for (RecordProxy<RelationshipTypeTokenRecord, Void> record : relationshipTypeTokenChanges) {
            commands.add(new Command.RelationshipTypeTokenCommand(
                    commandSerialization, record.getBefore(), record.forReadingLinkage()));
        }

        var propertyKeyTokenChanges =
                recordChangeSet.getPropertyKeyTokenChanges().changes();
        memoryTracker.allocateHeap(propertyKeyTokenChanges.size() * Command.PropertyKeyTokenCommand.HEAP_SIZE);
        for (RecordProxy<PropertyKeyTokenRecord, Void> record : propertyKeyTokenChanges) {
            commands.add(new Command.PropertyKeyTokenCommand(
                    commandSerialization, record.getBefore(), record.forReadingLinkage()));
        }

        // Collect nodes, relationships, properties
        Command[] nodeCommands = EMPTY_COMMANDS;
        int skippedCommands = 0;
        var nodeChanges = recordChangeSet.getNodeRecords().changes();
        if (!nodeChanges.isEmpty()) {
            memoryTracker.allocateHeap(nodeChanges.size() * Command.NodeCommand.HEAP_SIZE);
            nodeCommands = new Command[nodeChanges.size()];
            int i = 0;
            IdSequence nodeIdSequence = transactionIdSequenceProvider.getIdSequence(StoreType.NODE);
            for (RecordProxy<NodeRecord, Void> change : nodeChanges) {
                NodeRecord record = prepared(change, nodeIdSequence, nodeStore);
                IntegrityValidator.validateNodeRecord(record);
                nodeCommands[i++] = new Command.NodeCommand(commandSerialization, change.getBefore(), record);
            }
            Arrays.sort(nodeCommands, COMMAND_COMPARATOR);
        }

        Command[] relCommands = EMPTY_COMMANDS;
        var relationshipChanges = recordChangeSet.getRelRecords().changes();
        if (!relationshipChanges.isEmpty()) {
            memoryTracker.allocateHeap(relationshipChanges.size() * Command.RelationshipCommand.HEAP_SIZE);
            relCommands = new Command[relationshipChanges.size()];
            int i = 0;
            IdSequence relIdSequence = transactionIdSequenceProvider.getIdSequence(StoreType.RELATIONSHIP);
            for (RecordProxy<RelationshipRecord, Void> change : relationshipChanges) {
                relCommands[i++] = new Command.RelationshipCommand(
                        commandSerialization, change.getBefore(), prepared(change, relIdSequence, relationshipStore));
            }
            Arrays.sort(relCommands, COMMAND_COMPARATOR);
        }

        Command[] propCommands = EMPTY_COMMANDS;
        var propertyChanges = recordChangeSet.getPropertyRecords().changes();
        if (!propertyChanges.isEmpty()) {
            memoryTracker.allocateHeap(propertyChanges.size() * Command.PropertyCommand.HEAP_SIZE);
            propCommands = new Command[propertyChanges.size()];
            int i = 0;
            IdSequence propertyIdSequence = transactionIdSequenceProvider.getIdSequence(StoreType.PROPERTY);
            for (RecordProxy<PropertyRecord, PrimitiveRecord> change : propertyChanges) {
                propCommands[i++] = new Command.PropertyCommand(
                        commandSerialization, change.getBefore(), prepared(change, propertyIdSequence, propertyStore));
            }
            Arrays.sort(propCommands, COMMAND_COMPARATOR);
        }

        Command[] relGroupCommands = EMPTY_COMMANDS;
        var relationshipGroupChanges = recordChangeSet.getRelGroupRecords().changes();
        if (!relationshipGroupChanges.isEmpty()) {
            memoryTracker.allocateHeap(relationshipGroupChanges.size() * Command.RelationshipGroupCommand.HEAP_SIZE);
            relGroupCommands = new Command[relationshipGroupChanges.size()];
            int i = 0;
            IdSequence relGroupSequence = transactionIdSequenceProvider.getIdSequence(StoreType.RELATIONSHIP_GROUP);
            for (RecordProxy<RelationshipGroupRecord, Integer> change : relationshipGroupChanges) {
                if (change.isCreated() && !change.forReadingLinkage().inUse()) {
                    /*
                     * This is an edge case that may come up and which we must handle properly. Relationship groups are
                     * not managed by the tx state, since they are created as side effects rather than through
                     * direct calls. However, they differ from say, dynamic records, in that their management can happen
                     * through separate code paths. What we are interested in here is the following scenario.
                     * 0. A node has one less relationship that is required to transition to dense node. The relationships
                     *    it has belong to at least two different types
                     * 1. In the same tx, a relationship is added making the node dense and all the relationships of a type
                     *    are removed from that node. Regardless of the order these operations happen, the creation of the
                     *    relationship (and the transition of the node to dense) will happen first.
                     * 2. A relationship group will be created because of the transition to dense and then deleted because
                     *    all the relationships it would hold are no longer there. This results in a relationship group
                     *    command that appears in the tx as not in use. Depending on the final order of operations, this
                     *    can end up using an id that is higher than the highest id seen so far. This may not be a problem
                     *    for a single instance, but it can result in errors in cases where transactions are applied
                     *    externally, such as backup.
                     *
                     * The way we deal with this issue here is by not issuing a command for that offending record. This is
                     * safe, since the record is not in use and never was, so the high id is not necessary to change and
                     * the store remains consistent.
                     */
                    skippedCommands++;
                    continue;
                }
                relGroupCommands[i++] = new Command.RelationshipGroupCommand(
                        commandSerialization,
                        change.getBefore(),
                        prepared(change, relGroupSequence, relationshipGroupStore));
            }
            relGroupCommands = i < relGroupCommands.length ? Arrays.copyOf(relGroupCommands, i) : relGroupCommands;
            Arrays.sort(relGroupCommands, COMMAND_COMPARATOR);
        }

        addFiltered(commands, Mode.CREATE, propCommands, relCommands, relGroupCommands, nodeCommands);
        addFiltered(commands, Mode.UPDATE, propCommands, relCommands, relGroupCommands, nodeCommands);
        addFiltered(commands, Mode.DELETE, relCommands, relGroupCommands, nodeCommands);

        EnumMap<Mode, List<Command>> schemaChangeByMode = new EnumMap<>(Mode.class);
        var schemaRuleChange = recordChangeSet.getSchemaRuleChanges().changes();
        memoryTracker.allocateHeap(schemaRuleChange.size() * Command.SchemaRuleCommand.HEAP_SIZE);
        for (RecordProxy<SchemaRecord, SchemaRule> change : schemaRuleChange) {
            SchemaRecord schemaRecord = change.forReadingLinkage();
            SchemaRule rule = change.getAdditionalData();
            if (schemaRecord.inUse()) {
                IntegrityValidator.validateSchemaRule(rule, kernelVersionProvider.kernelVersion());
            }
            Command.SchemaRuleCommand cmd = new Command.SchemaRuleCommand(
                    commandSerialization, change.getBefore(), change.forChangingData(), rule);
            schemaChangeByMode
                    .computeIfAbsent(cmd.getMode(), MODE_TO_ARRAY_LIST)
                    .add(cmd);
        }

        commands.addAll(schemaChangeByMode.getOrDefault(Mode.DELETE, Collections.emptyList()));
        commands.addAll(schemaChangeByMode.getOrDefault(Mode.CREATE, Collections.emptyList()));
        commands.addAll(schemaChangeByMode.getOrDefault(Mode.UPDATE, Collections.emptyList()));

        // Add deleted property commands last, so they happen after the schema record changes.
        // This extends the lifetime of property records just past the last moment of use,
        // and prevents reading and deleting of schema records from racing, and making the
        // schema records look malformed.
        addFiltered(commands, Mode.DELETE, propCommands);

        assert commands.size() == noOfCommands - skippedCommands
                : format(
                        "Expected %d final commands, got %d " + "instead, with %d skipped",
                        noOfCommands, commands.size(), skippedCommands);
        if (groupDegreesUpdater.degrees != null) {
            memoryTracker.allocateHeap(groupDegreesUpdater.degrees.size() * Command.GroupDegreeCommand.SHALLOW_SIZE);
            groupDegreesUpdater.degrees.forEachKeyValue((key, delta) -> {
                if (delta.longValue() != 0) {
                    long groupId = Command.GroupDegreeCommand.groupIdFromCombinedKey(key);
                    RelationshipDirection direction = Command.GroupDegreeCommand.directionFromCombinedKey(key);
                    commands.add(new Command.GroupDegreeCommand(
                            commandSerialization, groupId, direction, delta.longValue()));
                }
            });
        }

        if (upgrade != null) {
            MetaDataRecord before = new MetaDataRecord();
            before.initialize(true, upgrade.from().version());
            MetaDataRecord after = new MetaDataRecord();
            after.initialize(true, upgrade.to().version());
            // This command will be the last one in the "old" version, indicating the switch and writing it to the
            // KernelVersionRepository. The KernelVersionRepository update will make the transaction that triggered
            // upgrade be written in the "new" version
            commands.add(new Command.MetaDataCommand(commandSerialization, before, after));
        }

        prepared = true;
    }

    private <RECORD extends AbstractBaseRecord> RECORD prepared(
            RecordProxy<RECORD, ?> proxy, IdSequence idSequence, RecordStore<RECORD> store) {
        RECORD after = proxy.forReadingLinkage();
        store.prepareForCommit(after, idSequence, cursorContext);
        return after;
    }

    void relModify(RelationshipModifications modifications) {
        relationshipModifier.modifyRelationships(modifications, recordChangeSet, groupDegreesUpdater);
    }

    private static void addFiltered(Collection<StorageCommand> target, Mode mode, Command[]... commands) {
        for (Command[] c : commands) {
            for (Command command : c) {
                if (command.getMode() == mode) {
                    target.add(command);
                }
            }
        }
    }

    /**
     * Deletes a node by its id, returning its properties which are now removed.
     *
     * @param nodeId The id of the node to delete.
     */
    public void nodeDelete(long nodeId) {
        RecordProxy<NodeRecord, Void> nodeChange =
                recordChangeSet.getNodeRecords().getOrLoad(nodeId, null);
        NodeRecord nodeRecord = nodeChange.forChangingData();
        if (!nodeRecord.inUse()) {
            throw new IllegalStateException("Unable to delete Node[" + nodeId + "] since it has already been deleted.");
        }
        if (nodeRecord.isDense()) {
            RelationshipGroupGetter.deleteEmptyGroups(
                    nodeChange,
                    g -> {
                        // This lock make be taken out-of-order but we have NODE_RELATIONSHIP_GROUP_DELETE exclusive. No
                        // concurrent transaction using this node exists.
                        locks.acquireExclusive(
                                lockTracer,
                                RELATIONSHIP_GROUP,
                                nodeId); // We may take this lock multiple times but that's so rare we don't care.
                        return true;
                    },
                    directGroupLookup);
        }
        nodeRecord.setInUse(false);
        nodeRecord.setLabelField(Record.NO_LABELS_FIELD.intValue(), markNotInUse(nodeRecord.getDynamicLabelRecords()));
        getAndDeletePropertyChain(nodeRecord);
    }

    private static List<DynamicRecord> markNotInUse(List<DynamicRecord> dynamicLabelRecords) {
        for (DynamicRecord record : dynamicLabelRecords) {
            record.setInUse(false);
        }
        return dynamicLabelRecords;
    }

    private void getAndDeletePropertyChain(PrimitiveRecord record) {
        propertyDeleter.deletePropertyChain(record, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    /**
     * Removes the given property identified by its index from the relationship
     * with the given id.
     *
     * @param relId The id of the relationship that is to have the property removed.
     * @param propertyKey The index key of the property.
     */
    void relRemoveProperty(long relId, int propertyKey) {
        RecordProxy<RelationshipRecord, Void> rel =
                recordChangeSet.getRelRecords().getOrLoad(relId, null);
        propertyDeleter.removeProperty(rel, propertyKey, recordChangeSet.getPropertyRecords());
    }

    /**
     * Removes the given property identified by indexKeyId of the node with the
     * given id.
     *
     * @param nodeId The id of the node that is to have the property removed.
     * @param propertyKey The index key of the property.
     */
    public void nodeRemoveProperty(long nodeId, int propertyKey) {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad(nodeId, null);
        propertyDeleter.removeProperty(node, propertyKey, recordChangeSet.getPropertyRecords());
    }

    /**
     * Changes an existing property's value of the given relationship, with the
     * given index to the passed value
     * @param relId The id of the relationship which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    void relChangeProperty(long relId, int propertyKey, Value value) {
        RecordProxy<RelationshipRecord, Void> rel =
                recordChangeSet.getRelRecords().getOrLoad(relId, null);
        propertyCreator.primitiveSetProperty(
                rel, propertyKey, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     * @param nodeId The id of the node which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    void nodeChangeProperty(long nodeId, int propertyKey, Value value) {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad(nodeId, null);
        propertyCreator.primitiveSetProperty(
                node, propertyKey, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     * @param relId The id of the relationship to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    void relAddProperty(long relId, int propertyKey, Value value) {
        RecordProxy<RelationshipRecord, Void> rel =
                recordChangeSet.getRelRecords().getOrLoad(relId, null);
        propertyCreator.primitiveSetProperty(
                rel, propertyKey, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    /**
     * Adds a property to the given node, with the given index and value.
     * @param nodeId The id of the node to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    void nodeAddProperty(long nodeId, int propertyKey, Value value) {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad(nodeId, null);
        propertyCreator.primitiveSetProperty(
                node, propertyKey, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    void addLabelToNode(int labelId, long nodeId) {
        NodeRecord nodeRecord =
                recordChangeSet.getNodeRecords().getOrLoad(nodeId, null).forChangingData();
        parseLabelsField(nodeRecord)
                .add(
                        labelId,
                        nodeStore,
                        dynamicAllocators.allocator(StoreType.NODE_LABEL),
                        cursorContext,
                        storeCursors,
                        memoryTracker);
    }

    void removeLabelFromNode(int labelId, long nodeId) {
        NodeRecord nodeRecord =
                recordChangeSet.getNodeRecords().getOrLoad(nodeId, null).forChangingData();
        parseLabelsField(nodeRecord)
                .remove(
                        labelId,
                        nodeStore,
                        dynamicAllocators.allocator(StoreType.NODE_LABEL),
                        cursorContext,
                        storeCursors,
                        memoryTracker);
    }

    /**
     * Creates a node for the given id
     *
     * @param nodeId The id of the node to create.
     */
    public void nodeCreate(long nodeId) {
        NodeRecord nodeRecord = recordChangeSet
                .getNodeRecords()
                .create(nodeId, null, cursorContext)
                .forChangingData();
        nodeRecord.setInUse(true);
        nodeRecord.setCreated();
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param key The key of the property index, as a string.
     * @param id The property index record id.
     */
    void createPropertyKeyToken(String key, long id, boolean internal) {
        createToken(
                neoStores.getPropertyKeyTokenStore(),
                key,
                id,
                internal,
                recordChangeSet.getPropertyKeyTokenChanges(),
                dynamicAllocators.allocator(StoreType.PROPERTY_KEY_TOKEN_NAME),
                cursorContext,
                memoryTracker);
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param name The key of the property index, as a string.
     * @param id The property index record id.
     */
    void createLabelToken(String name, long id, boolean internal) {
        createToken(
                neoStores.getLabelTokenStore(),
                name,
                id,
                internal,
                recordChangeSet.getLabelTokenChanges(),
                dynamicAllocators.allocator(StoreType.LABEL_TOKEN_NAME),
                cursorContext,
                memoryTracker);
    }

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param name The name of the relationship type.
     * @param id The id of the new relationship type record.
     */
    void createRelationshipTypeToken(String name, long id, boolean internal) {
        createToken(
                neoStores.getRelationshipTypeTokenStore(),
                name,
                id,
                internal,
                recordChangeSet.getRelationshipTypeTokenChanges(),
                dynamicAllocators.allocator(StoreType.RELATIONSHIP_TYPE_TOKEN_NAME),
                cursorContext,
                memoryTracker);
    }

    private static <R extends TokenRecord> void createToken(
            TokenStore<R> store,
            String name,
            long id,
            boolean internal,
            RecordAccess<R, Void> recordAccess,
            DynamicRecordAllocator nameStoreRecordAllocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        R record = recordAccess.create(id, null, cursorContext).forChangingData();
        record.setInUse(true);
        record.setInternal(internal);
        record.setCreated();
        Collection<DynamicRecord> nameRecords =
                store.allocateNameRecords(encodeString(name), nameStoreRecordAllocator, cursorContext, memoryTracker);
        record.setNameId((int) Iterables.first(nameRecords).getId());
        record.addNameRecords(nameRecords);
    }

    void upgrade(Upgrade.KernelUpgrade kernelUpgrade) {
        checkState(
                kernelUpgrade.to().isGreaterThan(kernelUpgrade.from()),
                "Can not downgrade from %s to %s",
                kernelUpgrade.from(),
                kernelUpgrade.to());

        this.upgrade = kernelUpgrade;
    }

    private static class CommandComparator implements Comparator<Command> {
        @Override
        public int compare(Command o1, Command o2) {
            long id1 = o1.getKey();
            long id2 = o2.getKey();
            return Long.compare(id1, id2);
        }
    }

    void schemaRuleCreate(long ruleId, boolean isConstraint, SchemaRule rule) {
        SchemaRecord record = recordChangeSet
                .getSchemaRuleChanges()
                .create(ruleId, rule, cursorContext)
                .forChangingData();
        record.setInUse(true);
        record.setCreated();
        record.setConstraint(isConstraint);
    }

    void schemaRuleDelete(long ruleId, SchemaRule rule) {
        // Index schema rules may be deleted twice, if they were owned by a constraint; once for dropping the index, and
        // then again as part of
        // dropping the constraint. So we keep this method idempotent.
        RecordProxy<SchemaRecord, SchemaRule> proxy =
                recordChangeSet.getSchemaRuleChanges().getOrLoad(ruleId, rule, RecordLoad.CHECK);
        SchemaRecord record = proxy.forReadingData();
        if (record.inUse()) {
            record = proxy.forChangingData();
            record.setInUse(false);
            getAndDeletePropertyChain(record);
        }
    }

    void schemaRuleSetProperty(long ruleId, int propertyKeyId, Value value, SchemaRule rule) {
        RecordProxy<SchemaRecord, SchemaRule> record =
                recordChangeSet.getSchemaRuleChanges().getOrLoad(ruleId, rule);
        propertyCreator.primitiveSetProperty(
                record, propertyKeyId, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    void schemaRuleSetIndexOwner(IndexDescriptor rule, long constraintId, int propertyKeyId, Value value) {
        // It is possible that the added property will only modify the property chain and leave the owning record
        // untouched.
        // However, we need the schema record to be marked as updated so that an UPDATE schema command is generated.
        // Otherwise, the command appliers, who are responsible for activating index proxies and clearing the schema
        // cache,
        // will not notice our change.
        long ruleId = rule.getId();
        rule = rule.withOwningConstraintId(constraintId);
        RecordAccess<SchemaRecord, SchemaRule> changes = recordChangeSet.getSchemaRuleChanges();
        RecordProxy<SchemaRecord, SchemaRule> record = changes.getOrLoad(ruleId, rule);
        changes.setRecord(ruleId, record.forReadingData(), rule, cursorContext).forChangingData();
        propertyCreator.primitiveSetProperty(
                record, propertyKeyId, value, recordChangeSet.getPropertyRecords(), memoryTracker);
    }

    @VisibleForTesting
    Long groupDegreeDelta(long groupId, RelationshipDirection direction) {
        if (groupDegreesUpdater.degrees != null) {
            MutableLong delta = groupDegreesUpdater.degrees.get(combinedKeyOnGroupAndDirection(groupId, direction));
            if (delta != null) {
                return delta.getValue();
            }
        }
        return null;
    }

    private static class DegreesUpdater implements DegreeUpdater {
        private MutableLongObjectMap<MutableLong> degrees;

        @Override
        public void increment(long groupId, RelationshipDirection direction, long delta) {
            if (degrees == null) {
                degrees = LongObjectMaps.mutable.empty();
            }
            degrees.getIfAbsentPut(combinedKeyOnGroupAndDirection(groupId, direction), MutableLong::new)
                    .add(delta);
        }

        @Override
        public void close() {}
    }
}
