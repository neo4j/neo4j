/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Function;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command.Mode;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;

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
public class TransactionRecordState implements RecordState
{
    private static final CommandComparator COMMAND_COMPARATOR = new CommandComparator();
    private static final Command[] EMPTY_COMMANDS = new Command[0];
    private static final Function<Mode,List<Command>> MODE_TO_ARRAY_LIST = mode -> new ArrayList<>();

    private final NeoStores neoStores;
    private final IntegrityValidator integrityValidator;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final RecordAccessSet recordChangeSet;
    private final long lastCommittedTxWhenTransactionStarted;
    private final ResourceLocker locks;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;

    private boolean prepared;

    TransactionRecordState(
            NeoStores neoStores,
            IntegrityValidator integrityValidator,
            RecordChangeSet recordChangeSet,
            long lastCommittedTxWhenTransactionStarted,
            ResourceLocker locks,
            RelationshipCreator relationshipCreator,
            RelationshipDeleter relationshipDeleter,
            PropertyCreator propertyCreator,
            PropertyDeleter propertyDeleter )
    {
        this.neoStores = neoStores;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.integrityValidator = integrityValidator;
        this.recordChangeSet = recordChangeSet;
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.locks = locks;
        this.relationshipCreator = relationshipCreator;
        this.relationshipDeleter = relationshipDeleter;
        this.propertyCreator = propertyCreator;
        this.propertyDeleter = propertyDeleter;
    }

    @Override
    public boolean hasChanges()
    {
        return recordChangeSet.hasChanges();
    }

    @Override
    public void extractCommands( Collection<StorageCommand> commands ) throws TransactionFailureException
    {
        assert !prepared : "Transaction has already been prepared";

        integrityValidator.validateTransactionStartKnowledge( lastCommittedTxWhenTransactionStarted );

        int noOfCommands = recordChangeSet.changeSize();

        for ( RecordProxy<LabelTokenRecord, Void> record : recordChangeSet.getLabelTokenChanges().changes() )
        {
            commands.add( new Command.LabelTokenCommand( record.getBefore(), record.forReadingLinkage() ) );
        }
        for ( RecordProxy<RelationshipTypeTokenRecord, Void> record :
            recordChangeSet.getRelationshipTypeTokenChanges().changes() )
        {
            commands.add( new Command.RelationshipTypeTokenCommand( record.getBefore(), record.forReadingLinkage() ) );
        }
        for ( RecordProxy<PropertyKeyTokenRecord, Void> record :
            recordChangeSet.getPropertyKeyTokenChanges().changes() )
        {
            commands.add( new Command.PropertyKeyTokenCommand( record.getBefore(), record.forReadingLinkage() ) );
        }

        // Collect nodes, relationships, properties
        Command[] nodeCommands = EMPTY_COMMANDS;
        int skippedCommands = 0;
        if ( recordChangeSet.getNodeRecords().changeSize() > 0 )
        {
            nodeCommands = new Command[recordChangeSet.getNodeRecords().changeSize()];
            int i = 0;
            for ( RecordProxy<NodeRecord, Void> change : recordChangeSet.getNodeRecords().changes() )
            {
                NodeRecord record = prepared( change, nodeStore );
                integrityValidator.validateNodeRecord( record );
                nodeCommands[i++] = new Command.NodeCommand( change.getBefore(), record );
            }
            Arrays.sort( nodeCommands, COMMAND_COMPARATOR );
        }

        Command[] relCommands = EMPTY_COMMANDS;
        if ( recordChangeSet.getRelRecords().changeSize() > 0 )
        {
            relCommands = new Command[recordChangeSet.getRelRecords().changeSize()];
            int i = 0;
            for ( RecordProxy<RelationshipRecord, Void> change : recordChangeSet.getRelRecords().changes() )
            {
                relCommands[i++] = new Command.RelationshipCommand( change.getBefore(),
                        prepared( change, relationshipStore ) );
            }
            Arrays.sort( relCommands, COMMAND_COMPARATOR );
        }

        Command[] propCommands = EMPTY_COMMANDS;
        if ( recordChangeSet.getPropertyRecords().changeSize() > 0 )
        {
            propCommands = new Command[recordChangeSet.getPropertyRecords().changeSize()];
            int i = 0;
            for ( RecordProxy<PropertyRecord, PrimitiveRecord> change :
                recordChangeSet.getPropertyRecords().changes() )
            {
                propCommands[i++] = new Command.PropertyCommand( change.getBefore(),
                        prepared( change, propertyStore ) );
            }
            Arrays.sort( propCommands, COMMAND_COMPARATOR );
        }

        Command[] relGroupCommands = EMPTY_COMMANDS;
        if ( recordChangeSet.getRelGroupRecords().changeSize() > 0 )
        {
            relGroupCommands = new Command[recordChangeSet.getRelGroupRecords().changeSize()];
            int i = 0;
            for ( RecordProxy<RelationshipGroupRecord, Integer> change :
                recordChangeSet.getRelGroupRecords().changes() )
            {
                if ( change.isCreated() && !change.forReadingLinkage().inUse() )
                {
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
                relGroupCommands[i++] = new Command.RelationshipGroupCommand( change.getBefore(),
                        prepared( change, relationshipGroupStore ) );
            }
            relGroupCommands = i < relGroupCommands.length ? Arrays.copyOf( relGroupCommands, i ) : relGroupCommands;
            Arrays.sort( relGroupCommands, COMMAND_COMPARATOR );
        }

        addFiltered( commands, Mode.CREATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.UPDATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.DELETE, relCommands, relGroupCommands, nodeCommands );

        EnumMap<Mode,List<Command>> schemaChangeByMode = new EnumMap<>( Mode.class );
        for ( RecordProxy<SchemaRecord,SchemaRule> change : recordChangeSet.getSchemaRuleChanges().changes() )
        {
            SchemaRecord schemaRecord = change.forReadingLinkage();
            SchemaRule rule = change.getAdditionalData();
            if ( schemaRecord.inUse() )
            {
                integrityValidator.validateSchemaRule( rule );
            }
            Command.SchemaRuleCommand cmd = new Command.SchemaRuleCommand( change.getBefore(), change.forChangingData(), rule );
            schemaChangeByMode.computeIfAbsent( cmd.getMode(), MODE_TO_ARRAY_LIST ).add( cmd );
        }

        commands.addAll( schemaChangeByMode.getOrDefault( Mode.DELETE, Collections.emptyList() ) );
        commands.addAll( schemaChangeByMode.getOrDefault( Mode.CREATE, Collections.emptyList() ) );
        commands.addAll( schemaChangeByMode.getOrDefault( Mode.UPDATE, Collections.emptyList() ) );

        // Add deleted property commands last, so they happen after the schema record changes.
        // This extends the lifetime of property records just past the last moment of use,
        // and prevents reading and deleting of schema records from racing, and making the
        // schema records look malformed.
        addFiltered( commands, Mode.DELETE, propCommands );

        assert commands.size() == noOfCommands - skippedCommands : format( "Expected %d final commands, got %d " +
                "instead, with %d skipped", noOfCommands, commands.size(), skippedCommands );

        prepared = true;
    }

    private <RECORD extends AbstractBaseRecord> RECORD prepared(
            RecordProxy<RECORD,?> proxy, RecordStore<RECORD> store )
    {
        RECORD after = proxy.forReadingLinkage();
        store.prepareForCommit( after );
        return after;
    }

    void relCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet, locks );
    }

    void relDelete( long relId )
    {
        relationshipDeleter.relDelete( relId, recordChangeSet, locks );
    }

    private void addFiltered( Collection<StorageCommand> target, Mode mode, Command[]... commands )
    {
        for ( Command[] c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == mode )
                {
                    target.add( command );
                }
            }
        }
    }

    /**
     * Deletes a node by its id, returning its properties which are now removed.
     *
     * @param nodeId The id of the node to delete.
     */
    public void nodeDelete( long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete Node[" + nodeId +
                                             "] since it has already been deleted." );
        }
        nodeRecord.setInUse( false );
        nodeRecord.setLabelField( Record.NO_LABELS_FIELD.intValue(),
                markNotInUse( nodeRecord.getDynamicLabelRecords() ) );
        getAndDeletePropertyChain( nodeRecord );
    }

    private Collection<DynamicRecord> markNotInUse( Collection<DynamicRecord> dynamicLabelRecords )
    {
        for ( DynamicRecord record : dynamicLabelRecords )
        {
            record.setInUse( false );
        }
        return dynamicLabelRecords;
    }

    private void getAndDeletePropertyChain( PrimitiveRecord record )
    {
        propertyDeleter.deletePropertyChain( record, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Removes the given property identified by its index from the relationship
     * with the given id.
     *
     * @param relId The id of the relationship that is to have the property removed.
     * @param propertyKey The index key of the property.
     */
    void relRemoveProperty( long relId, int propertyKey )
    {
        RecordProxy<RelationshipRecord, Void> rel = recordChangeSet.getRelRecords().getOrLoad( relId, null );
        propertyDeleter.removeProperty( rel, propertyKey, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Removes the given property identified by indexKeyId of the node with the
     * given id.
     *
     * @param nodeId The id of the node that is to have the property removed.
     * @param propertyKey The index key of the property.
     */
    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null );
        propertyDeleter.removeProperty( node, propertyKey, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Changes an existing property's value of the given relationship, with the
     * given index to the passed value
     * @param relId The id of the relationship which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    void relChangeProperty( long relId, int propertyKey, Value value )
    {
        RecordProxy<RelationshipRecord, Void> rel = recordChangeSet.getRelRecords().getOrLoad( relId, null );
        propertyCreator.primitiveSetProperty( rel, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     * @param nodeId The id of the node which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    void nodeChangeProperty( long nodeId, int propertyKey, Value value )
    {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null );
        propertyCreator.primitiveSetProperty( node, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     * @param relId The id of the relationship to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    void relAddProperty( long relId, int propertyKey, Value value )
    {
        RecordProxy<RelationshipRecord, Void> rel = recordChangeSet.getRelRecords().getOrLoad( relId, null );
        propertyCreator.primitiveSetProperty( rel, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Adds a property to the given node, with the given index and value.
     * @param nodeId The id of the node to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    void nodeAddProperty( long nodeId, int propertyKey, Value value )
    {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null );
        propertyCreator.primitiveSetProperty( node, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    void addLabelToNode( long labelId, long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).add( labelId, nodeStore, nodeStore.getDynamicLabelStore() );
    }

    void removeLabelFromNode( long labelId, long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).remove( labelId, nodeStore );
    }

    /**
     * Creates a node for the given id
     *
     * @param nodeId The id of the node to create.
     */
    public void nodeCreate( long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().create( nodeId, null ).forChangingData();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param key The key of the property index, as a string.
     * @param id The property index record id.
     */
    void createPropertyKeyToken( String key, long id, boolean internal )
    {
        createToken( neoStores.getPropertyKeyTokenStore(), key, id, internal, recordChangeSet.getPropertyKeyTokenChanges() );
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param name The key of the property index, as a string.
     * @param id The property index record id.
     */
    void createLabelToken( String name, long id, boolean internal )
    {
        createToken( neoStores.getLabelTokenStore(), name, id, internal, recordChangeSet.getLabelTokenChanges() );
    }

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param name The name of the relationship type.
     * @param id The id of the new relationship type record.
     */
    void createRelationshipTypeToken( String name, long id, boolean internal )
    {
        createToken( neoStores.getRelationshipTypeTokenStore(), name, id, internal, recordChangeSet.getRelationshipTypeTokenChanges() );
    }

    private static <R extends TokenRecord> void createToken( TokenStore<R> store, String name, long id, boolean internal, RecordAccess<R, Void> recordAccess )
    {
        R record = recordAccess.create( id, null ).forChangingData();
        record.setInUse( true );
        record.setInternal( internal );
        record.setCreated();
        Collection<DynamicRecord> nameRecords = store.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) Iterables.first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
    }

    private static class CommandComparator implements Comparator<Command>
    {
        @Override
        public int compare( Command o1, Command o2 )
        {
            long id1 = o1.getKey();
            long id2 = o2.getKey();
            return Long.compare( id1, id2 );
        }
    }

    void schemaRuleCreate( long ruleId, boolean isConstraint, SchemaRule rule )
    {
        SchemaRecord record = recordChangeSet.getSchemaRuleChanges().create( ruleId, rule ).forChangingData();
        record.setInUse( true );
        record.setCreated();
        record.setConstraint( isConstraint );
    }

    void schemaRuleDelete( long ruleId, SchemaRule rule )
    {
        RecordProxy<SchemaRecord,SchemaRule> proxy = recordChangeSet.getSchemaRuleChanges().getOrLoad( ruleId, rule );
        SchemaRecord record = proxy.forReadingData();
        if ( record.inUse() )
        {
            record = proxy.forChangingData();
            record.setInUse( false );
            getAndDeletePropertyChain( record );
        }
        // Index schema rules may be deleted twice, if they were owned by a constraint; once for dropping the index, and then again as part of
        // dropping the constraint. So we keep this method idempotent.
    }

    void schemaRuleSetProperty( long ruleId, int propertyKeyId, Value value, SchemaRule rule )
    {
        RecordProxy<SchemaRecord, SchemaRule> record = recordChangeSet.getSchemaRuleChanges().getOrLoad( ruleId, rule );
        propertyCreator.primitiveSetProperty( record, propertyKeyId, value, recordChangeSet.getPropertyRecords() );
    }

    void schemaRuleSetIndexOwner( IndexDescriptor rule, long constraintId, int propertyKeyId, Value value )
    {
        // It is possible that the added property will only modify the property chain and leave the owning record untouched.
        // However, we need the schema record to be marked as updated so that an UPDATE schema command is generated.
        // Otherwise, the command appliers, who are responsible for activating index proxies and clearing the schema cache,
        // will not notice our change.
        long ruleId = rule.getId();
        rule = rule.withOwningConstraintId( constraintId );
        RecordAccess<SchemaRecord,SchemaRule> changes = recordChangeSet.getSchemaRuleChanges();
        RecordProxy<SchemaRecord,SchemaRule> record = changes.getOrLoad( ruleId, rule );
        changes.setRecord( ruleId, record.forReadingData(), rule ).forChangingData();
        propertyCreator.primitiveSetProperty( record, propertyKeyId, value, recordChangeSet.getPropertyRecords() );
    }

    public interface PropertyReceiver<P extends StorageProperty>
    {
        void receive( P property, long propertyRecordId );
    }
}
