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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.Mode;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Transaction containing {@link org.neo4j.kernel.impl.transaction.command.Command commands} reflecting the operations
 * performed in the transaction.
 *
 * This class currently has a symbiotic relationship with {@link KernelTransaction}, with which it always has a 1-1
 * relationship.
 *
 * The idea here is that KernelTransaction will eventually take on the responsibilities of WriteTransaction, such as
 * keeping track of transaction state, serialization and deserialization to and from logical log, and applying things
 * to store. It would most likely do this by keeping a component derived from the current WriteTransaction
 * implementation as a sub-component, responsible for handling logical log commands.
 */
public class TransactionRecordState implements RecordState
{
    private static final Command[] EMPTY_COMMANDS = new Command[0];

    private final NeoStores neoStores;
    private final IntegrityValidator integrityValidator;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final MetaDataStore metaDataStore;
    private final SchemaStore schemaStore;
    private final RecordAccessSet recordChangeSet;
    private final long lastCommittedTxWhenTransactionStarted;
    private final ResourceLocker locks;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;

    private RecordChanges<NeoStoreRecord, Void> neoStoreRecord;
    private boolean prepared;

    public TransactionRecordState( NeoStores neoStores, IntegrityValidator integrityValidator,
            RecordChangeSet recordChangeSet, long lastCommittedTxWhenTransactionStarted,
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
        this.metaDataStore = neoStores.getMetaDataStore();
        this.schemaStore = neoStores.getSchemaStore();
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
        return recordChangeSet.hasChanges() ||
                (neoStoreRecord != null && neoStoreRecord.changeSize() > 0);
    }

    @Override
    public void extractCommands( Collection<StorageCommand> commands ) throws TransactionFailureException
    {
        assert !prepared : "Transaction has already been prepared";

        integrityValidator.validateTransactionStartKnowledge( lastCommittedTxWhenTransactionStarted );

        int noOfCommands = recordChangeSet.changeSize() +
                           (neoStoreRecord != null ? neoStoreRecord.changeSize() : 0);

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
            Arrays.sort( nodeCommands, COMMAND_SORTER );
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
            Arrays.sort( relCommands, COMMAND_SORTER );
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
            Arrays.sort( propCommands, COMMAND_SORTER );
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
                     *    externally, such as backup or HA.
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
            Arrays.sort( relGroupCommands, COMMAND_SORTER );
        }

        addFiltered( commands, Mode.CREATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.UPDATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.DELETE, propCommands, relCommands, relGroupCommands, nodeCommands );

        if ( neoStoreRecord != null )
        {
            for ( RecordProxy<NeoStoreRecord, Void> change : neoStoreRecord.changes() )
            {
                commands.add( new Command.NeoStoreCommand( change.getBefore(), change.forReadingData() ) );
            }
        }
        for ( RecordProxy<SchemaRecord, SchemaRule> change : recordChangeSet.getSchemaRuleChanges().changes() )
        {
            if ( change.forReadingLinkage().inUse() )
            {
                integrityValidator.validateSchemaRule( change.getAdditionalData() );
            }
            commands.add( new Command.SchemaRuleCommand(
                    change.getBefore(), change.forChangingData(), change.getAdditionalData() ) );
        }
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

    public void relCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet, locks );
    }

    public void relDelete( long relId )
    {
        relationshipDeleter.relDelete( relId, recordChangeSet, locks );
    }

    @SafeVarargs
    private final void addFiltered( Collection<StorageCommand> target, Mode mode,
                                    Command[]... commands )
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

    private void getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        propertyDeleter.deletePropertyChain( nodeRecord, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Removes the given property identified by its index from the relationship
     * with the given id.
     *
     * @param relId The id of the relationship that is to have the property
     *            removed.
     * @param propertyKey The index key of the property.
     */
    public void relRemoveProperty( long relId, int propertyKey )
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
     *  @param relId The id of the relationship which holds the property to
     *            change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    public void relChangeProperty( long relId, int propertyKey, Value value )
    {
        RecordProxy<RelationshipRecord, Void> rel = recordChangeSet.getRelRecords().getOrLoad( relId, null );
        propertyCreator.primitiveSetProperty( rel, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     *  @param nodeId The id of the node which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    public void nodeChangeProperty( long nodeId, int propertyKey, Value value )
    {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null );
        propertyCreator.primitiveSetProperty( node, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     *  @param relId The id of the relationship to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    public void relAddProperty( long relId, int propertyKey, Value value )
    {
        RecordProxy<RelationshipRecord, Void> rel = recordChangeSet.getRelRecords().getOrLoad( relId, null );
        propertyCreator.primitiveSetProperty( rel, propertyKey, value, recordChangeSet.getPropertyRecords() );
    }

    /**
     * Adds a property to the given node, with the given index and value.
     *  @param nodeId The id of the node to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    public void nodeAddProperty( long nodeId, int propertyKey, Value value )
    {
        RecordProxy<NodeRecord, Void> node = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null );
        propertyCreator.primitiveSetProperty( node, propertyKey, value, recordChangeSet.getPropertyRecords() );
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
    public void createPropertyKeyToken( String key, int id )
    {
        TokenCreator<PropertyKeyTokenRecord, Token> creator =
                new TokenCreator<>( neoStores.getPropertyKeyTokenStore() );
        creator.createToken( key, id, recordChangeSet.getPropertyKeyTokenChanges() );
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param name The key of the property index, as a string.
     * @param id The property index record id.
     */
    public void createLabelToken( String name, int id )
    {
        TokenCreator<LabelTokenRecord, Token> creator =
                new TokenCreator<>( neoStores.getLabelTokenStore() );
        creator.createToken( name, id, recordChangeSet.getLabelTokenChanges() );
    }

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param name The name of the relationship type.
     * @param id The id of the new relationship type record.
     */
    public void createRelationshipTypeToken( String name, int id )
    {
        TokenCreator<RelationshipTypeTokenRecord, RelationshipTypeToken> creator =
                new TokenCreator<>( neoStores.getRelationshipTypeTokenStore() );
        creator.createToken( name, id, recordChangeSet.getRelationshipTypeTokenChanges() );
    }

    private static class CommandSorter implements Comparator<Command>
    {
        @Override
        public int compare( Command o1, Command o2 )
        {
            long id1 = o1.getKey();
            long id2 = o2.getKey();
            long diff = id1 - id2;
            if ( diff > Integer.MAX_VALUE )
            {
                return Integer.MAX_VALUE;
            }
            else if ( diff < Integer.MIN_VALUE )
            {
                return Integer.MIN_VALUE;
            }
            else
            {
                return (int) diff;
            }
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof CommandSorter;
        }

        @Override
        public int hashCode()
        {
            return 3217;
        }
    }

    private static final CommandSorter COMMAND_SORTER = new CommandSorter();

    private RecordProxy<NeoStoreRecord, Void> getOrLoadNeoStoreRecord()
    {
        // TODO Move this neo store record thingie into RecordAccessSet
        if ( neoStoreRecord == null )
        {
            neoStoreRecord = new RecordChanges<>( new RecordChanges.Loader<NeoStoreRecord, Void>()
            {
                @Override
                public NeoStoreRecord newUnused( long key, Void additionalData )
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public NeoStoreRecord load( long key, Void additionalData )
                {
                    return metaDataStore.graphPropertyRecord();
                }

                @Override
                public void ensureHeavy( NeoStoreRecord record )
                {
                }

                @Override
                public NeoStoreRecord clone( NeoStoreRecord neoStoreRecord )
                {
                    return neoStoreRecord.clone();
                }
            }, new IntCounter() );
        }
        return neoStoreRecord.getOrLoad( 0L, null );
    }

    /**
     * Adds a property to the graph, with the given index and value.
     *  @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     */
    public void graphAddProperty( int propertyKey, Value value )
    {
        propertyCreator.primitiveSetProperty( getOrLoadNeoStoreRecord(), propertyKey, value,
                recordChangeSet.getPropertyRecords() );
    }

    /**
     * Changes an existing property of the graph, with the given index to
     * the passed value
     *
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     */
    public void graphChangeProperty( int propertyKey, Value value )
    {
        propertyCreator.primitiveSetProperty( getOrLoadNeoStoreRecord(), propertyKey, value,
                recordChangeSet.getPropertyRecords() );
    }

    /**
     * Removes the given property identified by indexKeyId of the graph with the
     * given id.
     *
     * @param propertyKey The index key of the property.
     */
    public void graphRemoveProperty( int propertyKey )
    {
        RecordProxy<NeoStoreRecord, Void> recordChange = getOrLoadNeoStoreRecord();
        propertyDeleter.removeProperty( recordChange, propertyKey, recordChangeSet.getPropertyRecords() );
    }

    public void createSchemaRule( SchemaRule schemaRule )
    {
        for ( DynamicRecord change : recordChangeSet.getSchemaRuleChanges()
                .create( schemaRule.getId(), schemaRule )
                .forChangingData() )
        {
            change.setInUse( true );
            change.setCreated();
        }
    }

    public void dropSchemaRule( SchemaRule rule )
    {
        RecordProxy<SchemaRecord, SchemaRule> change =
                recordChangeSet.getSchemaRuleChanges().getOrLoad( rule.getId(), rule );
        SchemaRecord records = change.forChangingData();
        for ( DynamicRecord record : records )
        {
            record.setInUse( false );
        }
        records.setInUse( false );
    }

    public void changeSchemaRule( SchemaRule rule, SchemaRule updatedRule )
    {
        //Read the current record
        RecordProxy<SchemaRecord,SchemaRule> change = recordChangeSet.getSchemaRuleChanges()
                .getOrLoad( rule.getId(), rule );
        SchemaRecord records = change.forReadingData();

        //Register the change of the record
        RecordProxy<SchemaRecord,SchemaRule> recordChange = recordChangeSet.getSchemaRuleChanges()
                .setRecord( rule.getId(), records, updatedRule );
        SchemaRecord dynamicRecords = recordChange.forChangingData();

        //Update the record
        dynamicRecords.setDynamicRecords( schemaStore.allocateFrom( updatedRule ) );
    }

    public void addLabelToNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).add( labelId, nodeStore, nodeStore.getDynamicLabelStore() );
    }

    public void removeLabelFromNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).remove( labelId, nodeStore );
    }

    public void setConstraintIndexOwner( IndexRule indexRule, long constraintId )
    {
        IndexRule updatedIndexRule = indexRule.withOwningConstraint( constraintId );
        changeSchemaRule( indexRule, updatedIndexRule );
    }

    public interface PropertyReceiver<P extends StorageProperty>
    {
        void receive( P property, long propertyRecordId );
    }
}
