/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.xa.RecordChanges.RecordChange;
import org.neo4j.kernel.impl.persistence.NeoStoreTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.copyOf;

import static org.neo4j.helpers.collection.IteratorUtil.asPrimitiveIterator;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.CREATE;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.DELETE;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.UPDATE;

/**
 * Transaction containing {@link Command commands} reflecting the operations
 * performed in the transaction.
 *
 * This class currently has a symbiotic relationship with {@link KernelTransaction}, with which it always has a 1-1
 * relationship.
 *
 * The idea here is that KernelTransaction will eventually take on the responsibilities of WriteTransaction, such as
 * keeping track of transaction state, serialization and deserialization to and from logical log, and applying things
 * to store. It would most likely do this by keeping a component derived from the current WriteTransaction
 * implementation as a sub-component, responsible for handling logical log commands.
 *
 * The class XAResourceManager plays in here as well, in that it shares responsibilities with WriteTransaction to
 * write data to the logical log. As we continue to refactor this subsystem, XAResourceManager should ideally not know
 * about the logical log, but defer entirely to the Kernel to handle this. Doing that will give the kernel full
 * discretion to start experimenting with higher-performing logical log implementations, without being hindered by
 * having to contend with the JTA compliance layers. In short, it would encapsulate the logical log/storage logic better
 * and thus make it easier to change.
 */
public class WriteTransaction extends XaTransaction implements NeoStoreTransaction
{
    private final RecordChanges<Long, NodeRecord, Void> nodeRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, NodeRecord, Void>()
            {
                @Override
                public NodeRecord newUnused( Long key, Void additionalData )
                {
                    return new NodeRecord( key, Record.NO_NEXT_RELATIONSHIP.intValue(),
                                           Record.NO_NEXT_PROPERTY.intValue() );
                }

                @Override
                public NodeRecord load( Long key, Void additionalData )
                {
                    return getNodeStore().getRecord( key );
                }

                @Override
                public void ensureHeavy( NodeRecord record )
                {
                    getNodeStore().ensureHeavy( record );
                }

                @Override
                public NodeRecord clone(NodeRecord nodeRecord)
                {
                    return nodeRecord.clone();
                }
            }, true );
    private final RecordChanges<Long, PropertyRecord, PrimitiveRecord> propertyRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, PropertyRecord, PrimitiveRecord>()
            {
                @Override
                public PropertyRecord newUnused( Long key, PrimitiveRecord additionalData )
                {
                    PropertyRecord record = new PropertyRecord( key );
                    setOwner( record, additionalData );
                    return record;
                }

                private void setOwner( PropertyRecord record, PrimitiveRecord owner )
                {
                    if ( owner != null )
                    {
                        owner.setIdTo( record );
                    }
                }

                @Override
                public PropertyRecord load( Long key, PrimitiveRecord additionalData )
                {
                    PropertyRecord record = getPropertyStore().getRecord( key.longValue() );
                    setOwner( record, additionalData );
                    return record;
                }

                @Override
                public void ensureHeavy( PropertyRecord record )
                {
                    for ( PropertyBlock block : record.getPropertyBlocks() )
                    {
                        getPropertyStore().ensureHeavy( block );
                    }
                }

                @Override
                public PropertyRecord clone(PropertyRecord propertyRecord)
                {
                    return propertyRecord.clone();
                }
            }, true );
    private final RecordChanges<Long, RelationshipRecord, Void> relRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, RelationshipRecord, Void>()
            {
                @Override
                public RelationshipRecord newUnused( Long key, Void additionalData )
                {
                    return new RelationshipRecord( key );
                }

                @Override
                public RelationshipRecord load( Long key, Void additionalData )
                {
                    return getRelationshipStore().getRecord( key );
                }

                @Override
                public void ensureHeavy( RelationshipRecord record )
                {
                }

                @Override
                public RelationshipRecord clone(RelationshipRecord relationshipRecord) {
                    // Not needed because we don't manage before state for relationship records.
                    throw new UnsupportedOperationException("Unexpected call to clone on a relationshipRecord");
                }
            }, false );

    private final RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> schemaRuleChanges = new RecordChanges<>(new RecordChanges.Loader<Long, Collection<DynamicRecord>, SchemaRule>() {
        @Override
        public Collection<DynamicRecord> newUnused(Long key, SchemaRule additionalData)
        {
            return getSchemaStore().allocateFrom(additionalData);
        }

        @Override
        public Collection<DynamicRecord> load(Long key, SchemaRule additionalData)
        {
            return getSchemaStore().getRecords( key );
        }

        @Override
        public void ensureHeavy(Collection<DynamicRecord> dynamicRecords)
        {
            SchemaStore schemaStore = getSchemaStore();
            for ( DynamicRecord record : dynamicRecords)
            {
                schemaStore.ensureHeavy(record);
            }
        }

        @Override
        public Collection<DynamicRecord> clone(Collection<DynamicRecord> dynamicRecords) {
            Collection<DynamicRecord> list = new ArrayList<>( dynamicRecords.size() );
            for ( DynamicRecord record : dynamicRecords)
            {
                list.add( record.clone() );
            }
            return list;
        }
    }, true);
    private Map<Integer, RelationshipTypeTokenRecord> relationshipTypeTokenRecords;
    private Map<Integer, LabelTokenRecord> labelTokenRecords;
    private Map<Integer, PropertyKeyTokenRecord> propertyKeyTokenRecords;
    private RecordChanges<Long, NeoStoreRecord, Void> neoStoreRecord;

    private final Map<Long, Command.NodeCommand> nodeCommands = new TreeMap<>();
    private final ArrayList<Command.PropertyCommand> propCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipCommand> relCommands = new ArrayList<>();
    private final ArrayList<Command.SchemaRuleCommand> schemaRuleCommands = new ArrayList<>();
    private ArrayList<Command.RelationshipTypeTokenCommand> relationshipTypeTokenCommands;
    private ArrayList<Command.LabelTokenCommand> labelTokenCommands;
    private ArrayList<Command.PropertyKeyTokenCommand> propertyKeyTokenCommands;
    private Command.NeoStoreCommand neoStoreCommand;

    private boolean committed = false;
    private boolean prepared = false;

    private XaConnection xaConnection;

    private final long lastCommittedTxWhenTransactionStarted;
    private final TransactionState state;
    private final CacheAccessBackDoor cacheAccess;
    private final IndexingService indexes;
    private final NeoStore neoStore;
    private final LabelScanStore labelScanStore;
    private final IntegrityValidator integrityValidator;
    private final KernelTransactionImplementation kernelTransaction;
    private final LockService locks;

    /**
     * @param lastCommittedTxWhenTransactionStarted is the highest committed transaction id when this transaction
     *                                              begun. No operations in this transaction are allowed to have
     *                                              taken place before that transaction id. This is used by
     *                                              constraint validation - if a constraint was not online when this
     *                                              transaction begun, it will be verified during prepare. If you are
     *                                              writing code against this API and are unsure about what to set
     *                                              this value to, 0 is a safe choice. That will ensure all
     *                                              constraints are checked.
     * @param kernelTransaction is the vanilla sauce to the WriteTransaction apple pie.
     */
    WriteTransaction( int identifier, long lastCommittedTxWhenTransactionStarted, XaLogicalLog log,
                      TransactionState state, NeoStore neoStore, CacheAccessBackDoor cacheAccess,
                      IndexingService indexingService, LabelScanStore labelScanStore,
                      IntegrityValidator integrityValidator, KernelTransactionImplementation kernelTransaction,
                      LockService locks )
    {
        super( identifier, log, state );
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.neoStore = neoStore;
        this.state = state;
        this.cacheAccess = cacheAccess;
        this.indexes = indexingService;
        this.labelScanStore = labelScanStore;
        this.integrityValidator = integrityValidator;
        this.kernelTransaction = kernelTransaction;
        this.locks = locks;
    }

    @Override
    public KernelTransactionImplementation kernelTransaction()
    {
        return kernelTransaction;
    }

    @Override
    public boolean isReadOnly()
    {
        if ( isRecovered() )
        {
            return nodeCommands.size() == 0 && propCommands.size() == 0 &&
                   relCommands.size() == 0 && schemaRuleCommands.size() == 0 && relationshipTypeTokenCommands == null &&
                   labelTokenCommands == null && propertyKeyTokenCommands == null && kernelTransaction.isReadOnly();
        }
        return nodeRecords.changeSize() == 0 && relRecords.changeSize() == 0 && schemaRuleChanges.changeSize() == 0 &&
               propertyRecords.changeSize() == 0 && relationshipTypeTokenRecords == null && labelTokenRecords == null &&
               propertyKeyTokenRecords == null && kernelTransaction.isReadOnly();
    }

    // Make this accessible in this package
    @Override
    protected void setRecovered()
    {
        super.setRecovered();
    }

    @Override
    public void doAddCommand( XaCommand command )
    {
        // override
    }

    @Override
    protected void doPrepare() throws XAException
    {

        if ( committed )
        {
            throw new XAException( "Cannot prepare committed transaction["
                    + getIdentifier() + "]" );
        }
        if ( prepared )
        {
            throw new XAException( "Cannot prepare prepared transaction["
                    + getIdentifier() + "]" );
        }

        kernelTransaction.prepare();

        prepared = true;

        int noOfCommands = nodeRecords.changeSize() +
                           relRecords.changeSize() +
                           propertyRecords.changeSize() +
                           schemaRuleChanges.changeSize() +
                           (propertyKeyTokenRecords != null ? propertyKeyTokenRecords.size() : 0) +
                           (relationshipTypeTokenRecords != null ? relationshipTypeTokenRecords.size() : 0) +
                           (labelTokenRecords != null ? labelTokenRecords.size() : 0);
        List<Command> commands = new ArrayList<>( noOfCommands );
        if ( relationshipTypeTokenRecords != null )
        {
            relationshipTypeTokenCommands = new ArrayList<>();
            for ( RelationshipTypeTokenRecord record : relationshipTypeTokenRecords.values() )
            {
                Command.RelationshipTypeTokenCommand command =
                        new Command.RelationshipTypeTokenCommand(
                                neoStore.getRelationshipTypeStore(), record );
                relationshipTypeTokenCommands.add( command );
                commands.add( command );
            }
        }
        if ( labelTokenRecords != null )
        {
            labelTokenCommands = new ArrayList<>();
            for ( LabelTokenRecord record : labelTokenRecords.values() )
            {
                Command.LabelTokenCommand command =
                        new Command.LabelTokenCommand(
                                neoStore.getLabelTokenStore(), record );
                labelTokenCommands.add( command );
                commands.add( command );
            }
        }
        for ( RecordChange<Long, NodeRecord, Void> change : nodeRecords.changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            integrityValidator.validateNodeRecord( record );
            Command.NodeCommand command = new Command.NodeCommand(
                    neoStore.getNodeStore(), change.getBefore(), record );
            nodeCommands.put( record.getId(), command );
            commands.add( command );
        }
        for ( RecordChange<Long, RelationshipRecord, Void> record : relRecords.changes() )
        {
            Command.RelationshipCommand command = new Command.RelationshipCommand(
                    neoStore.getRelationshipStore(), record.forReadingLinkage() );
            relCommands.add( command );
            commands.add( command );
        }
        if ( neoStoreRecord != null )
        {
            for ( RecordChange<Long, NeoStoreRecord, Void> change : neoStoreRecord.changes() )
            {
                neoStoreCommand = new Command.NeoStoreCommand( neoStore, change.forReadingData() );
                addCommand( neoStoreCommand );
            }
        }
        if ( propertyKeyTokenRecords != null )
        {
            propertyKeyTokenCommands = new ArrayList<>();
            for ( PropertyKeyTokenRecord record : propertyKeyTokenRecords.values() )
            {
                Command.PropertyKeyTokenCommand command =
                        new Command.PropertyKeyTokenCommand(
                                neoStore.getPropertyStore().getPropertyKeyTokenStore(), record );
                propertyKeyTokenCommands.add( command );
                commands.add( command );
            }
        }
        for ( RecordChange<Long, PropertyRecord, PrimitiveRecord> change : propertyRecords.changes() )
        {
            Command.PropertyCommand command = new Command.PropertyCommand(
                    neoStore.getPropertyStore(), change.getBefore(), change.forReadingLinkage() );
            propCommands.add( command );
            commands.add( command );
        }
        for ( RecordChange<Long, Collection<DynamicRecord>, SchemaRule> change : schemaRuleChanges.changes() )
        {
            integrityValidator.validateSchemaRule( change.getAdditionalData() );
            Command.SchemaRuleCommand command = new Command.SchemaRuleCommand(
                    neoStore,
                    neoStore.getSchemaStore(),
                    indexes,
                    change.getBefore(),
                    change.forChangingData(),
                    change.getAdditionalData(),
                    -1 );
            schemaRuleCommands.add( command );
            commands.add( command );
        }
        assert commands.size() == noOfCommands : "Expected " + noOfCommands
                                                 + " final commands, got "
                                                 + commands.size() + " instead";
        intercept( commands );

        for ( Command command : commands )
        {
            addCommand( command );
        }

        integrityValidator.validateTransactionStartKnowledge( lastCommittedTxWhenTransactionStarted );
    }

    protected void intercept( List<Command> commands )
    {
        // default no op
    }

    @Override
    protected void injectCommand( XaCommand xaCommand )
    {
        if ( xaCommand instanceof Command.NodeCommand )
        {
            NodeCommand nodeCommand = (Command.NodeCommand) xaCommand;
            nodeCommands.put( nodeCommand.getKey(), nodeCommand );
        }
        else if ( xaCommand instanceof Command.RelationshipCommand )
        {
            relCommands.add( (Command.RelationshipCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.PropertyCommand )
        {
            propCommands.add( (Command.PropertyCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.PropertyKeyTokenCommand )
        {
            if ( propertyKeyTokenCommands == null )
            {
                propertyKeyTokenCommands = new ArrayList<>();
            }
            propertyKeyTokenCommands.add( (Command.PropertyKeyTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.RelationshipTypeTokenCommand )
        {
            if ( relationshipTypeTokenCommands == null )
            {
                relationshipTypeTokenCommands = new ArrayList<>();
            }
            relationshipTypeTokenCommands.add( (Command.RelationshipTypeTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.LabelTokenCommand )
        {
            if ( labelTokenCommands == null )
            {
                labelTokenCommands = new ArrayList<>();
            }
            labelTokenCommands.add( (Command.LabelTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.NeoStoreCommand )
        {
            assert neoStoreCommand == null;
            neoStoreCommand = (Command.NeoStoreCommand) xaCommand;
        }
        else if ( xaCommand instanceof Command.SchemaRuleCommand )
        {
            schemaRuleCommands.add( (Command.SchemaRuleCommand) xaCommand );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown command " + xaCommand );
        }
    }

    @Override
    public void doRollback() throws XAException
    {
        if ( committed )
        {
            throw new XAException( "Cannot rollback partialy commited "
                                   + "transaction[" + getIdentifier() + "]. Recover and "
                                   + "commit" );
        }
        try
        {
            boolean freeIds = neoStore.freeIdsDuringRollback();
            if ( relationshipTypeTokenRecords != null )
            {
                for ( RelationshipTypeTokenRecord record : relationshipTypeTokenRecords.values() )
                {
                    if ( record.isCreated() )
                    {
                        if ( freeIds )
                        {
                            getRelationshipTypeStore().freeId( record.getId() );
                        }
                        for ( DynamicRecord dynamicRecord : record.getNameRecords() )
                        {
                            if ( dynamicRecord.isCreated() )
                            {
                                getRelationshipTypeStore().freeId(
                                        (int) dynamicRecord.getId() );
                            }
                        }
                    }
                    removeRelationshipTypeFromCache( record.getId() );
                }
            }
            for ( RecordChange<Long, NodeRecord, Void> change : nodeRecords.changes() )
            {
                NodeRecord record = change.forReadingLinkage();
                if ( freeIds && record.isCreated() )
                {
                    getNodeStore().freeId( record.getId() );
                }
                removeNodeFromCache( record.getId() );
            }
            for ( RecordChange<Long, RelationshipRecord, Void> change : relRecords.changes() )
            {
                long id = change.getKey();
                RelationshipRecord record = change.forReadingLinkage();
                if ( freeIds && change.isCreated() )
                {
                    getRelationshipStore().freeId( id );
                }
                removeRelationshipFromCache( id );
                patchDeletedRelationshipNodes( id, record.getFirstNode(), record.getFirstNextRel(),
                                               record.getSecondNode(), record.getSecondNextRel() );
            }
            if ( neoStoreRecord != null )
            {
                removeGraphPropertiesFromCache();
            }
            if ( propertyKeyTokenRecords != null )
            {
                for ( PropertyKeyTokenRecord record : propertyKeyTokenRecords.values() )
                {
                    if ( record.isCreated() )
                    {
                        if ( freeIds )
                        {
                            getPropertyStore().getPropertyKeyTokenStore().freeId( record.getId() );
                        }
                        for ( DynamicRecord dynamicRecord : record.getNameRecords() )
                        {
                            if ( dynamicRecord.isCreated() )
                            {
                                getPropertyStore().getPropertyKeyTokenStore().freeId(
                                        (int) dynamicRecord.getId() );
                            }
                        }
                    }
                }
            }
            for ( RecordChange<Long, PropertyRecord, PrimitiveRecord> change : propertyRecords.changes() )
            {
                PropertyRecord record = change.forReadingLinkage();
                if ( record.getNodeId() != -1 )
                {
                    removeNodeFromCache( record.getNodeId() );
                }
                else if ( record.getRelId() != -1 )
                {
                    removeRelationshipFromCache( record.getRelId() );
                }
                if ( record.isCreated() )
                {
                    if ( freeIds )
                    {
                        getPropertyStore().freeId( record.getId() );
                    }
                    for ( PropertyBlock block : record.getPropertyBlocks() )
                    {
                        for ( DynamicRecord dynamicRecord : block.getValueRecords() )
                        {
                            if ( dynamicRecord.isCreated() )
                            {
                                if ( dynamicRecord.getType() == PropertyType.STRING.intValue() )
                                {
                                    getPropertyStore().freeStringBlockId(
                                            dynamicRecord.getId() );
                                }
                                else if ( dynamicRecord.getType() == PropertyType.ARRAY.intValue() )
                                {
                                    getPropertyStore().freeArrayBlockId(
                                            dynamicRecord.getId() );
                                }
                                else
                                {
                                    throw new InvalidRecordException(
                                            "Unknown type on " + dynamicRecord );
                                }
                            }
                        }
                    }
                }
            }
            for ( RecordChange<Long, Collection<DynamicRecord>, SchemaRule> records : schemaRuleChanges.changes() )
            {
                long id = -1;
                for ( DynamicRecord record : records.forChangingData() )
                {
                    if ( id == -1 )
                    {
                        id = record.getId();
                    }
                    if ( freeIds && record.isCreated() )
                    {
                        getSchemaStore().freeId( record.getId() );
                    }
                }
            }
        }
        finally
        {
            clear();
        }
    }

    private void removeRelationshipTypeFromCache( int id )
    {
        cacheAccess.removeRelationshipTypeFromCache( id );
    }

    private void patchDeletedRelationshipNodes( long id, long firstNodeId, long firstNodeNextRelId, long secondNodeId,
                                                long secondNextRelId )
    {
        cacheAccess.patchDeletedRelationshipNodes( id, firstNodeId, firstNodeNextRelId, secondNodeId, secondNextRelId );
    }

    private void removeRelationshipFromCache( long id )
    {
        cacheAccess.removeRelationshipFromCache( id );
    }

    private void removeNodeFromCache( long id )
    {
        cacheAccess.removeNodeFromCache( id );
    }

    private void removeGraphPropertiesFromCache()
    {
        cacheAccess.removeGraphPropertiesFromCache();
    }

    private void addRelationshipType( int id )
    {
        setRecovered();
        Token type = isRecovered() ?
                     neoStore.getRelationshipTypeStore().getToken( id, true ) :
                     neoStore.getRelationshipTypeStore().getToken( id );
        cacheAccess.addRelationshipTypeToken( type );
    }

    private void addLabel( int id )
    {
        Token labelId = isRecovered() ?
                        neoStore.getLabelTokenStore().getToken( id, true ) :
                        neoStore.getLabelTokenStore().getToken( id );
        cacheAccess.addLabelToken( labelId );
    }

    private void addPropertyKey( int id )
    {
        Token index = isRecovered() ?
                      neoStore.getPropertyStore().getPropertyKeyTokenStore().getToken( id, true ) :
                      neoStore.getPropertyStore().getPropertyKeyTokenStore().getToken( id );
        cacheAccess.addPropertyKeyToken( index );
    }

    @Override
    public void doCommit() throws XAException
    {
        if ( !isRecovered() && !prepared )
        {
            throw new XAException( "Cannot commit non prepared transaction[" + getIdentifier() + "]" );
        }
        if ( isRecovered() )
        {
            boolean wasInRecovery = neoStore.isInRecoveryMode();
            neoStore.setRecoveredStatus( true );
            try
            {
                applyCommit( true );
                return;
            }
            finally
            {
                neoStore.setRecoveredStatus( wasInRecovery );
            }
        }
        if ( getCommitTxId() != neoStore.getLastCommittedTx() + 1 )
        {
            throw new RuntimeException( "Tx id: " + getCommitTxId() +
                                        " not next transaction (" + neoStore.getLastCommittedTx() + ")" );
        }

        applyCommit( false );
    }

    private void applyCommit( boolean isRecovered )
    {
        try ( LockGroup lockGroup = new LockGroup() )
        {
            committed = true;
            CommandSorter sorter = new CommandSorter();
            // reltypes
            if ( relationshipTypeTokenCommands != null )
            {
                java.util.Collections.sort( relationshipTypeTokenCommands, sorter );
                for ( Command.RelationshipTypeTokenCommand command : relationshipTypeTokenCommands )
                {
                    command.execute();
                    if ( isRecovered )
                    {
                        addRelationshipType( (int) command.getKey() );
                    }
                }
            }
            // label keys
            if ( labelTokenCommands != null )
            {
                java.util.Collections.sort( labelTokenCommands, sorter );
                for ( Command.LabelTokenCommand command : labelTokenCommands )
                {
                    command.execute();
                    if ( isRecovered )
                    {
                        addLabel( (int) command.getKey() );
                    }
                }
            }
            // property keys
            if ( propertyKeyTokenCommands != null )
            {
                java.util.Collections.sort( propertyKeyTokenCommands, sorter );
                for ( Command.PropertyKeyTokenCommand command : propertyKeyTokenCommands )
                {
                    command.execute();
                    if ( isRecovered )
                    {
                        addPropertyKey( (int) command.getKey() );
                    }
                }
            }

            // primitives
            java.util.Collections.sort( relCommands, sorter );
            java.util.Collections.sort( propCommands, sorter );
            executeCreated( lockGroup, isRecovered, propCommands, relCommands, nodeCommands.values() );
            executeModified( lockGroup, isRecovered, propCommands, relCommands, nodeCommands.values() );
            executeDeleted( lockGroup, propCommands, relCommands, nodeCommands.values() );

            // property change set for index updates
            Collection<NodeLabelUpdate> labelUpdates = gatherLabelUpdates();
            if ( !labelUpdates.isEmpty() )
            {
                updateLabelScanStore( labelUpdates );
                cacheAccess.applyLabelUpdates( labelUpdates );
            }

            if ( !nodeCommands.isEmpty() || !propCommands.isEmpty() )
            {
                indexes.updateIndexes( new LazyIndexUpdates(
                        getNodeStore(), getPropertyStore(),
                        new ArrayList<>( propCommands ), new HashMap<>( nodeCommands ) ) );
            }

            // schema rules. Execute these after generating the property updates so. If executed
            // before and we've got a transaction that sets properties/labels as well as creating an index
            // we might end up with this corner-case:
            // 1) index rule created and index population job started
            // 2) index population job processes some nodes, but doesn't complete
            // 3) we gather up property updates and send those to the indexes. The newly created population
            //    job might get those as updates
            // 4) the population job will apply those updates as added properties, and might end up with duplicate
            //    entries for the same property
            for ( SchemaRuleCommand command : schemaRuleCommands )
            {
                command.setTxId( getCommitTxId() );
                command.execute();
                switch ( command.getMode() )
                {
                case DELETE:
                    cacheAccess.removeSchemaRuleFromCache( command.getKey() );
                    break;
                default:
                    cacheAccess.addSchemaRule( command.getSchemaRule() );
                }
            }

            if ( neoStoreCommand != null )
            {
                neoStoreCommand.execute();
                if ( isRecovered )
                {
                    removeGraphPropertiesFromCache();
                }
            }
            if ( !isRecovered )
            {
                updateFirstRelationships();
                state.commitCows(); // updates the cached primitives
            }
            neoStore.setLastCommittedTx( getCommitTxId() );
            if ( isRecovered )
            {
                neoStore.updateIdGenerators();
            }
        }
        finally
        {
            clear();
        }
    }

    private Collection<NodeLabelUpdate> gatherLabelUpdates()
    {
        List<NodeLabelUpdate> labelUpdates = new ArrayList<>();
        for ( NodeCommand nodeCommand : nodeCommands.values() )
        {
            NodeLabels labelFieldBefore = parseLabelsField( nodeCommand.getBefore() );
            NodeLabels labelFieldAfter = parseLabelsField( nodeCommand.getAfter() );
            if ( labelFieldBefore.isInlined() && labelFieldAfter.isInlined()
                 && nodeCommand.getBefore().getLabelField() == nodeCommand.getAfter().getLabelField() )
            {
                continue;
            }
            long[] labelsBefore = labelFieldBefore.getIfLoaded();
            long[] labelsAfter = labelFieldAfter.getIfLoaded();
            if ( labelsBefore == null || labelsAfter == null )
            {
                continue;
            }
            labelUpdates.add( NodeLabelUpdate.labelChanges( nodeCommand.getKey(), labelsBefore, labelsAfter ) );
        }
        return labelUpdates;
    }

    private void updateLabelScanStore( Iterable<NodeLabelUpdate> labelUpdates )
    {
        try
        {
            labelScanStore.updateAndCommit( labelUpdates.iterator() );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    static class LabelChangeSummary
    {
        private static final long[] NO_LABELS = new long[0];

        private final long[] addedLabels;
        private final long[] removedLabels;

        LabelChangeSummary( long[] labelsBefore, long[] labelsAfter )
        {
            // Ids are sorted in the store
            long[] addedLabels = new long[labelsAfter.length];
            long[] removedLabels = new long[labelsBefore.length];
            int addedLabelsCursor = 0, removedLabelsCursor = 0;
            for ( long labelAfter : labelsAfter )
            {
                if ( binarySearch( labelsBefore, labelAfter ) < 0 )
                {
                    addedLabels[addedLabelsCursor++] = labelAfter;
                }
            }
            for ( long labelBefore : labelsBefore )
            {
                if ( binarySearch( labelsAfter, labelBefore ) < 0 )
                {
                    removedLabels[removedLabelsCursor++] = labelBefore;
                }
            }

            // For each property on the node, produce one update for added labels and one for removed labels.
            this.addedLabels = shrink( addedLabels, addedLabelsCursor );
            this.removedLabels = shrink( removedLabels, removedLabelsCursor );
        }

        private long[] shrink( long[] array, int toLength )
        {
            if ( toLength == 0 )
            {
                return NO_LABELS;
            }
            return array.length == toLength ? array : copyOf( array, toLength );
        }

        public boolean hasAddedLabels()
        {
            return addedLabels.length > 0;
        }

        public boolean hasRemovedLabels()
        {
            return removedLabels.length > 0;
        }

        public long[] getAddedLabels()
        {
            return addedLabels;
        }

        public long[] getRemovedLabels()
        {
            return removedLabels;
        }
    }

    @Override
    public boolean delistResource( Transaction tx, int tmsuccess )
            throws SystemException
    {
        return xaConnection.delistResource( tx, tmsuccess );
    }

    private void updateFirstRelationships()
    {
        for ( RecordChange<Long, NodeRecord, Void> change : nodeRecords.changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            state.setFirstIds( record.getId(), record.getNextRel(), record.getNextProp() );
        }
    }

    @SafeVarargs
    private final void executeCreated( LockGroup lockGroup, boolean removeFromCache,
                                       Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == CREATE )
                {
                    lockEntity( lockGroup, command );
                    command.execute();
                    if ( removeFromCache )
                    {
                        command.removeFromCache( cacheAccess );
                    }
                }
            }
        }
    }

    @SafeVarargs
    private final void executeModified( LockGroup lockGroup, boolean removeFromCache,
                                        Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == UPDATE )
                {
                    lockEntity( lockGroup, command );
                    command.execute();
                    if ( removeFromCache )
                    {
                        command.removeFromCache( cacheAccess );
                    }
                }
            }
        }
    }

    @SafeVarargs
    private final void executeDeleted( LockGroup lockGroup, Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == DELETE )
                {
                /*
                 * We always update the disk image and then always invalidate the cache. In the case of relationships
                 * this is expected to also patch the relChainPosition in the start and end NodeImpls (if they actually
                 * are in cache).
                 */
                    lockEntity( lockGroup, command );
                    command.execute();
                    command.removeFromCache( cacheAccess );
                }
            }
        }
    }

    private void lockEntity( LockGroup lockGroup, Command command )
    {
        if ( command instanceof NodeCommand )
        {
            lockGroup.add( locks.acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK ) );
        }
        if ( command instanceof Command.PropertyCommand )
        {
            long nodeId = ((Command.PropertyCommand) command).getNodeId();
            if ( nodeId != -1 )
            {
                lockGroup.add( locks.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK ) );
            }
        }
    }

    private void clear()
    {
        nodeRecords.clear();
        propertyRecords.clear();
        relRecords.clear();
        schemaRuleChanges.clear();
        relationshipTypeTokenRecords = null;
        propertyKeyTokenRecords = null;
        neoStoreRecord = null;

        nodeCommands.clear();
        propCommands.clear();
        propertyKeyTokenCommands = null;
        relCommands.clear();
        schemaRuleCommands.clear();
        relationshipTypeTokenCommands = null;
        labelTokenCommands = null;
        neoStoreCommand = null;
    }

    private RelationshipTypeTokenStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    private LabelTokenStore getLabelTokenStore()
    {
        return neoStore.getLabelTokenStore();
    }

    private int getRelGrabSize()
    {
        return neoStore.getRelationshipGrabSize();
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private SchemaStore getSchemaStore()
    {
        return neoStore.getSchemaStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    @Override
    public NodeRecord nodeLoadLight( long nodeId )
    {
        try
        {
            return nodeRecords.getOrLoad( nodeId, null ).forReadingLinkage();
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }
    }

    @Override
    public RelationshipRecord relLoadLight( long id )
    {
        try
        {
            return relRecords.getOrLoad( id, null ).forReadingLinkage();
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> nodeDelete( long nodeId )
    {
        NodeRecord nodeRecord = nodeRecords.getOrLoad( nodeId, null ).forChangingData();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete Node[" + nodeId +
                                             "] since it has already been deleted." );
        }
        nodeRecord.setInUse( false );
        nodeRecord.setLabelField( 0, Collections.<DynamicRecord>emptyList() );
        return getAndDeletePropertyChain( nodeRecord );
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> relDelete( long id )
    {
        RelationshipRecord record = relRecords.getOrLoad( id, null ).forChangingLinkage();
        if ( !record.inUse() )
        {
            throw new IllegalStateException( "Unable to delete relationship[" +
                                             id + "] since it is already deleted." );
        }
        ArrayMap<Integer, DefinedProperty> propertyMap = getAndDeletePropertyChain( record );
        disconnectRelationship( record );
        updateNodes( record );
        record.setInUse( false );
        return propertyMap;
    }

    private ArrayMap<Integer, DefinedProperty> getAndDeletePropertyChain( PrimitiveRecord primitive )
    {
        ArrayMap<Integer, DefinedProperty> result = new ArrayMap<>( (byte) 9, false, true );
        long nextProp = primitive.getNextProp();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            RecordChange<Long, PropertyRecord, PrimitiveRecord> propertyChange =
                    propertyRecords.getOrLoad( nextProp, primitive );

            // TODO forChanging/forReading piggy-backing
            PropertyRecord propRecord = propertyChange.forChangingData();
            PropertyRecord before = propertyChange.getBefore();
            for ( PropertyBlock block : before.getPropertyBlocks() )
            {
                result.put( block.getKeyIndexId(), block.newPropertyData( getPropertyStore() ) );
            }
            for ( PropertyBlock block : propRecord.getPropertyBlocks() )
            {
                for ( DynamicRecord valueRecord : block.getValueRecords() )
                {
                    assert valueRecord.inUse();
                    valueRecord.setInUse( false );
                    propRecord.addDeletedRecord( valueRecord );
                }
            }
            nextProp = propRecord.getNextProp();
            propRecord.setInUse( false );
            propRecord.setChanged( primitive );
            // We do not remove them individually, but all together here
            propRecord.getPropertyBlocks().clear();
        }
        return result;
    }

    private void disconnectRelationship( RelationshipRecord rel )
    {
        // update first node prev
        if ( rel.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship( rel.getFirstPrevRel() );
            getWriteLock( lockableRel );
            RelationshipRecord prevRel = relRecords.getOrLoad( rel.getFirstPrevRel(), null ).forChangingLinkage();
            boolean changed = false;
            if ( prevRel.getFirstNode() == rel.getFirstNode() )
            {
                prevRel.setFirstNextRel( rel.getFirstNextRel() );
                changed = true;
            }
            if ( prevRel.getSecondNode() == rel.getFirstNode() )
            {
                prevRel.setSecondNextRel( rel.getFirstNextRel() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException(
                        prevRel + " don't match " + rel );
            }
        }
        // update first node next
        if ( rel.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship( rel.getFirstNextRel() );
            getWriteLock( lockableRel );
            RelationshipRecord nextRel = relRecords.getOrLoad( rel.getFirstNextRel(), null ).forChangingLinkage();
            boolean changed = false;
            if ( nextRel.getFirstNode() == rel.getFirstNode() )
            {
                nextRel.setFirstPrevRel( rel.getFirstPrevRel() );
                changed = true;
            }
            if ( nextRel.getSecondNode() == rel.getFirstNode() )
            {
                nextRel.setSecondPrevRel( rel.getFirstPrevRel() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( nextRel + " don't match " + rel );
            }
        }
        // update second node prev
        if ( rel.getSecondPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship( rel.getSecondPrevRel() );
            getWriteLock( lockableRel );
            RelationshipRecord prevRel = relRecords.getOrLoad( rel.getSecondPrevRel(), null ).forChangingLinkage();
            boolean changed = false;
            if ( prevRel.getFirstNode() == rel.getSecondNode() )
            {
                prevRel.setFirstNextRel( rel.getSecondNextRel() );
                changed = true;
            }
            if ( prevRel.getSecondNode() == rel.getSecondNode() )
            {
                prevRel.setSecondNextRel( rel.getSecondNextRel() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( prevRel + " don't match " + rel );
            }
        }
        // update second node next
        if ( rel.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship( rel.getSecondNextRel() );
            getWriteLock( lockableRel );
            RelationshipRecord nextRel = relRecords.getOrLoad( rel.getSecondNextRel(), null ).forChangingLinkage();
            boolean changed = false;
            if ( nextRel.getFirstNode() == rel.getSecondNode() )
            {
                nextRel.setFirstPrevRel( rel.getSecondPrevRel() );
                changed = true;
            }
            if ( nextRel.getSecondNode() == rel.getSecondNode() )
            {
                nextRel.setSecondPrevRel( rel.getSecondPrevRel() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( nextRel + " don't match " + rel );
            }
        }
    }

    private void getWriteLock( Relationship lockableRel )
    {
        state.acquireWriteLock( lockableRel );
    }

    @Override
    public long getRelationshipChainPosition( long nodeId )
    {
        return nodeRecords.getOrLoad( nodeId, null ).getBefore().getNextRel();
    }

    @Override
    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships( long nodeId,
                                                                                                 long position )
    {
        return getMoreRelationships( nodeId, position, getRelGrabSize(), getRelationshipStore() );
    }

    private void updateNodes( RelationshipRecord rel )
    {
        if ( rel.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord firstNode = nodeRecords.getOrLoad( rel.getFirstNode(), null ).forChangingLinkage();
            firstNode.setNextRel( rel.getFirstNextRel() );
        }
        if ( rel.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord secondNode = nodeRecords.getOrLoad( rel.getSecondNode(), null ).forChangingLinkage();
            secondNode.setNextRel( rel.getSecondNextRel() );
        }
    }

    @Override
    public void relRemoveProperty( long relId, int propertyKey )
    {
        RecordChange<Long, RelationshipRecord, Void> rel = relRecords.getOrLoad( relId, null );
        RelationshipRecord relRecord = rel.forReadingLinkage();
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        assert assertPropertyChain( relRecord );
        removeProperty( relRecord, rel, propertyKey );
    }

    @Override
    public void relLoadProperties( long relId, boolean light, PropertyReceiver receiver )
    {
        RecordChange<Long, RelationshipRecord, Void> rel = relRecords.getIfLoaded( relId );
        if ( rel != null )
        {
            if ( rel.isCreated() )
            {
                return;
            }
            if ( !rel.forReadingLinkage().inUse() && !light )
            {
                throw new IllegalStateException( "Relationship[" + relId + "] has been deleted in this tx" );
            }
        }

        RelationshipRecord relRecord = relRecords.getOrLoad( relId, null ).forReadingLinkage();
        if ( !relRecord.inUse() )
        {
            throw new InvalidRecordException( "Relationship[" + relId + "] not in use" );
        }
        loadProperties( getPropertyStore(), relRecord.getNextProp(), receiver );
    }

    @Override
    public void nodeLoadProperties( long nodeId, boolean light, PropertyReceiver receiver )
    {
        RecordChange<Long, NodeRecord, Void> node = nodeRecords.getIfLoaded( nodeId );
        if ( node != null )
        {
            if ( node.isCreated() )
            {
                return;
            }
            if ( !node.forReadingLinkage().inUse() && !light )
            {
                throw new IllegalStateException( "Node[" + nodeId + "] has been deleted in this tx" );
            }
        }

        NodeRecord nodeRecord = nodeRecords.getOrLoad( nodeId, null ).forReadingLinkage();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Node[" + nodeId + "] has been deleted in this tx" );
        }
        loadProperties( getPropertyStore(), nodeRecord.getNextProp(), receiver );
    }

    @Override
    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        RecordChange<Long, NodeRecord, Void> node = nodeRecords.getOrLoad( nodeId, null );
        NodeRecord nodeRecord = node.forReadingLinkage();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on node[" +
                    nodeId + "] illegal since it has been deleted." );
        }
        assert assertPropertyChain( nodeRecord );

        removeProperty( nodeRecord, node, propertyKey );
    }

    private <P extends PrimitiveRecord> void removeProperty( P primitive,
            RecordChange<Long, P, Void> primitiveRecordChange, int propertyKey )
    {
        long propertyId = // propertyData.getId();
                findPropertyRecordContaining( primitive, propertyKey );
        RecordChange<Long, PropertyRecord, PrimitiveRecord> recordChange =
                propertyRecords.getOrLoad( propertyId, primitiveRecordChange.forReadingLinkage() );
        PropertyRecord propRecord = recordChange.forChangingData();
        if ( !propRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete property[" +
                    propertyId + "] since it is already deleted." );
        }

        PropertyBlock block = propRecord.removePropertyBlock( propertyKey );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyKey
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }

        for ( DynamicRecord valueRecord : block.getValueRecords() )
        {
            assert valueRecord.inUse();
            valueRecord.setInUse( false, block.getType().intValue() );
            propRecord.addDeletedRecord( valueRecord );
        }
        if ( propRecord.size() > 0 )
        {
            /*
             * There are remaining blocks in the record. We do not unlink yet.
             */
            propRecord.setChanged( primitiveRecordChange.forReadingLinkage() );
            assert assertPropertyChain( primitiveRecordChange.forReadingLinkage() );
        }
        else
        {
            unlinkPropertyRecord( propRecord, primitiveRecordChange );
        }
    }

    private <P extends PrimitiveRecord> void unlinkPropertyRecord( PropertyRecord propRecord,
                                                                   RecordChange<Long, P, Void> primitiveRecordChange )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert assertPropertyChain( primitive );
        assert propRecord.size() == 0;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if ( primitive.getNextProp() == propRecord.getId() )
        {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            primitiveRecordChange.forChangingLinkage().setNextProp( nextProp );
        }
        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord prevPropRecord = propertyRecords.getOrLoad( prevProp, primitive ).forChangingLinkage();
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
                                            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            prevPropRecord.setChanged( primitive );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = propertyRecords.getOrLoad( nextProp, primitive ).forChangingLinkage();
            assert nextPropRecord.inUse() : propRecord + "->" + nextPropRecord
                                            + " for " + primitive;
            nextPropRecord.setPrevProp( prevProp );
            nextPropRecord.setChanged( primitive );
        }
        propRecord.setInUse( false );
        /*
         *  The following two are not needed - the above line does all the work (PropertyStore
         *  does not write out the prev/next for !inUse records). It is nice to set this
         *  however to check for consistency when assertPropertyChain().
         */
        propRecord.setPrevProp( Record.NO_PREVIOUS_PROPERTY.intValue() );
        propRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
        propRecord.setChanged( primitive );
        assert assertPropertyChain( primitive );
    }

    @Override
    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        RecordChange<Long, RelationshipRecord, Void> rel = relRecords.getOrLoad( relId, null );
        if ( !rel.forReadingLinkage().inUse() )
        {
            throw new IllegalStateException( "Property change on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        return primitiveChangeProperty( rel, propertyKey, value );
    }

    @Override
    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        RecordChange<Long, NodeRecord, Void> node = nodeRecords.getOrLoad( nodeId, null ); //getNodeRecord( nodeId );
        if ( !node.forReadingLinkage().inUse() )
        {
            throw new IllegalStateException( "Property change on node[" +
                                             nodeId + "] illegal since it has been deleted." );
        }
        return primitiveChangeProperty( node, propertyKey, value );
    }

    /**
     * TODO MP: itroduces performance regression
     * This method was introduced during moving handling of entity properties from NodeImpl/RelationshipImpl
     * to the {@link KernelAPI}. Reason was that the {@link Property} object at the time didn't have a notion
     * of property record id, and didn't want to have it.
     */
    private long findPropertyRecordContaining( PrimitiveRecord primitive, int propertyKey )
    {
        long propertyRecordId = primitive.getNextProp();
        while ( !Record.NO_NEXT_PROPERTY.is( propertyRecordId ) )
        {
            PropertyRecord propertyRecord =
                    propertyRecords.getOrLoad( propertyRecordId, primitive ).forReadingLinkage();
            if ( propertyRecord.getPropertyBlock( propertyKey ) != null )
            {
                return propertyRecordId;
            }
            propertyRecordId = propertyRecord.getNextProp();
        }
        throw new IllegalStateException( "No property record in property chain for " + primitive +
                " contained property with key " + propertyKey );
    }

    private <P extends PrimitiveRecord> DefinedProperty primitiveChangeProperty(
            RecordChange<Long, P, Void> primitiveRecordChange, int propertyKey, Object value )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert assertPropertyChain( primitive );
        long propertyId = // propertyData.getId();
                findPropertyRecordContaining( primitive, propertyKey );
        PropertyRecord propertyRecord = propertyRecords.getOrLoad( propertyId, primitive ).forChangingData();
        if ( !propertyRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to change property["
                                             + propertyId
                                             + "] since it has been deleted." );
        }
        PropertyBlock block = propertyRecord.getPropertyBlock( propertyKey );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyKey
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }
        propertyRecord.setChanged( primitive );
        for ( DynamicRecord record : block.getValueRecords() )
        {
            assert record.inUse();
            record.setInUse( false, block.getType().intValue() );
            propertyRecord.addDeletedRecord( record );
        }
        getPropertyStore().encodeValue( block, propertyKey, value );
        if ( propertyRecord.size() > PropertyType.getPayloadSize() )
        {
            propertyRecord.removePropertyBlock( propertyKey );
            /*
             * The record should never, ever be above max size. Less obviously, it should
             * never remain empty. If removing a property because it won't fit when changing
             * it leaves the record empty it means that this block was the last one which
             * means that it doesn't fit in an empty record. Where i come from, we call this
             * weird.
             *
             assert propertyRecord.size() <= PropertyType.getPayloadSize() : propertyRecord;
             assert propertyRecord.size() > 0 : propertyRecord;
             */
            addPropertyBlockToPrimitive( block, primitiveRecordChange );
        }
        assert assertPropertyChain( primitive );
        return Property.property( propertyKey, value );
    }

    private <P extends PrimitiveRecord> DefinedProperty addPropertyToPrimitive(
            RecordChange<Long, P, Void> node, int propertyKey, Object value )
    {
        P record = node.forReadingLinkage();
        assert assertPropertyChain( record );
        PropertyBlock block = new PropertyBlock();
        getPropertyStore().encodeValue( block, propertyKey, value );
        addPropertyBlockToPrimitive( block, node );
        assert assertPropertyChain( record );
        return Property.property( propertyKey, value );
    }

    @Override
    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        RecordChange<Long, RelationshipRecord, Void> rel = relRecords.getOrLoad( relId, null );
        RelationshipRecord relRecord = rel.forReadingLinkage();
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        return addPropertyToPrimitive( rel, propertyKey, value );
    }

    @Override
    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        RecordChange<Long, NodeRecord, Void> node = nodeRecords.getOrLoad( nodeId, null );
        NodeRecord nodeRecord = node.forReadingLinkage();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on node[" +
                                             nodeId + "] illegal since it has been deleted." );
        }
        return addPropertyToPrimitive( node, propertyKey, value );
    }

    private <P extends PrimitiveRecord> void addPropertyBlockToPrimitive(
            PropertyBlock block, RecordChange<Long, P, Void> primitiveRecordChange )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert assertPropertyChain( primitive );
        int newBlockSizeInBytes = block.getSize();
        /*
         * Here we could either iterate over the whole chain or just go for the first record
         * which is the most likely to be the less full one. Currently we opt for the second
         * to perform better.
         */
        PropertyRecord host = null;
        long firstProp = primitive.getNextProp();
        if ( firstProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            // We do not store in map - might not have enough space
            RecordChange<Long, PropertyRecord, PrimitiveRecord> change = propertyRecords
                    .getOrLoad( firstProp, primitive );
            PropertyRecord propRecord = change.forReadingLinkage();
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            assert propRecord.inUse() : propRecord;
            int propSize = propRecord.size();
            assert propSize > 0 : propRecord;
            if ( propSize + newBlockSizeInBytes <= PropertyType.getPayloadSize() )
            {
                propRecord = change.forChangingData();
                host = propRecord;
                host.addPropertyBlock( block );
                host.setChanged( primitive );
            }
        }
        if ( host == null )
        {
            // First record in chain didn't fit, make new one
            host = propertyRecords.create( getPropertyStore().nextId(), primitive ).forChangingData();
            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord prevProp = propertyRecords.getOrLoad( primitive.getNextProp(), primitive )
                                                         .forChangingLinkage();
                assert prevProp.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue();
                prevProp.setPrevProp( host.getId() );
                host.setNextProp( prevProp.getId() );
                prevProp.setChanged( primitive );
            }
            primitiveRecordChange.forChangingLinkage().setNextProp( host.getId() );
            host.addPropertyBlock( block );
            host.setInUse( true );
        }
        // Ok, here host does for the job. Use it
        assert assertPropertyChain( primitive );
    }

    @Override
    public void relationshipCreate( long id, int type, long firstNodeId, long secondNodeId )
    {
        NodeRecord firstNode = nodeRecords.getOrLoad( firstNodeId, null ).forChangingLinkage();
        if ( !firstNode.inUse() )
        {
            throw new IllegalStateException( "First node[" + firstNodeId +
                                             "] is deleted and cannot be used to create a relationship" );
        }
        NodeRecord secondNode = nodeRecords.getOrLoad( secondNodeId, null ).forChangingLinkage();
        if ( !secondNode.inUse() )
        {
            throw new IllegalStateException( "Second node[" + secondNodeId +
                                             "] is deleted and cannot be used to create a relationship" );
        }
        RelationshipRecord record = relRecords.create( id, null ).forChangingLinkage();
        record.setLinks( firstNodeId, secondNodeId, type );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record );
    }

    private void connectRelationship( NodeRecord firstNode,
                                      NodeRecord secondNode, RelationshipRecord rel )
    {
        assert firstNode.getNextRel() != rel.getId();
        assert secondNode.getNextRel() != rel.getId();
        rel.setFirstNextRel( firstNode.getNextRel() );
        rel.setSecondNextRel( secondNode.getNextRel() );
        connect( firstNode, rel );
        connect( secondNode, rel );
        firstNode.setNextRel( rel.getId() );
        secondNode.setNextRel( rel.getId() );
    }

    private void connect( NodeRecord node, RelationshipRecord rel )
    {
        if ( node.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship( node.getNextRel() );
            getWriteLock( lockableRel );
            RelationshipRecord nextRel = relRecords.getOrLoad( node.getNextRel(), null ).forChangingLinkage();
            boolean changed = false;
            if ( nextRel.getFirstNode() == node.getId() )
            {
                nextRel.setFirstPrevRel( rel.getId() );
                changed = true;
            }
            if ( nextRel.getSecondNode() == node.getId() )
            {
                nextRel.setSecondPrevRel( rel.getId() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( node + " dont match " + nextRel );
            }
        }
    }

    @Override
    public void nodeCreate( long nodeId )
    {
        NodeRecord nodeRecord = nodeRecords.create( nodeId, null ).forChangingData();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
    }

    @Override
    public void createPropertyKeyToken( String key, int id )
    {
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( true );
        record.setCreated();
        PropertyKeyTokenStore propIndexStore = getPropertyStore().getPropertyKeyTokenStore();
        Collection<DynamicRecord> nameRecords =
                propIndexStore.allocateNameRecords( encodeString( key ) );
        record.setNameId( (int) first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
        addPropertyKeyTokenRecord( record );
    }

    @Override
    public void createLabelToken( String name, int id )
    {
        LabelTokenRecord record = new LabelTokenRecord( id );
        record.setInUse( true );
        record.setCreated();
        LabelTokenStore labelTokenStore = getLabelTokenStore();
        Collection<DynamicRecord> nameRecords =
                labelTokenStore.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
        addLabelIdRecord( record );
    }

    @Override
    public void createRelationshipTypeToken( int id, String name )
    {
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> typeNameRecords =
                getRelationshipTypeStore().allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) first( typeNameRecords ).getId() );
        record.addNameRecords( typeNameRecords );
        addRelationshipTypeRecord( record );
    }

    static class CommandSorter implements Comparator<Command>, Serializable
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

    void addRelationshipTypeRecord( RelationshipTypeTokenRecord record )
    {
        if ( relationshipTypeTokenRecords == null )
        {
            relationshipTypeTokenRecords = new HashMap<>();
        }
        relationshipTypeTokenRecords.put( record.getId(), record );
    }

    void addLabelIdRecord( LabelTokenRecord record )
    {
        if ( labelTokenRecords == null )
        {
            labelTokenRecords = new HashMap<>();
        }
        labelTokenRecords.put( record.getId(), record );
    }

    void addPropertyKeyTokenRecord( PropertyKeyTokenRecord record )
    {
        if ( propertyKeyTokenRecords == null )
        {
            propertyKeyTokenRecords = new HashMap<>();
        }
        propertyKeyTokenRecords.put( record.getId(), record );
    }

    private static class LockableRelationship implements Relationship
    {
        private final long id;

        LockableRelationship( long id )
        {
            this.id = id;
        }

        @Override
        public void delete()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getEndNode()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public long getId()
        {
            return this.id;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node[] getNodes()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getOtherNode( Node node )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object getProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getStartNode()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public RelationshipType getType()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean hasProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object removeProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public void setProperty( String key, Object value )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
        }

        @Override
        public int hashCode()
        {
            return (int) ((id >>> 32) ^ id);
        }

        @Override
        public String toString()
        {
            return "Lockable relationship #" + this.getId();
        }
    }

    @Override
    public void destroy()
    {
        xaConnection.destroy();
    }

    @Override
    public void setXaConnection( XaConnection connection )
    {
        this.xaConnection = connection;
    }

    private boolean assertPropertyChain( PrimitiveRecord primitive )
    {
        List<PropertyRecord> toCheck = new LinkedList<>();
        long nextIdToFetch = primitive.getNextProp();
        while ( nextIdToFetch != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyRecords.getOrLoad( nextIdToFetch, primitive ).forReadingLinkage();
            toCheck.add( propRecord );
            assert propRecord.inUse() : primitive + "->"
                                        + Arrays.toString( toCheck.toArray() );
            nextIdToFetch = propRecord.getNextProp();
        }
        if ( toCheck.isEmpty() )
        {
            assert primitive.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() : primitive;
            return true;
        }
        PropertyRecord first = toCheck.get( 0 );
        PropertyRecord last = toCheck.get( toCheck.size() - 1 );
        assert first.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : primitive
                                                                               + "->"
                                                                               + Arrays.toString( toCheck.toArray() );
        assert last.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() : primitive
                                                                          + "->"
                                                                          + Arrays.toString( toCheck.toArray() );
        PropertyRecord current, previous = first;
        for ( int i = 1; i < toCheck.size(); i++ )
        {
            current = toCheck.get( i );
            assert current.getPrevProp() == previous.getId() : primitive
                                                               + "->"
                                                               + Arrays.toString( toCheck.toArray() );
            assert previous.getNextProp() == current.getId() : primitive
                                                               + "->"
                                                               + Arrays.toString( toCheck.toArray() );
            previous = current;
        }
        return true;
    }

    private RecordChange<Long, NeoStoreRecord, Void> getOrLoadNeoStoreRecord()
    {
        if ( neoStoreRecord == null )
        {
            neoStoreRecord = new RecordChanges<>( new RecordChanges.Loader<Long, NeoStoreRecord, Void>()
            {
                @Override
                public NeoStoreRecord newUnused( Long key, Void additionalData )
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public NeoStoreRecord load( Long key, Void additionalData )
                {
                    return neoStore.asRecord();
                }

                @Override
                public void ensureHeavy( NeoStoreRecord record )
                {
                }

                @Override
                public NeoStoreRecord clone(NeoStoreRecord neoStoreRecord) {
                    // We do not expect to manage the before state, so this operation will not be called.
                    throw new UnsupportedOperationException("Clone on NeoStoreRecord");
                }
            }, false );
        }
        return neoStoreRecord.getOrLoad( 0L, null );
    }

    @Override
    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        PropertyBlock block = new PropertyBlock();
        /*
         * Encoding has to be set here before anything is changed,
         * since an exception could be thrown in encodeValue now and tx not marked
         * rollback only.
         */
        getPropertyStore().encodeValue( block, propertyKey, value );
        RecordChange<Long, NeoStoreRecord, Void> change = getOrLoadNeoStoreRecord();
        addPropertyBlockToPrimitive( block, change );
        assert assertPropertyChain( change.forReadingLinkage() );
        return Property.property( propertyKey, value );
    }

    @Override
    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        return primitiveChangeProperty( getOrLoadNeoStoreRecord(), propertyKey, value );
    }

    @Override
    public void graphRemoveProperty( int propertyKey )
    {
        RecordChange<Long, NeoStoreRecord, Void> recordChange = getOrLoadNeoStoreRecord();
        removeProperty( recordChange.forReadingLinkage(), recordChange, propertyKey );
    }

    @Override
    public void graphLoadProperties( boolean light, PropertyReceiver records )
    {
        loadProperties( getPropertyStore(),
                getOrLoadNeoStoreRecord().forReadingLinkage().getNextProp(), records );
    }

    @Override
    public void createSchemaRule( SchemaRule schemaRule )
    {
        for(DynamicRecord change : schemaRuleChanges.create( schemaRule.getId(), schemaRule ).forChangingData())
        {
            change.setInUse( true );
            change.setCreated();
        }
    }

    @Override
    public void dropSchemaRule( SchemaRule rule )
    {
        RecordChange<Long, Collection<DynamicRecord>, SchemaRule> change =
                schemaRuleChanges.getOrLoad(rule.getId(), rule);
        Collection<DynamicRecord> records = change.forChangingData();
        for ( DynamicRecord record : records )
        {
            record.setInUse( false );
        }
    }

    @Override
    public void addLabelToNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = nodeRecords.getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).add( labelId, getNodeStore() );
    }

    @Override
    public void removeLabelFromNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = nodeRecords.getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).remove( labelId, getNodeStore() );
    }

    @Override
    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        // Don't consider changes in this transaction
        NodeRecord node = getNodeStore().getRecord( nodeId );
        return asPrimitiveIterator( parseLabelsField( node ).get( getNodeStore() ) );
    }

    @Override
    public void setConstraintIndexOwner( IndexRule indexRule, long constraintId )
    {
        RecordChange<Long, Collection<DynamicRecord>, SchemaRule> change =
                schemaRuleChanges.getOrLoad( indexRule.getId(), indexRule );
        Collection<DynamicRecord> records = change.forChangingData();

        indexRule = indexRule.withOwningConstraint( constraintId );

        records.clear();
        records.addAll( getSchemaStore().allocateFrom( indexRule ) );
    }

    private Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position, int grabSize, RelationshipStore relStore )
    {
        // initialCapacity=grabSize saves the lists the trouble of resizing
        List<RelationshipRecord> out = new ArrayList<>();
        List<RelationshipRecord> in = new ArrayList<>();
        List<RelationshipRecord> loop = null;
        Map<DirectionWrapper, Iterable<RelationshipRecord>> result = new EnumMap<>( DirectionWrapper.class );
        result.put( DirectionWrapper.OUTGOING, out );
        result.put( DirectionWrapper.INCOMING, in );
        for ( int i = 0; i < grabSize &&
                position != Record.NO_NEXT_RELATIONSHIP.intValue(); i++ )
        {
            RelationshipRecord relRecord = relStore.getChainRecord( position );
            if ( relRecord == null )
            {
                // return what we got so far
                return Pair.of( result, position );
            }
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( relRecord.inUse() )
            {
                if ( firstNode == secondNode )
                {
                    if ( loop == null )
                    {
                        // This is done lazily because loops are probably quite
                        // rarely encountered
                        loop = new ArrayList<>();
                        result.put( DirectionWrapper.BOTH, loop );
                    }
                    loop.add( relRecord );
                }
                else if ( firstNode == nodeId )
                {
                    out.add( relRecord );
                }
                else if ( secondNode == nodeId )
                {
                    in.add( relRecord );
                }
            }
            else
            {
                i--;
            }

            if ( firstNode == nodeId )
            {
                position = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                position = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                        "] is neither firstNode[" + firstNode +
                        "] nor secondNode[" + secondNode + "] for Relationship[" + relRecord.getId() + "]" );
            }
        }
        return Pair.of( result, position );
    }

    private static void loadPropertyChain( Collection<PropertyRecord> chain, PropertyStore propertyStore,
                                   PropertyReceiver receiver )
    {
        if ( chain != null )
        {
            for ( PropertyRecord propRecord : chain )
            {
                for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
                {
                    receiver.receive( propBlock.newPropertyData( propertyStore ), propRecord.getId() );
                }
            }
        }
    }

    static void loadProperties(
            PropertyStore propertyStore, long nextProp, PropertyReceiver receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp );
        if ( chain != null )
        {
            loadPropertyChain( chain, propertyStore, receiver );
        }
    }
}
