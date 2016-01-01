/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdateNodeIdComparator;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexUpdates;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.DenseNodeChainPosition;
import org.neo4j.kernel.impl.core.RelationshipLoadingPosition;
import org.neo4j.kernel.impl.core.SingleChainPosition;
import org.neo4j.kernel.impl.core.Token;
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
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenStore;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.nioneo.xa.RecordChanges.RecordChange;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoXaCommandExecutor;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.copyOf;

import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.nioneo.xa.command.Command.*;
import static org.neo4j.kernel.impl.nioneo.xa.command.Command.Mode.CREATE;
import static org.neo4j.kernel.impl.nioneo.xa.command.Command.Mode.DELETE;
import static org.neo4j.kernel.impl.nioneo.xa.command.Command.Mode.UPDATE;

/**
 * Transaction containing {@link org.neo4j.kernel.impl.nioneo.xa.command.Command commands} reflecting the operations
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
public class NeoStoreTransaction extends XaTransaction
{
    private final Map<Long, Map<Integer, RelationshipGroupRecord>> relGroupCache = new HashMap<>();
    private RecordChanges<Long, NeoStoreRecord, Void> neoStoreRecord;

    private boolean committed = false;
    private boolean prepared = false;

    private final long lastCommittedTxWhenTransactionStarted;
    private final CacheAccessBackDoor cacheAccess;
    private final IndexingService indexes;
    private final NeoStore neoStore;
    private final LabelScanStore labelScanStore;
    private final IntegrityValidator integrityValidator;
    private final KernelTransactionImplementation kernelTransaction;
    private final LockService locks;

    private final NeoStoreTransactionContext context;
    private final NeoXaCommandExecutor executor;

    private ValidatedIndexUpdates indexUpdates = ValidatedIndexUpdates.NO_UPDATES;

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
     * @param context
     */
    NeoStoreTransaction( long lastCommittedTxWhenTransactionStarted, XaLogicalLog log,
                         NeoStore neoStore, CacheAccessBackDoor cacheAccess,
                         IndexingService indexingService, LabelScanStore labelScanStore,
                         IntegrityValidator integrityValidator, KernelTransactionImplementation kernelTransaction,
                         LockService locks, NeoStoreTransactionContext context )
    {
        super( log, context.getTransactionState() );
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.neoStore = neoStore;
        this.executor = new NeoXaCommandExecutor( neoStore, indexingService );
        this.cacheAccess = cacheAccess;
        this.indexes = indexingService;
        this.labelScanStore = labelScanStore;
        this.integrityValidator = integrityValidator;
        this.kernelTransaction = kernelTransaction;
        this.locks = locks;
        this.context = context;
    }

    /**
     * This is a smell, a result of the kernel refactorings. Right now, both NeoStoreTransaction and KernelTransaction
     * are "publicly" consumable, and one owns the other. In the future, they should be merged such that
     * KernelTransaction rules supreme, and has internal components to manage the responsibilities currently handled by
     * WriteTransaction and ReadTransaction.
     */
    public KernelTransactionImplementation kernelTransaction()
    {
        return kernelTransaction;
    }

    @Override
    public boolean isReadOnly()
    {
        if ( isRecovered() )
        {
            return context.getNodeCommands().isEmpty() && context.getPropCommands().isEmpty() &&
                    context.getRelCommands().isEmpty() && context.getSchemaRuleCommands().isEmpty() &&
                    context.getRelationshipTypeTokenCommands().isEmpty() &&
                    context.getLabelTokenCommands().isEmpty() &&
                    context.getRelGroupCommands().isEmpty() &&
                    context.getPropertyKeyTokenCommands().isEmpty() && kernelTransaction.isReadOnly();
        }
        return context.getNodeRecords().changeSize() == 0 && context.getRelRecords().changeSize() == 0 &&
                context.getSchemaRuleChanges().changeSize() == 0 &&
                context.getPropertyRecords().changeSize() == 0 &&
                context.getRelGroupRecords().changeSize() == 0 &&
                context.getPropertyKeyTokenRecords().changeSize() == 0 &&
                context.getLabelTokenRecords().changeSize() == 0 &&
                context.getRelationshipTypeTokenRecords().changeSize() == 0 &&
                kernelTransaction.isReadOnly();
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

        prepared = true;

        int noOfCommands = context.getNodeRecords().changeSize() +
                           context.getRelRecords().changeSize() +
                           context.getPropertyRecords().changeSize() +
                           context.getSchemaRuleChanges().changeSize() +
                           context.getPropertyKeyTokenRecords().changeSize() +
                           context.getLabelTokenRecords().changeSize() +
                           context.getRelationshipTypeTokenRecords().changeSize() +
                           context.getRelGroupRecords().changeSize();
        List<Command> commands = new ArrayList<>( noOfCommands );
        for ( RecordProxy<Integer, LabelTokenRecord, Void> record : context.getLabelTokenRecords().changes() )
        {
            LabelTokenCommand command = new LabelTokenCommand();
            command.init(  record.forReadingLinkage()  );
            context.getLabelTokenCommands().add( command );
            commands.add( command );
        }
        for ( RecordProxy<Integer, RelationshipTypeTokenRecord, Void> record : context.getRelationshipTypeTokenRecords().changes() )
        {
            RelationshipTypeTokenCommand command = new RelationshipTypeTokenCommand();
            command.init( record.forReadingLinkage() );
            context.getRelationshipTypeTokenCommands().add( command );
            commands.add( command );
        }
        for ( RecordChange<Long, NodeRecord, Void> change : context.getNodeRecords().changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            integrityValidator.validateNodeRecord( record );
            NodeCommand command = new NodeCommand();
            command.init( change.getBefore(), record );
            context.getNodeCommands().put( record.getId(), command );
            commands.add( command );
        }
        for ( RecordProxy<Long, RelationshipRecord, Void> record : context.getRelRecords().changes() )
        {
            RelationshipCommand command = new RelationshipCommand();
            command.init(  record.forReadingLinkage()  );
            context.getRelCommands().add( command );
            commands.add( command );
        }
        if ( neoStoreRecord != null )
        {
            for ( RecordProxy<Long, NeoStoreRecord, Void> change : neoStoreRecord.changes() )
            {
                context.generateNeoStoreCommand( change.forReadingData() );
                addCommand( context.getNeoStoreCommand() );
            }
        }
        for ( RecordProxy<Integer, PropertyKeyTokenRecord, Void> record : context.getPropertyKeyTokenRecords().changes() )
        {
            PropertyKeyTokenCommand command =
                    new PropertyKeyTokenCommand();
            command.init( record.forReadingLinkage() );
            context.getPropertyKeyTokenCommands().add( command );
            commands.add( command );
        }
        for ( RecordChange<Long, PropertyRecord, PrimitiveRecord> change : context.getPropertyRecords().changes() )
        {
            PropertyCommand command = new PropertyCommand();
            command.init( change.getBefore(), change.forReadingLinkage() );
            context.getPropCommands().add( command );
            commands.add( command );
        }
        for ( RecordChange<Long, Collection<DynamicRecord>, SchemaRule> change : context.getSchemaRuleChanges().changes() )
        {
            integrityValidator.validateSchemaRule( change.getAdditionalData() );
            SchemaRuleCommand command = new SchemaRuleCommand();
            command.init( change.getBefore(), change.forChangingData(), change.getAdditionalData(), -1 );
            context.getSchemaRuleCommands().add( command );
            commands.add( command );
        }
        for ( RecordProxy<Long, RelationshipGroupRecord, Integer> change : context.getRelGroupRecords().changes() )
        {
            RelationshipGroupCommand command = new RelationshipGroupCommand();
            command.init( change.forReadingData() );
            context.getRelGroupCommands().add( command );
            commands.add( command );
        }
        assert commands.size() == noOfCommands : "Expected " + noOfCommands
                                                 + " final commands, got "
                                                 + commands.size() + " instead";
        intercept( commands );

        validateIndexUpdates();

        for ( Command command : commands )
        {
            addCommand( command );
        }

        integrityValidator.validateTransactionStartKnowledge( lastCommittedTxWhenTransactionStarted );
    }

    private void validateIndexUpdates()
    {
        if ( !context.getNodeCommands().isEmpty() || !context.getPropCommands().isEmpty() )
        {
            IndexUpdates rawIndexUpdates = new LazyIndexUpdates( getNodeStore(), getPropertyStore(),
                    groupedNodePropertyCommands( context.getPropCommands() ), context.getNodeCommands() );

            indexUpdates = indexes.validate( rawIndexUpdates );
        }
    }

    protected void intercept( List<Command> commands )
    {
        // default no op
    }

    @Override
    protected void injectCommand( XaCommand xaCommand )
    {
        if ( xaCommand instanceof NodeCommand )
        {
            NodeCommand nodeCommand = (NodeCommand) xaCommand;
            context.getNodeCommands().put( nodeCommand.getKey(), nodeCommand );
        }
        else if ( xaCommand instanceof RelationshipCommand )
        {
            context.getRelCommands().add( (RelationshipCommand) xaCommand );
        }
        else if ( xaCommand instanceof PropertyCommand )
        {
            context.getPropCommands().add( (PropertyCommand) xaCommand );
        }
        else if ( xaCommand instanceof PropertyKeyTokenCommand )
        {
            context.getPropertyKeyTokenCommands().add( (PropertyKeyTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof RelationshipTypeTokenCommand )
        {
            context.getRelationshipTypeTokenCommands().add( (RelationshipTypeTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof LabelTokenCommand )
        {
            context.getLabelTokenCommands().add( (LabelTokenCommand) xaCommand );
        }
        else if ( xaCommand instanceof NeoStoreCommand )
        {
            assert context.getNeoStoreCommand().getRecord() == null;
            context.setNeoStoreCommand( (NeoStoreCommand) xaCommand );
        }
        else if ( xaCommand instanceof SchemaRuleCommand )
        {
            context.getSchemaRuleCommands().add( (SchemaRuleCommand) xaCommand );
        }
        else if ( xaCommand instanceof RelationshipGroupCommand )
        {
            context.getRelGroupCommands().add( (RelationshipGroupCommand) xaCommand );
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
            for ( RecordProxy<Long, NodeRecord, Void> change : context.getNodeRecords().changes() )
            {
                NodeRecord record = change.forReadingLinkage();
                if ( freeIds && record.isCreated() )
                {
                    getNodeStore().freeId( record.getId() );
                }
                removeNodeFromCache( record.getId() );
            }
            for ( RecordChange<Long, RelationshipRecord, Void> change : context.getRelRecords().changes() )
            {
                long id = change.getKey();
                RelationshipRecord record = change.forReadingLinkage();
                if ( freeIds && change.isCreated() )
                {
                    getRelationshipStore().freeId( id );
                }
                removeRelationshipFromCache( id );
            }
            if ( neoStoreRecord != null )
            {
                removeGraphPropertiesFromCache();
            }

            rollbackTokenRecordChanges( context.getPropertyKeyTokenRecords(),
                    getPropertyKeyTokenStore(), freeIds, PROPERTY_KEY_CACHE_REMOVER );
            rollbackTokenRecordChanges( context.getLabelTokenRecords(), getLabelTokenStore(), freeIds,
                    LABEL_CACHE_REMOVER );
            rollbackTokenRecordChanges( context.getRelationshipTypeTokenRecords(), getRelationshipTypeStore(),
                    freeIds, RELATIONSHIP_TYPE_CACHE_REMOVER );

            for ( RecordProxy<Long, PropertyRecord, PrimitiveRecord> change : context.getPropertyRecords().changes() )
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
            for ( RecordProxy<Long, Collection<DynamicRecord>, SchemaRule> records : context.getSchemaRuleChanges().changes() )
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
            for ( RecordProxy<Long, RelationshipGroupRecord, Integer> change : context.getRelGroupRecords().changes() )
            {
                RelationshipGroupRecord record = change.forReadingData();
                if ( freeIds && record.isCreated() )
                {
                    getRelationshipGroupStore().freeId( record.getId() );
                }
            }
        }
        finally
        {
            indexUpdates.close();
            clear();
        }
    }

    private interface CacheRemover
    {
        void remove( CacheAccessBackDoor cacheAccess, int id );
    }

    private static final CacheRemover PROPERTY_KEY_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removePropertyKeyFromCache( id );
        }
    };

    private static final CacheRemover RELATIONSHIP_TYPE_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removeRelationshipTypeFromCache( id );
        }
    };

    private static final CacheRemover LABEL_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removeLabelFromCache( id );
        }
    };

    private <T extends TokenRecord> void rollbackTokenRecordChanges( RecordChanges<Integer, T, Void> records,
            TokenStore<T> store, boolean freeIds, CacheRemover cacheRemover )
    {
        for ( RecordChange<Integer, T, Void> record : records.changes() )
        {
            if ( record.isCreated() )
            {
                if ( freeIds )
                {
                    store.freeId( record.getKey() );
                }
                for ( DynamicRecord dynamicRecord : record.forReadingLinkage().getNameRecords() )
                {
                    if ( dynamicRecord.isCreated() )
                    {
                        store.getNameStore().freeId( (int) dynamicRecord.getId() );
                    }
                }
            }
            cacheRemover.remove( cacheAccess, record.getKey() );
        }
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
                      neoStore.getPropertyKeyTokenStore().getToken( id, true ) :
                      neoStore.getPropertyKeyTokenStore().getToken( id );
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
            catch( IOException e )
            {
                throw Exceptions.withCause( new XAException(), e );
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

        try
        {
            applyCommit( false );
        }
        catch( IOException e )
        {
            throw Exceptions.withCause( new XAException(), e );
        }
    }

    private void applyCommit( boolean isRecovered ) throws IOException
    {
        try ( LockGroup lockGroup = new LockGroup() )
        {
            if ( isRecovered && !prepared ) // no preparation of this tx happened - need to validate index updates now
            {
                validateIndexUpdates();
            }

            committed = true;
            CommandSorter sorter = new CommandSorter();
            // reltypes
            if ( context.getRelationshipTypeTokenCommands().size() != 0 )
            {
                java.util.Collections.sort( context.getRelationshipTypeTokenCommands(), sorter );
                for ( RelationshipTypeTokenCommand command : context.getRelationshipTypeTokenCommands() )
                {
                    executor.execute( command );
                    if ( isRecovered )
                    {
                        addRelationshipType( (int) command.getKey() );
                    }
                }
            }
            // label keys
            if ( context.getLabelTokenCommands().size() != 0 )
            {
                java.util.Collections.sort( context.getLabelTokenCommands(), sorter );
                for ( LabelTokenCommand command : context.getLabelTokenCommands() )
                {
                    executor.execute( command );
                    if ( isRecovered )
                    {
                        addLabel( (int) command.getKey() );
                    }
                }
            }
            // property keys
            if ( context.getPropertyKeyTokenCommands().size() != 0 )
            {
                java.util.Collections.sort( context.getPropertyKeyTokenCommands(), sorter );
                for ( PropertyKeyTokenCommand command : context.getPropertyKeyTokenCommands() )
                {
                    executor.execute( command );
                    if ( isRecovered )
                    {
                        addPropertyKey( (int) command.getKey() );
                    }
                }
            }

            // primitives
            java.util.Collections.sort( context.getRelCommands(), sorter );
            java.util.Collections.sort( context.getPropCommands(), sorter );
            executeCreated( lockGroup, isRecovered, context.getPropCommands(), context.getRelCommands(),
                    context.getRelGroupCommands(), context.getNodeCommands().values() );
            executeModified( lockGroup, isRecovered, context.getPropCommands(), context.getRelCommands(),
                    context.getRelGroupCommands(), context.getNodeCommands().values() );
            executeDeleted( lockGroup, context.getPropCommands(), context.getRelCommands(),
                    context.getRelGroupCommands(), context.getNodeCommands().values() );

            // property change set for index updates
            Collection<NodeLabelUpdate> labelUpdates = gatherLabelUpdatesSortedByNodeId();
            if ( !labelUpdates.isEmpty() )
            {
                updateLabelScanStore( labelUpdates );
                cacheAccess.applyLabelUpdates( labelUpdates );
            }

            indexes.updateIndexes( indexUpdates );

            // schema rules. Execute these after generating the property updates so. If executed
            // before and we've got a transaction that sets properties/labels as well as creating an index
            // we might end up with this corner-case:
            // 1) index rule created and index population job started
            // 2) index population job processes some nodes, but doesn't complete
            // 3) we gather up property updates and send those to the indexes. The newly created population
            //    job might get those as updates
            // 4) the population job will apply those updates as added properties, and might end up with duplicate
            //    entries for the same property
            for ( SchemaRuleCommand command : context.getSchemaRuleCommands() )
            {
                command.setTxId( getCommitTxId() );
                executor.execute( command );
                switch ( command.getMode() )
                {
                case DELETE:
                    cacheAccess.removeSchemaRuleFromCache( command.getKey() );
                    break;
                default:
                    cacheAccess.addSchemaRule( command.getSchemaRule() );
                }
            }

            if ( context.getNeoStoreCommand().getRecord() != null )
            {
                executor.execute( context.getNeoStoreCommand() );
                if ( isRecovered )
                {
                    removeGraphPropertiesFromCache();
                }
            }
            if ( !isRecovered )
            {
                context.updateFirstRelationships();
                context.commitCows( cacheAccess ); // updates the cached primitives
            }
            neoStore.setLastCommittedTx( getCommitTxId() );
            if ( isRecovered )
            {
                neoStore.updateIdGenerators();
            }
        }
        finally
        {
            indexUpdates.close();
        }
    }

    private Map<Long,List<Command.PropertyCommand>> groupedNodePropertyCommands(
            Iterable<Command.PropertyCommand> propCommands )
    {
        // A bit too expensive data structure, but don't know off the top of my head how to make it better.
        Map<Long, List<Command.PropertyCommand>> groups = new HashMap<>();
        for ( Command.PropertyCommand command : propCommands )
        {
            PropertyRecord record = command.getAfter();
            if ( !record.isNodeSet() )
            {
                continue;
            }

            long nodeId = command.getAfter().getNodeId();
            List<Command.PropertyCommand> group = groups.get( nodeId );
            if ( group == null )
            {
                groups.put( nodeId, group = new ArrayList<>() );
            }
            group.add( command );
        }
        return groups;
    }

    private Collection<NodeLabelUpdate> gatherLabelUpdatesSortedByNodeId()
    {
        List<NodeLabelUpdate> labelUpdates = new ArrayList<>();
        for ( NodeCommand nodeCommand : context.getNodeCommands().values() )
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

        Collections.sort(labelUpdates, new NodeLabelUpdateNodeIdComparator());

        return labelUpdates;
    }

    private void updateLabelScanStore( Iterable<NodeLabelUpdate> labelUpdates )
    {
        try ( LabelScanWriter writer = labelScanStore.newWriter() )
        {
            for ( NodeLabelUpdate update : labelUpdates )
            {
                writer.write( update );
            }
        }
        catch ( IOException | IndexCapacityExceededException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        context.relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public ArrayMap<Integer, DefinedProperty> relDelete( long relId )
    {
        return context.relationshipDelete( relId );
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

    @SafeVarargs
    private final void executeCreated( LockGroup lockGroup, boolean removeFromCache,
                                       Collection<? extends Command>... commands ) throws IOException
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == CREATE )
                {
                    lockEntity( lockGroup, command );
                    executor.execute( command );
                    if ( removeFromCache )
                    {
                        command.applyToCache( cacheAccess );
                    }
                }
            }
        }
    }

    @SafeVarargs
    private final void executeModified( LockGroup lockGroup, boolean removeFromCache,
                                        Collection<? extends Command>... commands ) throws IOException
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == UPDATE )
                {
                    lockEntity( lockGroup, command );
                    executor.execute( command );
                    if ( removeFromCache )
                    {
                        command.applyToCache( cacheAccess );
                    }
                }
            }
        }
    }

    @SafeVarargs
    private final void executeDeleted( LockGroup lockGroup, Collection<? extends Command>... commands ) throws IOException
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
                    executor.execute( command );
                    command.applyToCache( cacheAccess );
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
        if ( command instanceof PropertyCommand )
        {
            long nodeId = ((PropertyCommand) command).getNodeId();
            if ( nodeId != -1 )
            {
                lockGroup.add( locks.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK ) );
            }
        }
    }

    private void clear()
    {
        context.close();
        relGroupCache.clear();
        neoStoreRecord = null;
        indexUpdates = ValidatedIndexUpdates.NO_UPDATES;
    }

    private RelationshipTypeTokenStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    private LabelTokenStore getLabelTokenStore()
    {
        return neoStore.getLabelTokenStore();
    }

    private PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return neoStore.getPropertyKeyTokenStore();
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

    private RelationshipGroupStore getRelationshipGroupStore()
    {
        return neoStore.getRelationshipGroupStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    /**
     * Tries to load the light node with the given id, returns true on success.
     *
     * @param nodeId The id of the node to load.
     * @return True iff the node record can be found.
     */
    public NodeRecord nodeLoadLight( long nodeId )
    {
        try
        {
            return context.getNodeRecords().getOrLoad( nodeId, null ).forReadingLinkage();
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }
    }

    /**
     * Tries to load the light relationship with the given id, returns the
     * record on success.
     *
     * @param id The id of the relationship to load.
     * @return The light RelationshipRecord if it was found, null otherwise.
     */
    public RelationshipRecord relLoadLight( long id )
    {
        try
        {
            return context.getRelRecords().getOrLoad( id, null ).forReadingLinkage();
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }
    }

    /**
     * Deletes a node by its id, returning its properties which are now removed.
     *
     * @param nodeId The id of the node to delete.
     * @return The properties of the node that were removed during the delete.
     */
    public ArrayMap<Integer, DefinedProperty> nodeDelete( long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete Node[" + nodeId +
                                             "] since it has already been deleted." );
        }
        nodeRecord.setInUse( false );
        nodeRecord.setLabelField( 0, Collections.<DynamicRecord>emptyList() );
        return getAndDeletePropertyChain( nodeRecord );
    }

    private ArrayMap<Integer, DefinedProperty> getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        return context.getAndDeletePropertyChain( nodeRecord );
    }

    /*
     * List<Iterable<RelationshipRecord>> is a list with three items:
     * 0: outgoing relationships
     * 1: incoming relationships
     * 2: loop relationships
     */
    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, RelationshipLoadingPosition> getMoreRelationships(
            long nodeId, RelationshipLoadingPosition position, DirectionWrapper direction,
            int[] types )
    {
        return getMoreRelationships( nodeId, position, getRelGrabSize(), direction, types, getRelationshipStore() );
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
        RecordProxy<Long, RelationshipRecord, Void> rel = context.getRelRecords().getOrLoad( relId, null );
        RelationshipRecord relRecord = rel.forReadingLinkage();
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        context.removeProperty( rel, propertyKey );
    }

    /**
     * Loads the complete property chain for the given relationship and returns
     * it as a map from property index id to property data.
     *
     * @param relId The id of the relationship whose properties to load.
     * @param light If the properties should be loaded light or not.
     * @param receiver receiver of loaded properties.
     */
    public void relLoadProperties( long relId, boolean light, PropertyReceiver receiver )
    {
        RecordChange<Long, RelationshipRecord, Void> rel = context.getRelRecords().getIfLoaded( relId );
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

        RelationshipRecord relRecord = getRelationshipStore().getRecord( relId );
        if ( !relRecord.inUse() )
        {
            throw new InvalidRecordException( "Relationship[" + relId + "] not in use" );
        }
        loadProperties( getPropertyStore(), relRecord.getNextProp(), receiver );
    }

    /**
     * Loads the complete property chain for the given node and returns it as a
     * map from property index id to property data.
     *
     * @param nodeId The id of the node whose properties to load.
     * @param light If the properties should be loaded light or not.
     * @param receiver receiver of loaded properties.
     */
    public void nodeLoadProperties( long nodeId, boolean light, PropertyReceiver receiver )
    {
        RecordChange<Long, NodeRecord, Void> node = context.getNodeRecords().getIfLoaded( nodeId );
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

        NodeRecord nodeRecord = getNodeStore().getRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Node[" + nodeId + "] has been deleted in this tx" );
        }
        loadProperties( getPropertyStore(), nodeRecord.getNextProp(), receiver );
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
        RecordProxy<Long, NodeRecord, Void> node = context.getNodeRecords().getOrLoad( nodeId, null );
        NodeRecord nodeRecord = node.forReadingLinkage();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on node[" +
                    nodeId + "] illegal since it has been deleted." );
        }
        context.removeProperty( node, propertyKey );
    }

    /**
     * Changes an existing property's value of the given relationship, with the
     * given index to the passed value
     *
     * @param relId The id of the relationship which holds the property to
     *            change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        RecordProxy<Long, RelationshipRecord, Void> rel = context.getRelRecords().getOrLoad( relId, null );
        if ( !rel.forReadingLinkage().inUse() )
        {
            throw new IllegalStateException( "Property change on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        context.primitiveChangeProperty( rel, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     *
     * @param nodeId The id of the node which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        RecordProxy<Long, NodeRecord, Void> node = context.getNodeRecords().getOrLoad( nodeId, null ); //getNodeRecord( nodeId );
        if ( !node.forReadingLinkage().inUse() )
        {
            throw new IllegalStateException( "Property change on node[" +
                                             nodeId + "] illegal since it has been deleted." );
        }
        context.primitiveChangeProperty( node, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     *
     * @param relId The id of the relationship to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        RecordProxy<Long, RelationshipRecord, Void> rel = context.getRelRecords().getOrLoad( relId, null );
        RelationshipRecord relRecord = rel.forReadingLinkage();
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on relationship[" +
                                             relId + "] illegal since it has been deleted." );
        }
        context.primitiveAddProperty( rel, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Adds a property to the given node, with the given index and value.
     *
     * @param nodeId The id of the node to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        RecordProxy<Long, NodeRecord, Void> node = context.getNodeRecords().getOrLoad( nodeId, null );
        NodeRecord nodeRecord = node.forReadingLinkage();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on node[" +
                                             nodeId + "] illegal since it has been deleted." );
        }
        context.primitiveAddProperty( node, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Creates a node for the given id
     *
     * @param nodeId The id of the node to create.
     */
    public void nodeCreate( long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().create( nodeId, null ).forChangingData();
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
        context.createPropertyKeyToken( key, id );
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param name The key of the property index, as a string.
     * @param id The property index record id.
     */
    public void createLabelToken( String name, int id )
    {
        context.createLabelToken( name, id );
    }

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param id The id of the new relationship type record.
     * @param name The name of the relationship type.
     */
    public void createRelationshipTypeToken( int id, String name )
    {
        context.createRelationshipTypeToken( name, id );
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

    private RecordProxy<Long, NeoStoreRecord, Void> getOrLoadNeoStoreRecord()
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

    /**
     * Adds a property to the graph, with the given index and value.
     *
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        context.primitiveAddProperty( getOrLoadNeoStoreRecord(), propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Changes an existing property of the graph, with the given index to
     * the passed value
     *
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        context.primitiveChangeProperty( getOrLoadNeoStoreRecord(), propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Removes the given property identified by indexKeyId of the graph with the
     * given id.
     *
     * @param propertyKey The index key of the property.
     */
    public void graphRemoveProperty( int propertyKey )
    {
        RecordProxy<Long, NeoStoreRecord, Void> recordChange = getOrLoadNeoStoreRecord();
        context.removeProperty( recordChange, propertyKey );
    }

    /**
     * Loads the complete property chain for the graph and returns it as a
     * map from property index id to property data.
     *
     * @param light If the properties should be loaded light or not.
     * @param records receiver of loaded properties.
     */
    public void graphLoadProperties( boolean light, PropertyReceiver records )
    {
        loadProperties( getPropertyStore(), neoStore.asRecord().getNextProp(), records );
    }

    public void createSchemaRule( SchemaRule schemaRule )
    {
        for(DynamicRecord change : context.getSchemaRuleChanges().create( schemaRule.getId(), schemaRule ).forChangingData())
        {
            change.setInUse( true );
            change.setCreated();
        }
    }

    public void dropSchemaRule( SchemaRule rule )
    {
        RecordProxy<Long, Collection<DynamicRecord>, SchemaRule> change =
                context.getSchemaRuleChanges().getOrLoad( rule.getId(), rule );
        Collection<DynamicRecord> records = change.forChangingData();
        for ( DynamicRecord record : records )
        {
            record.setInUse( false );
        }
    }

    public void addLabelToNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).add( labelId, getNodeStore(), getNodeStore().getDynamicLabelStore() );
    }

    public void removeLabelFromNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).remove( labelId, getNodeStore() );
    }

    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        // Don't consider changes in this transaction
        NodeRecord node = getNodeStore().getRecord( nodeId );
        return PrimitiveLongCollections.iterator( parseLabelsField( node ).get( getNodeStore() ) );
    }

    public void setConstraintIndexOwner( IndexRule indexRule, long constraintId )
    {
        RecordProxy<Long, Collection<DynamicRecord>, SchemaRule> change =
                context.getSchemaRuleChanges().getOrLoad( indexRule.getId(), indexRule );
        Collection<DynamicRecord> records = change.forChangingData();

        indexRule = indexRule.withOwningConstraint( constraintId );

        records.clear();
        records.addAll( getSchemaStore().allocateFrom( indexRule ) );
    }

    private static Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, RelationshipLoadingPosition>
            getMoreRelationships( long nodeId, RelationshipLoadingPosition originalPosition, int grabSize,
                    DirectionWrapper direction, int[] types, RelationshipStore relStore )
    {
        // initialCapacity=grabSize saves the lists the trouble of resizing
        List<RelationshipRecord> out = new ArrayList<>();
        List<RelationshipRecord> in = new ArrayList<>();
        List<RelationshipRecord> loop = null;
        Map<DirectionWrapper, Iterable<RelationshipRecord>> result = new EnumMap<>( DirectionWrapper.class );
        result.put( DirectionWrapper.OUTGOING, out );
        result.put( DirectionWrapper.INCOMING, in );

        // Clone the position so that changes to it aren't visible as we go. This is necessary since
        // there are checks whether or not there are more relationships to load happening outside synchronization
        // blocks in NodeImpl.
        RelationshipLoadingPosition loadPosition = originalPosition.clone();

        long position = loadPosition.position( direction, types );
        for ( int i = 0; i < grabSize && position != Record.NO_NEXT_RELATIONSHIP.intValue(); i++ )
        {
            RelationshipRecord relRecord = relStore.getChainRecord( position );
            if ( relRecord == null )
            {
                // return what we got so far
                return Pair.of( result, loadPosition );
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

            long next = 0;
            if ( firstNode == nodeId )
            {
                next = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                next = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "While loading relationships for Node[" + nodeId +
                        "] a Relationship[" + relRecord.getId() + "] was encountered that had startNode: " + firstNode +
                        " and endNode: " + secondNode + ", i.e. which had neither start nor end node as the node we're " +
                        "loading relationships for" );
            }
            position = loadPosition.nextPosition( next, direction, types );
        }
        return Pair.of( result, loadPosition );
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

    static void loadProperties( PropertyStore propertyStore, long nextProp, PropertyReceiver receiver,
            Map<Long,PropertyRecord> propertyLookup )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp, propertyLookup );
        if ( chain != null )
        {
            loadPropertyChain( chain, propertyStore, receiver );
        }
    }

    static Map<Integer, RelationshipGroupRecord> loadRelationshipGroups( long firstGroup, RelationshipGroupStore store )
    {
        long groupId = firstGroup;
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        Map<Integer, RelationshipGroupRecord> result = new HashMap<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord record = store.getRecord( groupId );
            record.setPrev( previousGroupId );
            result.put( record.getType(), record );
            previousGroupId = groupId;
            groupId = record.getNext();
        }
        return result;
    }

    private Map<Integer, RelationshipGroupRecord> loadRelationshipGroups( NodeRecord node )
    {
        assert node.isDense();
        return loadRelationshipGroups( node.getNextRel(), getRelationshipGroupStore() );
    }

    public int getRelationshipCount( long id, int type, DirectionWrapper direction )
    {
        NodeRecord node = getNodeStore().getRecord( id );
        long nextRel = node.getNextRel();
        if ( nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return 0;
        }
        if ( !node.isDense() )
        {
            assert type == -1 : node + " isn't dense and so getRelationshipCount shouldn't have been called " +
                    "with a specific type";
            assert direction == DirectionWrapper.BOTH : node + " isn't dense and so getRelationshipCount " +
                    "shouldn't have been called with a specific direction";
            return getRelationshipCount( node, nextRel );
        }

        // From here on it's only dense node specific

        Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( node );
        if ( type == -1 && direction == DirectionWrapper.BOTH )
        {   // Count for all types/directions
            int count = 0;
            for ( RelationshipGroupRecord group : groups.values() )
            {
                count += getRelationshipCount( node, group.getFirstOut() );
                count += getRelationshipCount( node, group.getFirstIn() );
                count += getRelationshipCount( node, group.getFirstLoop() );
            }
            return count;
        }
        else if ( type == -1 )
        {   // Count for all types with a given direction
            int count = 0;
            for ( RelationshipGroupRecord group : groups.values() )
            {
                count += getRelationshipCount( node, group, direction );
            }
            return count;
        }
        else if ( direction == DirectionWrapper.BOTH )
        {   // Count for a type
            RelationshipGroupRecord group = groups.get( type );
            if ( group == null )
            {
                return 0;
            }
            int count = 0;
            count += getRelationshipCount( node, group.getFirstOut() );
            count += getRelationshipCount( node, group.getFirstIn() );
            count += getRelationshipCount( node, group.getFirstLoop() );
            return count;
        }
        else
        {   // Count for one type and direction
            RelationshipGroupRecord group = groups.get( type );
            if ( group == null )
            {
                return 0;
            }
            return getRelationshipCount( node, group, direction );
        }
    }

    private int getRelationshipCount( NodeRecord node, RelationshipGroupRecord group, DirectionWrapper direction )
    {
        if ( direction == DirectionWrapper.BOTH )
        {
            return getRelationshipCount( node, DirectionWrapper.OUTGOING.getNextRel( group ) ) +
                    getRelationshipCount( node, DirectionWrapper.INCOMING.getNextRel( group ) ) +
                    getRelationshipCount( node, DirectionWrapper.BOTH.getNextRel( group ) );
        }

        return getRelationshipCount( node, direction.getNextRel( group ) ) +
                getRelationshipCount( node, DirectionWrapper.BOTH.getNextRel( group ) );
    }

    private int getRelationshipCount( NodeRecord node, long relId )
    {   // Relationship count is in a PREV field of the first record in a chain
        if ( relId == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return 0;
        }
        RelationshipRecord rel = getRelationshipStore().getRecord( relId );
        return (int) (node.getId() == rel.getFirstNode() ? rel.getFirstPrevRel() : rel.getSecondPrevRel());
    }

    public Integer[] getRelationshipTypes( long id )
    {
        Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( getNodeStore().getRecord( id ) );
        Integer[] types = new Integer[groups.size()];
        int i = 0;
        for ( Integer type : groups.keySet() )
        {
            types[i++] = type;
        }
        return types;
    }

    public RelationshipLoadingPosition getRelationshipChainPosition( long id )
    {
        RecordChange<Long, NodeRecord, Void> nodeChange = context.getNodeRecords().getIfLoaded( id );
        if ( nodeChange != null && nodeChange.isCreated() )
        {
            return RelationshipLoadingPosition.EMPTY;
        }

        NodeRecord node = getNodeStore().getRecord( id );
        if ( node.isDense() )
        {
            long firstGroup = node.getNextRel();
            if ( firstGroup == Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                return RelationshipLoadingPosition.EMPTY;
            }
            Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( firstGroup,
                    neoStore.getRelationshipGroupStore() );
            return new DenseNodeChainPosition( groups );
        }

        long firstRel = node.getNextRel();
        return firstRel == Record.NO_NEXT_RELATIONSHIP.intValue() ?
                RelationshipLoadingPosition.EMPTY : new SingleChainPosition( firstRel );
    }

    public interface PropertyReceiver
    {
        void receive( DefinedProperty property, long propertyRecordId );
    }
}
