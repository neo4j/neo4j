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

import static java.util.Arrays.binarySearch;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.CREATE;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.DELETE;
import static org.neo4j.kernel.impl.nioneo.xa.Command.Mode.UPDATE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.persistence.NeoStoreTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * Transaction containing {@link Command commands} reflecting the operations
 * performed in the transaction.
 */
public class WriteTransaction extends XaTransaction implements NeoStoreTransaction
{
    private final Map<Long,Pair<NodeRecord,NodeRecord>> nodeRecords = new HashMap<Long, Pair<NodeRecord,NodeRecord>>();
    private final Map<Long,Pair<PropertyRecord/*before*/,PropertyRecord/*after*/>> propertyRecords =
            new HashMap<Long, Pair<PropertyRecord,PropertyRecord>>();
    private final Map<Long,RelationshipRecord> relRecords = new HashMap<Long,RelationshipRecord>();
    private final Map<Long,Pair<Collection<DynamicRecord>, SchemaRule>> schemaRuleRecords =
            new HashMap<Long, Pair<Collection<DynamicRecord>,SchemaRule>>();
    private Map<Integer,RelationshipTypeRecord> relTypeRecords;
    private Map<Integer,PropertyIndexRecord> propIndexRecords;
    private NeoStoreRecord neoStoreRecord;

    private final Map<Long,Command.NodeCommand> nodeCommands = new TreeMap<Long,Command.NodeCommand>();
    private final ArrayList<Command.PropertyCommand> propCommands = new ArrayList<Command.PropertyCommand>();
    private final ArrayList<Command.RelationshipCommand> relCommands = new ArrayList<Command.RelationshipCommand>();
    private final ArrayList<Command.SchemaRuleCommand> schemaRuleCommands = new ArrayList<Command.SchemaRuleCommand>();
    private ArrayList<Command.RelationshipTypeCommand> relTypeCommands;
    private ArrayList<Command.PropertyIndexCommand> propIndexCommands;
    private Command.NeoStoreCommand neoStoreCommand;

    private final NeoStore neoStore;
    private boolean committed = false;
    private boolean prepared = false;

    private final TransactionState state;
    private XaConnection xaConnection;
    private final CacheAccessBackDoor cacheAccess;
    private final IndexingService indexes;

    WriteTransaction( int identifier, XaLogicalLog log, TransactionState state, NeoStore neoStore,
            CacheAccessBackDoor cacheAccess, IndexingService indexingService )
    {
        super( identifier, log, state );
        this.neoStore = neoStore;
        this.state = state;
        this.cacheAccess = cacheAccess;
        this.indexes = indexingService;
    }

    @Override
    public boolean isReadOnly()
    {
        if ( isRecovered() )
        {
            return nodeCommands.size() == 0 && propCommands.size() == 0 &&
                relCommands.size() == 0 && schemaRuleCommands.size() == 0 && relTypeCommands == null &&
                propIndexCommands == null;
        }
        return nodeRecords.size() == 0 && relRecords.size() == 0 && schemaRuleRecords.size() == 0 &&
            propertyRecords.size() == 0 && relTypeRecords == null &&
            propIndexRecords == null;
    }

    @Override
    public void doAddCommand( XaCommand command )
    {
        // override
    }

    @Override
    protected void doPrepare() throws XAException
    {
        int noOfCommands = nodeRecords.size() +
                           relRecords.size() +
                           propertyRecords.size() +
                           schemaRuleRecords.size() +
                           (propIndexRecords != null ? propIndexRecords.size() : 0) +
                           (relTypeRecords != null ? relTypeRecords.size() : 0);
        List<Command> commands = new ArrayList<Command>( noOfCommands );
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
        /*
         * Generate records first, then write all together to logical log via
         * addCommand method but before give the option to intercept.
         */
        prepared = true;
        if ( relTypeRecords != null )
        {
            relTypeCommands = new ArrayList<Command.RelationshipTypeCommand>();
            for ( RelationshipTypeRecord record : relTypeRecords.values() )
            {
                Command.RelationshipTypeCommand command =
                    new Command.RelationshipTypeCommand(
                        neoStore.getRelationshipTypeStore(), record );
                relTypeCommands.add( command );
                commands.add( command );
            }
        }
        for ( Pair<NodeRecord,NodeRecord> records : nodeRecords.values() )
        {
            matchHeavy( records.first(), records.other() );
            NodeRecord record = records.other();
            if ( !record.inUse() && record.getNextRel() !=
                Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                throw Exceptions.withCause( new XAException( XAException.XA_RBINTEGRITY ),
                        new ConstraintViolationException("Node record " + record + " still has relationships"));
            }
            Command.NodeCommand command = new Command.NodeCommand(
                neoStore.getNodeStore(), records.first(), record );
            nodeCommands.put( record.getId(), command );
            commands.add( command );
        }
        for ( RelationshipRecord record : relRecords.values() )
        {
            Command.RelationshipCommand command =
                new Command.RelationshipCommand(
                    neoStore.getRelationshipStore(), record );
            relCommands.add( command );
            commands.add( command );
        }
        if ( neoStoreRecord != null )
        {
            neoStoreCommand = new Command.NeoStoreCommand( neoStore, neoStoreRecord );
            addCommand( neoStoreCommand );
        }
        if ( propIndexRecords != null )
        {
            propIndexCommands = new ArrayList<Command.PropertyIndexCommand>();
            for ( PropertyIndexRecord record : propIndexRecords.values() )
            {
                Command.PropertyIndexCommand command =
                    new Command.PropertyIndexCommand(
                        neoStore.getPropertyStore().getIndexStore(), record );
                propIndexCommands.add( command );
                commands.add( command );
            }
        }
        for ( Pair<PropertyRecord, PropertyRecord> change : propertyRecords.values() )
        {
            matchHeavy( change.first(), change.other() );
            Command.PropertyCommand command = new Command.PropertyCommand(
                    neoStore.getPropertyStore(), change.first(), change.other() );
            propCommands.add( command );
            commands.add( command );
        }
        for ( Pair<Collection<DynamicRecord>, SchemaRule> records : schemaRuleRecords.values() )
        {
            Command.SchemaRuleCommand command = new Command.SchemaRuleCommand( neoStore, neoStore.getSchemaStore(),
                    indexes, records.first(), records.other() );
            schemaRuleCommands.add( command );
            commands.add( command );
        }
        assert commands.size() == noOfCommands : "Expected " + noOfCommands
                                                 + " final commands, got "
                                                 + commands.size() + " instead";
        intercept( commands );

        for ( Command command : commands )
        {
            addCommand(command);
        }
    }

    private void matchHeavy( NodeRecord before, NodeRecord after )
    {
        getNodeStore().makeHeavy( before );
    }

    private void matchHeavy( PropertyRecord before, PropertyRecord after )
    {
        if ( !after.isChanged() )
            return;
        
        for ( PropertyBlock afterBlock : after.getPropertyBlocks() )
        {
            PropertyBlock beforeBlock = before.getPropertyBlock( afterBlock.getKeyIndexId() );
            if ( beforeBlock != null )
                getPropertyStore().makeHeavy( beforeBlock );
        }
        for ( PropertyBlock beforeBlock : before.getPropertyBlocks() )
        {
            if ( after.getPropertyBlock( beforeBlock.getKeyIndexId() ) == null )
                getPropertyStore().makeHeavy( beforeBlock );
        }
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
        else if ( xaCommand instanceof Command.PropertyIndexCommand )
        {
            if ( propIndexCommands == null ) propIndexCommands = new ArrayList<Command.PropertyIndexCommand>();
            propIndexCommands.add( (Command.PropertyIndexCommand) xaCommand );
        }
        else if ( xaCommand instanceof Command.RelationshipTypeCommand )
        {
            if ( relTypeCommands == null ) relTypeCommands = new ArrayList<Command.RelationshipTypeCommand>();
            relTypeCommands.add( (Command.RelationshipTypeCommand) xaCommand );
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
            if ( relTypeRecords != null ) for ( RelationshipTypeRecord record : relTypeRecords.values() )
            {
                if ( record.isCreated() )
                {
                    if ( freeIds ) getRelationshipTypeStore().freeId( record.getId() );
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
            for ( Pair<NodeRecord,NodeRecord> records : nodeRecords.values() )
            {
                NodeRecord record = records.other();
                if ( freeIds && record.isCreated() )
                {
                    getNodeStore().freeId( record.getId() );
                }
                removeNodeFromCache( record.getId() );
            }
            for ( RelationshipRecord record : relRecords.values() )
            {
                if ( freeIds && record.isCreated() )
                {
                    getRelationshipStore().freeId( record.getId() );
                }
                removeRelationshipFromCache( record.getId() );
                patchDeletedRelationshipNodes( record.getId(), record.getFirstNode(), record.getFirstNextRel(),
                        record.getSecondNode(), record.getSecondNextRel() );
            }
            if ( neoStoreRecord != null )
            {
                removeGraphPropertiesFromCache();
            }
            if ( propIndexRecords != null ) for ( PropertyIndexRecord record : propIndexRecords.values() )
            {
                if ( record.isCreated() )
                {
                    if ( freeIds ) getPropertyStore().getIndexStore().freeId( record.getId() );
                    for ( DynamicRecord dynamicRecord : record.getNameRecords() )
                    {
                        if ( dynamicRecord.isCreated() )
                        {
                            getPropertyStore().getIndexStore().freeId(
                                (int) dynamicRecord.getId() );
                        }
                    }
                }
            }
            for ( Pair<PropertyRecord,PropertyRecord> change : propertyRecords.values() )
            {
                PropertyRecord record = change.other();
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
                    if ( freeIds ) getPropertyStore().freeId( record.getId() );
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
            for ( Pair<Collection<DynamicRecord>,SchemaRule> records : schemaRuleRecords.values() )
            {
                long id = -1;
                for ( DynamicRecord record : records.first() )
                {
                    if ( id == -1 )
                        id = record.getId();
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
        NameData type = isRecovered() ?
                neoStore.getRelationshipTypeStore().getName( id, true ) :
                neoStore.getRelationshipTypeStore().getName( id );
        cacheAccess.addRelationshipType( type );
    }

    private void addPropertyIndexCommand( int id )
    {
        NameData index = isRecovered() ?
                neoStore.getPropertyStore().getIndexStore().getName( id, true ) :
                neoStore.getPropertyStore().getIndexStore().getName( id );
        cacheAccess.addPropertyIndex( index );
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
        if ( !isRecovered() && getCommitTxId() != neoStore.getLastCommittedTx() + 1 )
        {
            throw new RuntimeException( "Tx id: " + getCommitTxId() +
                    " not next transaction (" + neoStore.getLastCommittedTx() + ")" );
        }
        applyCommit( false );
    }

    private void applyCommit( boolean isRecovered )
    {
        try
        {
            committed = true;
            CommandSorter sorter = new CommandSorter();
            // reltypes
            if ( relTypeCommands != null )
            {
                java.util.Collections.sort( relTypeCommands, sorter );
                for ( Command.RelationshipTypeCommand command : relTypeCommands )
                {
                    command.execute();
                    if ( isRecovered )
                    {
                        addRelationshipType( (int) command.getKey() );
                    }
                }
            }
            // property keys
            if ( propIndexCommands != null )
            {
                java.util.Collections.sort( propIndexCommands, sorter );
                for ( Command.PropertyIndexCommand command : propIndexCommands )
                {
                    command.execute();
                    if ( isRecovered )
                    {
                        addPropertyIndexCommand( (int) command.getKey() );
                    }
                }
            }
            // schema rules
            for ( SchemaRuleCommand command : schemaRuleCommands )
            {
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

            // primitives
//            java.util.Collections.sort( nodeCommands, sorter ); // it's a TreeMap so already sorted.
            java.util.Collections.sort( relCommands, sorter );
            java.util.Collections.sort( propCommands, sorter );
            executeCreated( isRecovered, propCommands, relCommands, nodeCommands.values() );
            executeModified( isRecovered, propCommands, relCommands, nodeCommands.values() );
            executeDeleted( propCommands, relCommands, nodeCommands.values() );

            // property change set for index updates
            Iterable<NodePropertyUpdate> updates = convertIntoLogicalPropertyUpdates();
            indexes.update( updates );
            
            if ( neoStoreCommand != null )
            {
                neoStoreCommand.execute();
                if ( isRecovered )
                    removeGraphPropertiesFromCache();
            }
            if ( !isRecovered )
            {
                updateFirstRelationships();
                state.commitCows(); // updates the cached primitives
            }
            neoStore.setLastCommittedTx( getCommitTxId() );
            if ( isRecovered )
                neoStore.updateIdGenerators();
        }
        finally
        {
            clear();
        }
    }

    private Iterable<NodePropertyUpdate> convertIntoLogicalPropertyUpdates( )
    {
        Collection<NodePropertyUpdate> updates = new ArrayList<NodePropertyUpdate>();
        
        gatherUpdatesFromPropertyCommands( updates );
        gatherUpdatesFromNodeCommands( updates );

        return updates;
    }

    private void gatherUpdatesFromPropertyCommands( Collection<NodePropertyUpdate> updates )
    {
        final PropertyStore propertyStore = getPropertyStore();
        final NodeStore nodeStore = getNodeStore();
        for ( PropertyCommand propertyCommand : propCommands )
        {
            PropertyRecord after = propertyCommand.getAfter();
            if ( after.hostIsNode() )
            {
                long[] nodeLabelsBefore, nodeLabelsAfter;
                NodeCommand nodeChanges = nodeCommands.get( after.getNodeId() );
                if ( nodeChanges != null )
                {
                    nodeLabelsBefore = nodeStore.getLabelsForNode( nodeChanges.getBefore() );
                    nodeLabelsAfter = nodeStore.getLabelsForNode( nodeChanges.getAfter() );
                }
                else
                    nodeLabelsBefore = nodeLabelsAfter = nodeStore.getLabelsForNode( nodeStore.getRecord( after.getNodeId() ) );
                
                for ( NodePropertyUpdate update :
                    propertyStore.toLogicalUpdates( propertyCommand.getBefore(), nodeLabelsBefore, after, nodeLabelsAfter ) )
                {
                    updates.add( update );
                }
            }
        }
    }

    private void gatherUpdatesFromNodeCommands( Collection<NodePropertyUpdate> updates )
    {
        final NodeStore nodeStore = getNodeStore();
        for ( NodeCommand nodeCommand : nodeCommands.values() )
        {
            long nodeId = nodeCommand.getKey();
            long[] labelsBefore = nodeStore.getLabelsForNode( nodeCommand.getBefore() );
            long[] labelsAfter = nodeStore.getLabelsForNode( nodeCommand.getAfter() );
            // They are sorted in the store
            
            for ( long labelAfter : labelsAfter )
            {
                if ( binarySearch( labelsBefore, labelAfter ) < 0 )
                {
                    // This label has been added. Go through all node properties and create updates for this label
                    ArrayMap<Integer, PropertyData> properties = nodeLoadProperties( nodeId, false );
                    for ( PropertyData property : properties.values() )
                    {
                        updates.add( NodePropertyUpdate.add( nodeId, property.getIndex(), property.getValue(),
                                new long[] {labelAfter} ) );
                    }
                }
            }
            for ( long labelBefore : labelsBefore )
            {
                if ( binarySearch( labelsAfter, labelBefore ) < 0 )
                {
                    // This label has been removed. Go through all node properties and create updates for this label
                    ArrayMap<Integer, PropertyData> properties = nodeLoadProperties( nodeId, false );
                    for ( PropertyData property : properties.values() )
                        updates.add( NodePropertyUpdate.remove( nodeId, property.getIndex(), property.getValue(),
                                new long[] {labelBefore} ) );
                }
            }
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
        for ( Pair<NodeRecord,NodeRecord> records : nodeRecords.values() )
        {
            NodeRecord record = records.other();
            state.setFirstIds( record.getId(), record.getNextRel(), record.getNextProp() );
        }
    }

    private void executeCreated( boolean removeFromCache, Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands ) for ( Command command : c )
        {
            if ( command.getMode() == CREATE )
            {
                command.execute();
                if ( removeFromCache )
                {
                    command.removeFromCache( cacheAccess );
                }
            }
        }
    }

    private void executeModified( boolean removeFromCache, Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands ) for ( Command command : c )
        {
            if ( command.getMode() == UPDATE)
            {
                command.execute();
                if ( removeFromCache )
                {
                    command.removeFromCache( cacheAccess );
                }
            }
        }
    }

    private void executeDeleted( Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands ) for ( Command command : c )
        {
            if ( command.getMode() == DELETE )
            {
                /*
                 * We always update the disk image and then always invalidate the cache. In the case of relationships
                 * this is expected to also patch the relChainPosition in the start and end NodeImpls (if they actually
                 * are in cache).
                 */
                command.execute();
                command.removeFromCache( cacheAccess );
            }
        }
    }

    private void clear()
    {
        nodeRecords.clear();
        propertyRecords.clear();
        relRecords.clear();
        schemaRuleRecords.clear();
        relTypeRecords = null;
        propIndexRecords = null;
        neoStoreRecord = null;

        nodeCommands.clear();
        propCommands.clear();
        propIndexCommands = null;
        relCommands.clear();
        schemaRuleCommands.clear();
        relTypeCommands = null;
        neoStoreCommand = null;
    }

    private RelationshipTypeStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
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
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        if ( nodeRecord != null ) return nodeRecord;
        return getNodeStore().loadLightNode( nodeId );
    }

    @Override
    public RelationshipRecord relLoadLight( long id )
    {
        RelationshipRecord relRecord = getRelationshipRecord( id );
        if ( relRecord != null )
        {
            // if deleted in this tx still return it
//            if ( !relRecord.inUse() )
//            {
//                return null;
//            }
            return relRecord;
        }
        relRecord = getRelationshipStore().getLightRel( id );
        if ( relRecord != null )
        {
            return relRecord;
        }
        return null;
    }

    @Override
    public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
    {
        NodeRecord nodeRecord = getOrLoadNodeRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete Node[" + nodeId +
            "] since it has already been deleted." );
        }
        nodeRecord.setInUse( false );
        ArrayMap<Integer, PropertyData> propertyMap =
                getAndDeletePropertyChain( nodeRecord, PropertyEntityType.NODE, nodeId );
        return propertyMap;
    }

    @Override
    public ArrayMap<Integer,PropertyData> relDelete( long id )
    {
        RelationshipRecord record = getRelationshipRecord( id );
        if ( record == null )
        {
            record = getRelationshipStore().getRecord( id );
            addRelationshipRecord( record );
        }
        if ( !record.inUse() )
        {
            throw new IllegalStateException( "Unable to delete relationship[" +
                id + "] since it is already deleted." );
        }
        ArrayMap<Integer, PropertyData> propertyMap =
                getAndDeletePropertyChain( record, PropertyEntityType.RELATIONSHIP, id );
        disconnectRelationship( record );
        updateNodes( record );
        record.setInUse( false );
        return propertyMap;
    }

    private ArrayMap<Integer, PropertyData> getAndDeletePropertyChain(
            PrimitiveRecord primitive, PropertyEntityType entityType, long entityId )
    {
        ArrayMap<Integer, PropertyData> result = new ArrayMap<Integer, PropertyData>(
                (byte)9, false, true );
        long nextProp = primitive.getNextProp();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = getPropertyRecord( nextProp, false, true, entityType, entityId );
            if ( !propRecord.isCreated() && propRecord.isChanged() )
            {
                // Being here means a new value could be on disk. Re-read and replace
                propRecord = getPropertyStore().getRecord( propRecord.getId() );
                addPropertyRecord( propRecord.clone(), propRecord );
            }
            for ( PropertyBlock block : propRecord.getPropertyBlocks() )
            {
                if ( block.isLight() )
                {
                    getPropertyStore().makeHeavy( block );
                }
                if ( !block.isCreated() && !propRecord.isChanged() )
                {
                    result.put( block.getKeyIndexId(),
                            block.newPropertyData( propRecord,
                                    propertyGetValueOrNull( block ) ) );
                }
                // TODO: update count on property index record
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
            Relationship lockableRel = new LockableRelationship(
                rel.getFirstPrevRel() );
            getWriteLock( lockableRel );
            RelationshipRecord prevRel = getRelationshipRecord(
                rel.getFirstPrevRel() );
            if ( prevRel == null )
            {
                prevRel = getRelationshipStore().getRecord(
                    rel.getFirstPrevRel() );
                addRelationshipRecord( prevRel );
            }
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
            Relationship lockableRel = new LockableRelationship(
                rel.getFirstNextRel() );
            getWriteLock( lockableRel );
            RelationshipRecord nextRel = getRelationshipRecord(
                rel.getFirstNextRel() );
            if ( nextRel == null )
            {
                nextRel = getRelationshipStore().getRecord(
                    rel.getFirstNextRel() );
                addRelationshipRecord( nextRel );
            }
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
                throw new InvalidRecordException( nextRel + " don't match "
                    + rel );
            }
        }
        // update second node prev
        if ( rel.getSecondPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship(
                rel.getSecondPrevRel() );
            getWriteLock( lockableRel );
            RelationshipRecord prevRel = getRelationshipRecord(
                rel.getSecondPrevRel() );
            if ( prevRel == null )
            {
                prevRel = getRelationshipStore().getRecord(
                    rel.getSecondPrevRel() );
                addRelationshipRecord( prevRel );
            }
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
                throw new InvalidRecordException( prevRel + " don't match " +
                    rel );
            }
        }
        // update second node next
        if ( rel.getSecondNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            Relationship lockableRel = new LockableRelationship(
                rel.getSecondNextRel() );
            getWriteLock( lockableRel );
            RelationshipRecord nextRel = getRelationshipRecord(
                rel.getSecondNextRel() );
            if ( nextRel == null )
            {
                nextRel = getRelationshipStore().getRecord(
                    rel.getSecondNextRel() );
                addRelationshipRecord( nextRel );
            }
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
                throw new InvalidRecordException( nextRel + " don't match " +
                    rel );
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
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        if ( nodeRecord != null && nodeRecord.isCreated() )
        {
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }
        return getNodeStore().getRecord( nodeId ).getNextRel();
    }

    @Override
    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships( long nodeId,
        long position )
    {
        return ReadTransaction.getMoreRelationships( nodeId, position, getRelGrabSize(), getRelationshipStore() );
    }

    private void updateNodes( RelationshipRecord rel )
    {
        if ( rel.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord firstNode = getOrLoadNodeRecord( rel.getFirstNode() );
            firstNode.setNextRel( rel.getFirstNextRel() );
        }
        if ( rel.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            NodeRecord secondNode = getOrLoadNodeRecord( rel.getSecondNode() );
            secondNode.setNextRel( rel.getSecondNextRel() );
        }
    }

    @Override
    public void relRemoveProperty( long relId, PropertyData propertyData )
    {
        RelationshipRecord relRecord = getRelationshipRecord( relId );
        if ( relRecord == null )
        {
            relRecord = getRelationshipStore().getRecord( relId );
        }
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on relationship[" +
                relId + "] illegal since it has been deleted." );
        }
        assert assertPropertyChain( relRecord, PropertyEntityType.RELATIONSHIP );
        removeProperty( relRecord, propertyData, PropertyEntityType.RELATIONSHIP );
    }

    @Override
    public ArrayMap<Integer,PropertyData> relLoadProperties( long relId,
            boolean light )
    {
        RelationshipRecord relRecord = getRelationshipRecord( relId );
        if ( relRecord != null && relRecord.isCreated() )
            return ArrayMap.empty();
        if ( relRecord != null )
        {
            if ( !relRecord.inUse() && !light )
            {
                throw new IllegalStateException( "Relationship[" + relId +
                        "] has been deleted in this tx" );
            }
        }
        relRecord = getRelationshipStore().getRecord( relId );
        if ( !relRecord.inUse() )
        {
            throw new InvalidRecordException( "Relationship[" + relId +
                "] not in use" );
        }
        return ReadTransaction.loadProperties( getPropertyStore(), relRecord.getNextProp() );
    }

    @Override
    public ArrayMap<Integer,PropertyData> nodeLoadProperties( long nodeId, boolean light )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        if ( nodeRecord != null && nodeRecord.isCreated() )
        {
            return ArrayMap.empty();
        }
        if ( nodeRecord != null )
        {
            if ( !nodeRecord.inUse() && !light )
            {
                throw new IllegalStateException( "Node[" + nodeId +
                        "] has been deleted in this tx" );
            }
        }
        nodeRecord = getNodeStore().getRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new InvalidRecordException( "Node[" + nodeId +
                "] not in use" );
        }
        return ReadTransaction.loadProperties( getPropertyStore(), nodeRecord.getNextProp() );
    }

    public Object propertyGetValueOrNull( PropertyBlock block )
    {
        return block.getType().getValue( block,
                block.isLight() ? null : getPropertyStore() );
    }

    @Override
    public Object loadPropertyValue( PropertyData propertyData )
    {
        Pair<PropertyRecord,PropertyRecord> propertyChange = propertyRecords.get( propertyData.getId() );
        if ( propertyChange == null )
        {
            PropertyRecord before = getPropertyStore().getRecord( propertyData.getId() );
            propertyChange = Pair.of( before, before.clone() );
        }
        PropertyRecord propertyRecord = propertyChange.other();
        PropertyBlock block = propertyRecord.getPropertyBlock( propertyData.getIndex() );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyData.getIndex()
                                             + "] is not present in property["
                                             + propertyData.getId() + "]" );
        }
        if ( block.isLight() )
        {
            getPropertyStore().makeHeavy( block );
        }
        return block.getType().getValue( block, getPropertyStore() );
    }

    @Override
    public void nodeRemoveProperty( long nodeId, PropertyData propertyData )
    {
        NodeRecord nodeRecord = getOrLoadNodeRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property remove on node[" +
                nodeId + "] illegal since it has been deleted." );
        }
        assert assertPropertyChain( nodeRecord, PropertyEntityType.NODE );

        removeProperty( nodeRecord, propertyData, PropertyEntityType.NODE );
        // propRecord.removeBlock( propertyData.getIndex() );
    }

    private void removeProperty( PrimitiveRecord hostRecord, PropertyData propertyData, PropertyEntityType adder )
    {
        long propertyId = propertyData.getId();
        PropertyRecord propRecord = getPropertyRecord( propertyId, false, true, adder, hostRecord.getId() );
        if ( !propRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete property[" +
                propertyId + "] since it is already deleted." );
        }

        PropertyBlock block = propRecord.removePropertyBlock( propertyData.getIndex() );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyData.getIndex()
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }

        if ( block.isLight() )
        {
            getPropertyStore().makeHeavy( block );
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
            propRecord.setChanged( hostRecord );
            assert assertPropertyChain( hostRecord, adder );
        }
        else
        {
            if ( unlinkPropertyRecord( propRecord, hostRecord, adder ) )
            {
                adder.add( this, hostRecord );
            }
        }
    }

    private boolean unlinkPropertyRecord( PropertyRecord propRecord,
            PrimitiveRecord primitive, PropertyEntityType entityType )
    {
        assert assertPropertyChain( primitive, entityType );
        assert propRecord.size() == 0;
        boolean primitiveChanged = false;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if ( primitive.getNextProp() == propRecord.getId() )
        {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            primitive.setNextProp( nextProp );
            primitiveChanged = true;
        }
        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord prevPropRecord = getPropertyRecord( prevProp, true, true, entityType, primitive.getId() );
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
                                            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            prevPropRecord.setChanged( primitive );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = getPropertyRecord( nextProp, true, true, entityType, primitive.getId() );
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
        assert assertPropertyChain( primitive, entityType );
        return primitiveChanged;
    }

    @Override
    public PropertyData relChangeProperty( long relId,
            PropertyData propertyData, Object value )
    {
        RelationshipRecord relRecord = getRelationshipRecord( relId );
        if ( relRecord == null )
        {
            relRecord = getRelationshipStore().getRecord( relId );
        }
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property change on relationship[" +
                relId + "] illegal since it has been deleted." );
        }
        return primitiveChangeProperty( relRecord, propertyData, value, PropertyEntityType.RELATIONSHIP );
    }

    @Override
    public PropertyData nodeChangeProperty( long nodeId,
            PropertyData propertyData, Object value )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        if ( nodeRecord == null )
        {
            nodeRecord = getNodeStore().getRecord( nodeId );
        }
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property change on node[" +
                nodeId + "] illegal since it has been deleted." );
        }
        return primitiveChangeProperty( nodeRecord, propertyData, value, PropertyEntityType.NODE );
    }

    private PropertyData primitiveChangeProperty( PrimitiveRecord primitive,
            PropertyData propertyData, Object value, PropertyEntityType adder )
    {
        assert assertPropertyChain( primitive, adder );
        long propertyId = propertyData.getId();
        PropertyRecord propertyRecord = getPropertyRecord( propertyId, true, true, adder, primitive.getId() );
        if ( !propertyRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to change property["
                                             + propertyId
                                             + "] since it has been deleted." );
        }
        PropertyBlock block = propertyRecord.getPropertyBlock( propertyData.getIndex() );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyData.getIndex()
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }
        if ( block.isLight() )
        {
            getPropertyStore().makeHeavy( block );
        }
        propertyRecord.setChanged( primitive );
        for ( DynamicRecord record : block.getValueRecords() )
        {
            assert record.inUse();
            record.setInUse( false, block.getType().intValue() );
            propertyRecord.addDeletedRecord( record );
        }
        getPropertyStore().encodeValue( block, propertyData.getIndex(), value );
        if ( propertyRecord.size() > PropertyType.getPayloadSize() )
        {
            propertyRecord.removePropertyBlock( propertyData.getIndex() );
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
            propertyRecord = addPropertyBlockToPrimitive( block, primitive, adder );
        }
        assert assertPropertyChain( primitive, adder );
        return block.newPropertyData( propertyRecord, value );
    }

    @Override
    public PropertyData relAddProperty( long relId,
            PropertyIndex index, Object value )
    {
        RelationshipRecord relRecord = getRelationshipRecord( relId );
        if ( relRecord == null )
        {
            relRecord = getRelationshipStore().getRecord( relId );
            addRelationshipRecord( relRecord );
        }
        if ( !relRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on relationship[" +
                relId + "] illegal since it has been deleted." );
        }
        assert assertPropertyChain( relRecord, PropertyEntityType.RELATIONSHIP );
        PropertyBlock block = new PropertyBlock();
        block.setCreated();
        getPropertyStore().encodeValue( block, index.getKeyId(), value );
        PropertyRecord host = addPropertyBlockToPrimitive( block, relRecord, PropertyEntityType.RELATIONSHIP );
        assert assertPropertyChain( relRecord, PropertyEntityType.RELATIONSHIP );
        return block.newPropertyData( host, value );
    }

    @Override
    public PropertyData nodeAddProperty( long nodeId, PropertyIndex index,
        Object value )
    {
        NodeRecord nodeRecord = getOrLoadNodeRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Property add on node[" +
                nodeId + "] illegal since it has been deleted." );
        }

        assert assertPropertyChain( nodeRecord, PropertyEntityType.NODE );
        PropertyBlock block = new PropertyBlock();
        block.setCreated();
        /*
         * Encoding has to be set here before anything is changed,
         * since an exception could be thrown in encodeValue now and tx not marked
         * rollback only.
         */
        getPropertyStore().encodeValue( block, index.getKeyId(), value );
        PropertyRecord host = addPropertyBlockToPrimitive( block, nodeRecord, PropertyEntityType.NODE );
        assert assertPropertyChain( nodeRecord, PropertyEntityType.NODE );
        return block.newPropertyData( host, value );
    }

    private NodeRecord getOrLoadNodeRecord( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        if ( nodeRecord == null )
        {
            nodeRecord = getNodeStore().getRecord( nodeId );
            addNodeRecord( nodeRecord.clone(), nodeRecord );
        }
        return nodeRecord;
    }

    private PropertyRecord addPropertyBlockToPrimitive( PropertyBlock block,
            PrimitiveRecord primitive, PropertyEntityType adder )
    {
        assert assertPropertyChain( primitive, adder );
        int newBlockSizeInBytes = block.getSize();
        /*
         * Here we could either iterate over the whole chain or just go for the first record
         * which is the most likely to be the less full one. Currently we opt for the second
         * to perform better.
         */
        PropertyRecord host = null, hostBefore = null;
        long firstProp = primitive.getNextProp();
        if ( firstProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            // We do not store in map - might not have enough space
            PropertyRecord propRecord = getPropertyRecord( firstProp, false, false, adder, primitive.getId() );
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            assert propRecord.inUse() : propRecord;
            int propSize = propRecord.size();
            assert propSize > 0 : propRecord;
            if ( propSize + newBlockSizeInBytes <= PropertyType.getPayloadSize() )
            {
                host = propRecord;
                hostBefore = propRecord.clone();
                addPropertyRecord( hostBefore, host );
                host.addPropertyBlock( block );
                host.setChanged( primitive );
            }
        }
        if ( host == null )
        {
            // First record in chain didn't fit, make new one
            host = new PropertyRecord( getPropertyStore().nextId(), primitive );
            hostBefore = host.clone();
            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord prevProp = getPropertyRecord(
                        primitive.getNextProp(), true, true, adder, primitive.getId() );
                adder.add( this, primitive );
                assert prevProp.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue();
                prevProp.setPrevProp( host.getId() );
                host.setNextProp( prevProp.getId() );
                prevProp.setChanged( primitive );
            }
            primitive.setNextProp( host.getId() );
            host.addPropertyBlock( block );
            host.setInUse( true );
        }
        // Ok, here host does for the job. Use it
        addPropertyRecord( hostBefore, host );
        assert assertPropertyChain( primitive, adder );
        return host;
    }

    @Override
    public void relationshipCreate( long id, int type, long firstNodeId, long secondNodeId )
    {
        NodeRecord firstNode = getOrLoadNodeRecord( firstNodeId );
        if ( !firstNode.inUse() )
        {
            throw new IllegalStateException( "First node[" + firstNodeId +
                "] is deleted and cannot be used to create a relationship" );
        }
        NodeRecord secondNode = getOrLoadNodeRecord( secondNodeId );
        if ( !secondNode.inUse() )
        {
            throw new IllegalStateException( "Second node[" + secondNodeId +
                "] is deleted and cannot be used to create a relationship" );
        }
        RelationshipRecord record = new RelationshipRecord( id, firstNodeId,
            secondNodeId, type );
        record.setInUse( true );
        record.setCreated();
        addRelationshipRecord( record );
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
            RelationshipRecord nextRel = getRelationshipRecord( node.getNextRel() );
            if ( nextRel == null )
            {
                nextRel = getRelationshipStore().getRecord( node.getNextRel() );
                addRelationshipRecord( nextRel );
            }
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
        NodeRecord nodeRecord = new NodeRecord( nodeId, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
        NodeRecord before = nodeRecord.clone();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        addNodeRecord( before, nodeRecord );
    }

    @Override
    public String loadIndex( int id )
    {
        PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
        PropertyIndexRecord index = getPropertyIndexRecord( id );
        if ( index == null )
        {
            index = indexStore.getRecord( id );
        }
        if ( index.isLight() )
        {
            indexStore.makeHeavy( index );
        }
        return indexStore.getStringFor( index );
    }

    @Override
    public NameData[] loadPropertyIndexes()
    {
        PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
        return indexStore.getNames( Integer.MAX_VALUE );
    }

    @Override
    public void createPropertyIndex( String key, int id )
    {
        PropertyIndexRecord record = new PropertyIndexRecord( id );
        record.setInUse( true );
        record.setCreated();
        PropertyIndexStore propIndexStore = getPropertyStore().getIndexStore();
//        int nameId = propIndexStore.nextNameId();
        Collection<DynamicRecord> nameRecords =
            propIndexStore.allocateNameRecords( encodeString( key ) );
        record.setNameId( (int) first( nameRecords ).getId() );
        for ( DynamicRecord keyRecord : nameRecords )
        {
            record.addNameRecord( keyRecord );
        }
        addPropertyIndexRecord( record );
    }

    @Override
    public void createRelationshipType( int id, String name )
    {
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> typeNameRecords =
            getRelationshipTypeStore().allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) first( typeNameRecords ).getId() );
        for ( DynamicRecord typeRecord : typeNameRecords )
        {
            record.addNameRecord( typeRecord );
        }
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
            if ( o instanceof CommandSorter )
            {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return 3217;
        }
    }

    void addNodeRecord( NodeRecord before, NodeRecord record )
    {
        if ( nodeRecords.containsKey( record.getId() ) )
            return;
        
        nodeRecords.put( record.getId(), Pair.of( before, record ) );
    }

    NodeRecord getNodeRecord( long nodeId )
    {
        Pair<NodeRecord, NodeRecord> records = nodeRecords.get( nodeId );
        return records != null ? records.other() : null;
    }

    void addRelationshipRecord( RelationshipRecord record )
    {
        relRecords.put( record.getId(), record );
    }

    RelationshipRecord getRelationshipRecord( long relId )
    {
        return relRecords.get( relId );
    }

    /**
     * The object that gets passed in here should be used as the "after" state.
     * The "before" state is {@link PropertyRecord#clone() derived} in this method.
     * @param record
     * @param host 
     */
    void addPropertyRecord( PropertyRecord before, PropertyRecord record )
    {
        Pair<PropertyRecord, PropertyRecord> previous = propertyRecords.get( before.getId() );
        propertyRecords.put( before.getId(), Pair.of( previous != null ? previous.first() : before, record ) );
    }

    PropertyRecord getPropertyRecord( long propertyId, boolean light,
            boolean store, PropertyEntityType entityType, long entityId )
    {
        Pair<PropertyRecord, PropertyRecord> change = propertyRecords.get( propertyId );
        PropertyRecord record = null;
        if ( change == null )
        {
            if ( light )
            {
                record = getPropertyStore().getLightRecord( propertyId );
            }
            else
            {
                record = getPropertyStore().getRecord( propertyId );
            }
            entityType.setEntityId( record, entityId );
            if ( store )
            {
                addPropertyRecord( record.clone(), record );
            }
        }
        else
            record = change.other();
        return record;
    }

    void addRelationshipTypeRecord( RelationshipTypeRecord record )
    {
        if ( relTypeRecords == null ) relTypeRecords = new HashMap<Integer, RelationshipTypeRecord>();
        relTypeRecords.put( record.getId(), record );
    }

    void addPropertyIndexRecord( PropertyIndexRecord record )
    {
        if ( propIndexRecords == null ) propIndexRecords = new HashMap<Integer, PropertyIndexRecord>();
        propIndexRecords.put( record.getId(), record );
    }

    PropertyIndexRecord getPropertyIndexRecord( int id )
    {
        return propIndexRecords != null ? propIndexRecords.get( id ) : null;
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
        public Iterable<Object> getPropertyValues()
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
            if ( !(o instanceof Relationship) )
            {
                return false;
            }
            return this.getId() == ((Relationship) o).getId();
        }

        @Override
        public int hashCode()
        {
            return (int) (( id >>> 32 ) ^ id );
        }

        @Override
        public String toString()
        {
            return "Lockable relationship #" + this.getId();
        }
    }

    @Override
    public RelIdArray getCreatedNodes()
    {
        RelIdArray createdNodes = new RelIdArray( 0 );
        for ( Pair<NodeRecord,NodeRecord> records : nodeRecords.values() )
        {
            NodeRecord record = records.other();
            if ( record.isCreated() )
            {
                // TODO Direction doesn't matter... misuse of RelIdArray?
                createdNodes.add( record.getId(), DirectionWrapper.OUTGOING );
            }
        }
        return createdNodes;
    }

    @Override
    public boolean isNodeCreated( long nodeId )
    {
        Pair<NodeRecord, NodeRecord> records = nodeRecords.get( nodeId );
        return records != null ? records.other().isCreated() : false;
    }

    @Override
    public boolean isRelationshipCreated( long relId )
    {
        RelationshipRecord record = relRecords.get( relId );
        if ( record != null )
        {
            return record.isCreated();
        }
        return false;
    }

    @Override
    public int getKeyIdForProperty( PropertyData property )
    {
        return ReadTransaction.getKeyIdForProperty( property,
                getPropertyStore() );
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

    @Override
    public NameData[] loadRelationshipTypes()
    {
        NameData relTypeData[] = neoStore.getRelationshipTypeStore().getNames( Integer.MAX_VALUE );
        NameData rawRelTypeData[] = new NameData[relTypeData.length];
        for ( int i = 0; i < relTypeData.length; i++ )
        {
            rawRelTypeData[i] = new NameData( relTypeData[i].getId(), relTypeData[i].getName() );
        }
        return rawRelTypeData;
    }

    private boolean assertPropertyChain( PrimitiveRecord primitive, PropertyEntityType entityType )
    {
        List<PropertyRecord> toCheck = new LinkedList<PropertyRecord>();
        long nextIdToFetch = primitive.getNextProp();
        while ( nextIdToFetch != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord toAdd = getPropertyRecord( nextIdToFetch, true, false, entityType, primitive.getId() );
            toCheck.add( toAdd );
            assert toAdd.inUse() : primitive + "->"
                                   + Arrays.toString( toCheck.toArray() );
            nextIdToFetch = toAdd.getNextProp();
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

    private NeoStoreRecord getOrLoadNeoStoreRecord()
    {
        if ( neoStoreRecord == null )
        {
            neoStoreRecord = neoStore.asRecord();
        }
        return neoStoreRecord;
    }

    @Override
    public PropertyData graphAddProperty( PropertyIndex index, Object value )
    {
        PropertyBlock block = new PropertyBlock();
        block.setCreated();
        /*
         * Encoding has to be set here before anything is changed,
         * since an exception could be thrown in encodeValue now and tx not marked
         * rollback only.
         */
        getPropertyStore().encodeValue( block, index.getKeyId(), value );
        NeoStoreRecord record = getOrLoadNeoStoreRecord();
        PropertyRecord host = addPropertyBlockToPrimitive( block, record, PropertyEntityType.GRAPH );
        assert assertPropertyChain( record, PropertyEntityType.GRAPH );
        return block.newPropertyData( host, value );
    }

    @Override
    public PropertyData graphChangeProperty( PropertyData propertyData, Object value )
    {
        return primitiveChangeProperty( getOrLoadNeoStoreRecord(), propertyData, value, PropertyEntityType.GRAPH );
    }

    @Override
    public void graphRemoveProperty( PropertyData propertyData )
    {
        removeProperty( getOrLoadNeoStoreRecord(), propertyData, PropertyEntityType.GRAPH );
    }

    @Override
    public ArrayMap<Integer, PropertyData> graphLoadProperties( boolean light )
    {
        return ReadTransaction.loadProperties( getPropertyStore(), getOrLoadNeoStoreRecord().getNextProp() );
    }
    
    @Override
    public void createSchemaRule( SchemaRule schemaRule )
    {
        Collection<DynamicRecord> records = getSchemaStore().allocateFrom( schemaRule );
        addSchemaRule( first( records ).getId(), Pair.of( records, schemaRule ) );
    }

    @Override
    public void dropSchemaRule( long ruleId )
    {
        Pair<Collection<DynamicRecord>, SchemaRule> pair = schemaRuleRecords.get( ruleId );
        if ( pair == null )
        {
            Collection<DynamicRecord> records = getSchemaStore().getRecords( ruleId );
            pair = Pair.of( records, deserializeSchemaRule( ruleId, records ) );
            addSchemaRule( ruleId, pair );
        }
        
        for ( DynamicRecord record : pair.first() )
            record.setInUse( false );
    }

    private SchemaRule deserializeSchemaRule( long ruleId, Collection<DynamicRecord> records )
    {
        return SchemaRule.Kind.deserialize( ruleId, AbstractDynamicStore.concatData( records, new byte[100] ) );
    }

    private void addSchemaRule( long id, Pair<Collection<DynamicRecord>,SchemaRule> schemaRule )
    {
        schemaRuleRecords.put( id, schemaRule );
    }

    private static enum PropertyEntityType
    {
        NODE
        {
            @Override
            void add( WriteTransaction tx, PrimitiveRecord record )
            {
                NodeRecord nodeRecord = (NodeRecord) record;
                tx.addNodeRecord( nodeRecord.clone(), nodeRecord );
            }
            
            @Override
            void setEntityId( PropertyRecord record, long entityId )
            {
                record.setNodeId( entityId );
            }
        },
        RELATIONSHIP
        {
            @Override
            void add( WriteTransaction tx, PrimitiveRecord record )
            {
                tx.addRelationshipRecord( (RelationshipRecord) record );
            }
            
            @Override
            void setEntityId( PropertyRecord record, long entityId )
            {
                record.setRelId( entityId );
            }
        },
        GRAPH
        {
            @Override
            void add( WriteTransaction tx, PrimitiveRecord record )
            {
                tx.neoStoreRecord = (NeoStoreRecord) record;
            }
            
            @Override
            void setEntityId( PropertyRecord record, long entityId )
            {
            }
        };

        abstract void add( WriteTransaction tx, PrimitiveRecord record );
        
        abstract void setEntityId( PropertyRecord record, long entityId );
    }
    
    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( getOrLoadNodeRecord( nodeId ),
                getNodeStore() );
        manipulator.add( labelId );
    }
    
    @Override
    public void removeLabelFromNode( long labelId, long nodeId )
    {
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( getOrLoadNodeRecord( nodeId ),
                getNodeStore() );
        manipulator.remove( labelId );
    }
    
    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        // Don't consider changes in this transaction
        NodeRecord node = getNodeStore().getRecord( nodeId );
        return asIterable( getNodeStore().getLabelsForNode( node ) );
    }
}
