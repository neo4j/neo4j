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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.CommandSet;
import org.neo4j.kernel.impl.util.ArrayMap;

public class NeoStoreTransactionContext
{
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final NeoStoreTransactionContextSupplier supplier;
    private final PropertyTraverser propertyTraverser;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final TransactionalRelationshipLocker locker;
    private final RelationshipGroupGetter relationshipGroupGetter;

    private TransactionState txState;

    private final RecordChangeSet recordChangeSet;
    private final CommandSet commandSet;
    private final NeoStore neoStore;

    public NeoStoreTransactionContext( NeoStoreTransactionContextSupplier supplier, NeoStore neoStore )
    {
        this.supplier = supplier;
        this.neoStore = neoStore;

        recordChangeSet = new RecordChangeSet( neoStore );
        commandSet = new CommandSet();

        locker = new TransactionalRelationshipLocker();
        relationshipGroupGetter = new RelationshipGroupGetter( neoStore.getRelationshipGroupStore() );
        propertyTraverser = new PropertyTraverser();
        propertyCreator = new PropertyCreator( neoStore.getPropertyStore(), propertyTraverser );
        propertyDeleter = new PropertyDeleter( neoStore.getPropertyStore(), propertyTraverser );
        relationshipCreator = new RelationshipCreator( locker, relationshipGroupGetter, neoStore.getDenseNodeThreshold() );
        relationshipDeleter = new RelationshipDeleter( locker, relationshipGroupGetter, propertyDeleter );
    }

    public ArrayMap<Integer, DefinedProperty> relationshipDelete( long relId )
    {
        return relationshipDeleter.relDelete( relId, recordChangeSet );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet );
    }

    public void commitCows( CacheAccessBackDoor cacheAccess )
    {
        txState.commitCows( cacheAccess );
    }

    public void updateFirstRelationships()
    {
        for ( RecordProxy<Long, NodeRecord, Void> change : recordChangeSet.getNodeRecords().changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            long firstRelId = record.getNextRel();
            txState.setFirstIds( record.getId(), record.isDense(), firstRelId, record.getNextProp() );
        }
        for ( RecordProxy<Long,RelationshipGroupRecord,Integer> change : recordChangeSet.getRelGroupRecords().changes() )
        {
            RelationshipGroupRecord record = change.forReadingLinkage();
            txState.addRelationshipGroupChange( record.getOwningNode(), record );
        }
    }

    public ArrayMap<Integer, DefinedProperty>  getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        return propertyDeleter.getAndDeletePropertyChain( nodeRecord,
                recordChangeSet.getPropertyRecords() );
    }

    public <T extends PrimitiveRecord> void removeProperty( RecordProxy<Long,T,Void> primitiveProxy, int propertyKey )
    {
        propertyDeleter.removeProperty( primitiveProxy, propertyKey, getPropertyRecords() );
    }

    public <P extends PrimitiveRecord> void primitiveChangeProperty( RecordProxy<Long, P, Void> primitive,
            int propertyKey, Object value )
    {
        propertyCreator.primitiveChangeProperty( primitive, propertyKey, value, getPropertyRecords() );
    }

    public <P extends PrimitiveRecord> void primitiveAddProperty( RecordProxy<Long, P, Void> primitive,
            int propertyKey, Object value )
    {
        propertyCreator.primitiveAddProperty( primitive, propertyKey, value, getPropertyRecords() );
    }

    public void createPropertyKeyToken( String name, int id )
    {
        TokenCreator<PropertyKeyTokenRecord> creator =
                new TokenCreator<>( neoStore.getPropertyKeyTokenStore() );
        creator.createToken( name, id, getPropertyKeyTokenRecords() );
    }

    public void createLabelToken( String name, int id )
    {
        TokenCreator<LabelTokenRecord> creator =
                new TokenCreator<>( neoStore.getLabelTokenStore() );
        creator.createToken( name, id, getLabelTokenRecords() );
    }

    public void createRelationshipTypeToken( String name, int id )
    {
        TokenCreator<RelationshipTypeTokenRecord> creator =
                new TokenCreator<>( neoStore.getRelationshipTypeStore() );
        creator.createToken( name, id, getRelationshipTypeTokenRecords() );
    }

    public void bind( TransactionState txState )
    {
        this.txState = txState;
        locker.setLockClient( txState.locks() );
    }

    public void close()
    {
        recordChangeSet.close();
        commandSet.close();

        locker.setLockClient( null );
        txState = null;
        supplier.release( this );
    }

    public Map<Long, org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand> getNodeCommands()
    {
        return commandSet.getNodeCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand> getPropCommands()
    {
        return commandSet.getPropCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipCommand> getRelCommands()
    {
        return commandSet.getRelCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand> getSchemaRuleCommands()
    {
        return commandSet.getSchemaRuleCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipTypeTokenCommand> getRelationshipTypeTokenCommands()
    {
        return commandSet.getRelationshipTypeTokenCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.LabelTokenCommand> getLabelTokenCommands()
    {
        return commandSet.getLabelTokenCommands();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyKeyTokenCommand> getPropertyKeyTokenCommands()
    {
        return commandSet.getPropertyKeyTokenCommands();
    }

    public RecordChanges<Long, NodeRecord, Void> getNodeRecords()
    {
        return recordChangeSet.getNodeRecords();
    }

    public RecordChanges<Long, RelationshipRecord, Void> getRelRecords()
    {
        return recordChangeSet.getRelRecords();
    }

    public RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
    {
        return recordChangeSet.getSchemaRuleChanges();
    }

    public RecordChanges<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return recordChangeSet.getPropertyRecords();
    }

    public RecordChanges<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return recordChangeSet.getRelGroupRecords();
    }

    public RecordChanges<Integer,PropertyKeyTokenRecord,Void> getPropertyKeyTokenRecords()
    {
        return recordChangeSet.getPropertyKeyTokenChanges();
    }

    public RecordChanges<Integer,LabelTokenRecord,Void> getLabelTokenRecords()
    {
        return recordChangeSet.getLabelTokenChanges();
    }

    public RecordChanges<Integer,RelationshipTypeTokenRecord,Void> getRelationshipTypeTokenRecords()
    {
        return recordChangeSet.getRelationshipTypeTokenChanges();
    }

    public void generateNeoStoreCommand( NeoStoreRecord neoStoreRecord )
    {
        commandSet.generateNeoStoreCommand( neoStoreRecord );
    }

    public Command.NeoStoreCommand getNeoStoreCommand()
    {
        return commandSet.getNeoStoreCommand();
    }

    public ArrayList<org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipGroupCommand> getRelGroupCommands()
    {
        return commandSet.getRelGroupCommands();
    }

    public void setNeoStoreCommand( Command.NeoStoreCommand xaCommand )
    {
        commandSet.getNeoStoreCommand().init( xaCommand.getRecord() );
    }

    public RecordProxy<Long, RelationshipGroupRecord, Integer> getRelationshipGroup( NodeRecord node, int type )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        Set<Integer> allTypes = new HashSet<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordProxy<Long, RelationshipGroupRecord, Integer> change =
                    recordChangeSet.getRelGroupRecords().getOrLoad( groupId, type );
            RelationshipGroupRecord record = change.forReadingData();
            record.setPrev( previousGroupId ); // not persistent so not a "change"
            allTypes.add( record.getType() );
            if ( record.getType() == type )
            {
                return change;
            }
            previousGroupId = groupId;
            groupId = record.getNext();
        }
        return null;
    }

    public TransactionState getTransactionState()
    {
        return txState;
    }
}
