/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.util.ArrayMap;

public class NeoStoreTransactionContext
{
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final NeoStoreTransactionContextSupplier supplier;
    private final PropertyChainDeleter propertyChainDeleter;
    private final TransactionalRelationshipLocker locker;
    private final TransactionalRelationshipGroupGetter transactionalRelationshipGroupGetter;

    private TransactionState txState;

    private final RecordChangeSet recordChangeSet;

    public NeoStoreTransactionContext( NeoStoreTransactionContextSupplier supplier, NeoStore neoStore )
    {
        this.supplier = supplier;

        recordChangeSet = new RecordChangeSet( neoStore );

        locker = new TransactionalRelationshipLocker();
        transactionalRelationshipGroupGetter = new TransactionalRelationshipGroupGetter( recordChangeSet.getRelGroupRecords() );
        propertyChainDeleter = new PropertyChainDeleter( neoStore.getPropertyStore() );
        relationshipCreator = new RelationshipCreator( locker, transactionalRelationshipGroupGetter, neoStore );
        relationshipDeleter = new RelationshipDeleter( locker, transactionalRelationshipGroupGetter, propertyChainDeleter);
    }

    public ArrayMap<Integer, DefinedProperty> relationshipDelete( long relId )
    {
        return relationshipDeleter.relDelete( relId, recordChangeSet );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet );
    }

    public Collection<NodeRecord> getUpgradedDenseNodes()
    {
        return relationshipCreator.getUpgradedDenseNodes();
    }

    public void commitCows()
    {
        txState.commitCows();
    }

    public void updateFirstRelationships()
    {
        for ( RecordChanges.RecordChange<Long, NodeRecord, Void> change : recordChangeSet.getNodeRecords().changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            txState.setFirstIds( record.getId(), record.getNextRel(), record.getNextProp() );
        }
    }

    public ArrayMap<Integer, DefinedProperty>  getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        return propertyChainDeleter.getAndDeletePropertyChain( nodeRecord,
                recordChangeSet.getPropertyRecords() );
    }

    public void bind( TransactionState txState )
    {
        this.txState = txState;
        locker.setTransactionState( txState );
    }

    public void close()
    {
        recordChangeSet.close();
        locker.setTransactionState( null );
        txState = null;
        supplier.release( this );
    }

    public Map<Long, Command.NodeCommand> getNodeCommands()
    {
        return recordChangeSet.getNodeCommands();
    }

    public ArrayList<Command.PropertyCommand> getPropCommands()
    {
        return recordChangeSet.getPropCommands();
    }

    public ArrayList<Command.RelationshipCommand> getRelCommands()
    {
        return recordChangeSet.getRelCommands();
    }

    public ArrayList<Command.SchemaRuleCommand> getSchemaRuleCommands()
    {
        return recordChangeSet.getSchemaRuleCommands();
    }

    public ArrayList<Command.RelationshipTypeTokenCommand> getRelationshipTypeTokenCommands()
    {
        return recordChangeSet.getRelationshipTypeTokenCommands();
    }

    public ArrayList<Command.LabelTokenCommand> getLabelTokenCommands()
    {
        return recordChangeSet.getLabelTokenCommands();
    }

    public ArrayList<Command.PropertyKeyTokenCommand> getPropertyKeyTokenCommands()
    {
        return recordChangeSet.getPropertyKeyTokenCommands();
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

    public void generateNeoStoreCommand( NeoStoreRecord neoStoreRecord )
    {
        recordChangeSet.generateNeoStoreCommand( neoStoreRecord );
    }

    public XaCommand getNeoStoreCommand()
    {
        return recordChangeSet.getNeoStoreCommand();
    }

    public ArrayList<Command.RelationshipGroupCommand> getRelGroupCommands()
    {
        return recordChangeSet.getRelGroupCommands();
    }

    public void setNeoStoreCommand( Command.NeoStoreCommand xaCommand )
    {
        recordChangeSet.setNeoStoreCommand( xaCommand );
    }

    public RecordChanges.RecordChange<Long, RelationshipGroupRecord, Integer> getRelationshipGroup( NodeRecord node,
                                                                                                    int type )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        Set<Integer> allTypes = new HashSet<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordChanges.RecordChange<Long, RelationshipGroupRecord, Integer> change =
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
