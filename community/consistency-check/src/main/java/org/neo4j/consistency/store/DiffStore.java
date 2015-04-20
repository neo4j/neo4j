/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.store;

import java.util.Collection;

import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.CommandRecordVisitor;

/**
 * Not thread safe (since DiffRecordStore is not thread safe), intended for
 * single threaded use.
 */
public class DiffStore extends StoreAccess implements CommandRecordVisitor
{
    private NeoStoreRecord masterRecord;

    public DiffStore( NeoStore store )
    {
        super( store );
    }

    @Override
    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return new DiffRecordStore<>( store );
    }

    @Override
    protected <FAILURE extends Exception> void apply( RecordStore.Processor<FAILURE> processor, RecordStore<?> store ) throws FAILURE
    {
        processor.applyById( store, (DiffRecordStore<?>) store );
    }

    @Override
    public void visitNode( NodeRecord record )
    {
        getNodeStore().forceUpdateRecord( record );
        record = getNodeStore().forceGetRaw( record );
        if ( record.inUse() )
        {
            markProperty( record.getNextProp(), record.getId(), -1 );
            markRelationship( record.getNextRel() );
        }
    }

    @Override
    public void visitRelationship( RelationshipRecord record )
    {
        getRelationshipStore().forceUpdateRecord( record );
        record = getRelationshipStore().forceGetRaw( record );
        if ( record.inUse() )
        {
            getNodeStore().markDirty( record.getFirstNode() );
            getNodeStore().markDirty( record.getSecondNode() );
            markProperty( record.getNextProp(), -1, record.getId() );
            markRelationship( record.getFirstNextRel() );
            if ( !record.isFirstInFirstChain() )
            {
                markRelationship( record.getFirstPrevRel() );
            }
            markRelationship( record.getSecondNextRel() );
            if ( !record.isFirstInSecondChain() )
            {
                markRelationship( record.getSecondPrevRel() );
            }
        }
    }

    private void markRelationship( long rel )
    {
        if ( !Record.NO_NEXT_RELATIONSHIP.is( rel ) )
        {
            getRelationshipStore().markDirty( rel );
        }
    }

    private void markRelationshipGroup( long group )
    {
        if ( !Record.NO_NEXT_RELATIONSHIP.is( group ) )
        {
            getRelationshipGroupStore().markDirty( group );
        }
    }

    private void markProperty( long prop, long nodeId, long relId )
    {
        if ( !Record.NO_NEXT_PROPERTY.is( prop ) )
        {
            DiffRecordStore<PropertyRecord> store = getPropertyStore();
            PropertyRecord record = store.forceGetRaw( prop );
            if ( nodeId != -1 )
            {
                record.setNodeId( nodeId );
            }
            else if ( relId != -1 )
            {
                record.setRelId( relId );
            }
            store.updateRecord( record );
        }
    }

    @Override
    public void visitProperty( PropertyRecord record )
    {
        getPropertyStore().forceUpdateRecord( record );
        updateDynamic( record );
        record = getPropertyStore().forceGetRaw( record );
        updateDynamic( record );
        if ( record.inUse() )
        {
            markProperty( record.getNextProp(), record.getNodeId(), record.getRelId() );
            markProperty( record.getPrevProp(), record.getNodeId(), record.getRelId() );
        }
    }

    private void updateDynamic( PropertyRecord record )
    {
        for ( PropertyBlock block : record )
        {
            updateDynamic( block.getValueRecords() );
        }
        updateDynamic( record.getDeletedRecords() );
    }

    private void updateDynamic( Collection<DynamicRecord> records )
    {
        for ( DynamicRecord record : records )
        {
            DiffRecordStore<DynamicRecord> store = ( record.getType() == PropertyType.STRING.intValue() )
                    ? getStringStore() : getArrayStore();
            store.forceUpdateRecord( record );
            if ( !Record.NO_NEXT_BLOCK.is( record.getNextBlock() ) )
            {
                getBlockStore(record.getType()).markDirty( record.getNextBlock() );
            }
        }
    }

    private DiffRecordStore getBlockStore( int type )
    {
        if ( type == PropertyType.STRING.intValue() )
        {
            return getStringStore();
        }
        else
        {
            return getArrayStore();
        }
    }

    @Override
    public void visitPropertyKeyToken( PropertyKeyTokenRecord record )
    {
        visitNameStore( getPropertyKeyTokenStore(), getPropertyKeyNameStore(), record );
    }

    @Override
    public void visitRelationshipTypeToken( RelationshipTypeTokenRecord record )
    {
        visitNameStore( getRelationshipTypeTokenStore(), getRelationshipTypeNameStore(), record );
    }

    @Override
    public void visitLabelToken( LabelTokenRecord record )
    {
        visitNameStore( getLabelTokenStore(), getLabelNameStore(), record );
    }

    private <R extends TokenRecord> void visitNameStore( RecordStore<R> store, RecordStore<DynamicRecord> nameStore, R record )
    {
        store.forceUpdateRecord( record );
        for ( DynamicRecord key : record.getNameRecords() )
        {
            nameStore.forceUpdateRecord( key );
        }
    }

    @Override
    public void visitNeoStore( NeoStoreRecord record )
    {
        this.masterRecord = record;
    }

    @Override
    public void visitSchemaRule( Collection<DynamicRecord> records )
    {
        for ( DynamicRecord record : records )
        {
            getSchemaStore().forceUpdateRecord( record );
        }
    }

    @Override
    public void visitRelationshipGroup( RelationshipGroupRecord record )
    {
        getRelationshipGroupStore().forceUpdateRecord( record );
        record = getRelationshipGroupStore().forceGetRaw( record );
        if ( record.inUse() )
        {
            markRelationship( record.getFirstOut() );
            markRelationship( record.getFirstIn() );
            markRelationship( record.getFirstLoop() );
            markRelationshipGroup( record.getNext() );
        }
    }

    @Override
    public DiffRecordStore<DynamicRecord> getSchemaStore()
    {
        return (DiffRecordStore<DynamicRecord>) super.getSchemaStore();
    }

    @Override
    public DiffRecordStore<NodeRecord> getNodeStore()
    {
        return (DiffRecordStore<NodeRecord>) super.getNodeStore();
    }

    @Override
    public DiffRecordStore<RelationshipRecord> getRelationshipStore()
    {
        return (DiffRecordStore<RelationshipRecord>) super.getRelationshipStore();
    }

    @Override
    public DiffRecordStore<RelationshipGroupRecord> getRelationshipGroupStore()
    {
        return (DiffRecordStore<RelationshipGroupRecord>) super.getRelationshipGroupStore();
    }

    @Override
    public DiffRecordStore<PropertyRecord> getPropertyStore()
    {
        return (DiffRecordStore<PropertyRecord>) super.getPropertyStore();
    }

    @Override
    public DiffRecordStore<DynamicRecord> getStringStore()
    {
        return (DiffRecordStore<DynamicRecord>) super.getStringStore();
    }

    @Override
    public DiffRecordStore<DynamicRecord> getArrayStore()
    {
        return (DiffRecordStore<DynamicRecord>) super.getArrayStore();
    }

    @Override
    public DiffRecordStore<RelationshipTypeTokenRecord> getRelationshipTypeTokenStore()
    {
        return (DiffRecordStore<RelationshipTypeTokenRecord>) super.getRelationshipTypeTokenStore();
    }

    @Override
    public DiffRecordStore<DynamicRecord> getRelationshipTypeNameStore()
    {
        return (DiffRecordStore<DynamicRecord>) super.getRelationshipTypeNameStore();
    }

    @Override
    public DiffRecordStore<PropertyKeyTokenRecord> getPropertyKeyTokenStore()
    {
        return (DiffRecordStore<PropertyKeyTokenRecord>) super.getPropertyKeyTokenStore();
    }

    @Override
    public DiffRecordStore<DynamicRecord> getPropertyKeyNameStore()
    {
        return (DiffRecordStore<DynamicRecord>) super.getPropertyKeyNameStore();
    }

    public NeoStoreRecord getMasterRecord()
    {
        return masterRecord;
    }
}
