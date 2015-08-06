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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.RelationshipLoadingPosition;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.kernel.impl.store.NeoStore;
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
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

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
    private final RelationshipChainLoader relationshipLoader;

    private final RecordAccessSet recordChangeSet;
    private final NeoStore neoStore;

    public NeoStoreTransactionContext( NeoStoreTransactionContextSupplier supplier, NeoStore neoStore )
    {
        this( supplier, neoStore, new RecordChangeSet( neoStore ) );
    }

    public NeoStoreTransactionContext( NeoStoreTransactionContextSupplier supplier, NeoStore neoStore,
            RecordAccessSet recordAccessSet )
    {
        this.supplier = supplier;
        this.neoStore = neoStore;

        this.recordChangeSet = recordAccessSet;

        locker = new TransactionalRelationshipLocker();
        relationshipGroupGetter = new RelationshipGroupGetter( neoStore.getRelationshipGroupStore() );
        propertyTraverser = new PropertyTraverser();
        propertyCreator = new PropertyCreator( neoStore.getPropertyStore(), propertyTraverser );
        propertyDeleter = new PropertyDeleter( neoStore.getPropertyStore(), propertyTraverser );
        relationshipCreator = new RelationshipCreator( locker, relationshipGroupGetter, neoStore.getDenseNodeThreshold() );
        relationshipDeleter = new RelationshipDeleter( locker, relationshipGroupGetter, propertyDeleter );
        relationshipLoader = new RelationshipChainLoader( neoStore );
    }

    public RecordAccessSet getRecordChangeSet()
    {
        return recordChangeSet;
    }

    public ArrayMap<Integer, DefinedProperty> relationshipDelete( long relId )
    {
        return relationshipDeleter.relDelete( relId, recordChangeSet );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet );
    }

    public ArrayMap<Integer, DefinedProperty> getAndDeletePropertyChain( NodeRecord nodeRecord )
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
                new TokenCreator<>( neoStore.getRelationshipTypeTokenStore() );
        creator.createToken( name, id, getRelationshipTypeTokenRecords() );
    }

    public void bind( Client locksClient )
    {
        locker.setLockClient( locksClient );
    }

    public void initialize()
    {
        recordChangeSet.close();
    }

    public void close()
    {
        recordChangeSet.close();

        locker.setLockClient( null );
        supplier.release( this );
    }

    public RecordAccess<Long, NodeRecord, Void> getNodeRecords()
    {
        return recordChangeSet.getNodeRecords();
    }

    public RecordAccess<Long, RelationshipRecord, Void> getRelRecords()
    {
        return recordChangeSet.getRelRecords();
    }

    public RecordAccess<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
    {
        return recordChangeSet.getSchemaRuleChanges();
    }

    public RecordAccess<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return recordChangeSet.getPropertyRecords();
    }

    public RecordAccess<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return recordChangeSet.getRelGroupRecords();
    }

    public RecordAccess<Integer,PropertyKeyTokenRecord,Void> getPropertyKeyTokenRecords()
    {
        return recordChangeSet.getPropertyKeyTokenChanges();
    }

    public RecordAccess<Integer,LabelTokenRecord,Void> getLabelTokenRecords()
    {
        return recordChangeSet.getLabelTokenChanges();
    }

    public RecordAccess<Integer,RelationshipTypeTokenRecord,Void> getRelationshipTypeTokenRecords()
    {
        return recordChangeSet.getRelationshipTypeTokenChanges();
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

    public boolean hasChanges()
    {
        return recordChangeSet.hasChanges();
    }

    public int getRelationshipCount( long id, int type, DirectionWrapper direction )
    {
        return relationshipLoader.getRelationshipCount( id, type, direction );
    }

    public Integer[] getRelationshipTypes( long id )
    {
        return relationshipLoader.getRelationshipTypes( id );
    }

    public RelationshipLoadingPosition getRelationshipLoadingChainPoisition( long id )
    {
        return relationshipLoader.getRelationshipChainPosition( id );
    }
}
