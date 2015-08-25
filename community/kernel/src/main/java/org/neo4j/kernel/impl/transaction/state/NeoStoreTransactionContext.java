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

public class NeoStoreTransactionContext
{
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final RecordAccessSet recordChangeSet;

    private final NeoStore neoStore;

    public NeoStoreTransactionContext( NeoStore neoStore, Client locks )
    {
        this.neoStore = neoStore;

        recordChangeSet = new RecordChangeSet( neoStore );
        RelationshipGroupGetter relGroupGetter = new RelationshipGroupGetter( neoStore.getRelationshipGroupStore() );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        propertyCreator = new PropertyCreator( neoStore.getPropertyStore(), propertyTraverser );
        propertyDeleter = new PropertyDeleter( neoStore.getPropertyStore(), propertyTraverser );
        relationshipCreator = new RelationshipCreator( locks, relGroupGetter, neoStore.getDenseNodeThreshold() );
        relationshipDeleter = new RelationshipDeleter( locks, relGroupGetter, propertyDeleter );
    }

    public void relationshipDelete( long relId )
    {
        relationshipDeleter.relDelete( relId, recordChangeSet );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        relationshipCreator.relationshipCreate( id, typeId, startNodeId, endNodeId, recordChangeSet );
    }

    public void getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        propertyDeleter.deletePropertyChain( nodeRecord, recordChangeSet.getPropertyRecords() );
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

    public void initialize()
    {
        recordChangeSet.close();
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
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordProxy<Long, RelationshipGroupRecord, Integer> change =
                    recordChangeSet.getRelGroupRecords().getOrLoad( groupId, type );
            RelationshipGroupRecord record = change.forReadingData();
            record.setPrev( previousGroupId ); // not persistent so not a "change"
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
}
