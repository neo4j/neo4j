/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

public class RelationshipVisitor extends EntityVisitor
{
    private final BatchingRelationshipTypeTokenRepository relationshipTypeTokenRepository;
    private final IdMapper idMapper;
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord relationshipRecord;
    private final BatchingIdGetter relationshipIds;

    protected RelationshipVisitor( NeoStores stores, BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            BatchingRelationshipTypeTokenRepository relationshipTypeTokenRepository, IdMapper idMapper )
    {
        super( stores.getPropertyStore(), propertyKeyTokenRepository );
        this.relationshipTypeTokenRepository = relationshipTypeTokenRepository;
        this.idMapper = idMapper;
        this.relationshipStore = stores.getRelationshipStore();
        this.relationshipRecord = relationshipStore.newRecord();
        this.relationshipIds = new BatchingIdGetter( relationshipStore );
        relationshipRecord.setInUse( true );
    }

    @Override
    protected PrimitiveRecord primitiveRecord()
    {
        return relationshipRecord;
    }

    @Override
    public boolean startId( long id, Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean startId( String id, Group group )
    {
        long nodeId = idMapper.get( id, group );
        if ( nodeId == ID_NOT_FOUND )
        {
            relationshipRecord.setInUse( false );
            return false;
        }

        relationshipRecord.setFirstNode( nodeId );
        return true;
    }

    @Override
    public boolean endId( long id, Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean endId( String id, Group group )
    {
        long nodeId = idMapper.get( id, group );
        if ( nodeId == ID_NOT_FOUND )
        {
            relationshipRecord.setInUse( false );
            return false;
        }

        relationshipRecord.setSecondNode( nodeId );
        return true;
    }

    @Override
    public boolean type( int type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean type( String type )
    {
        relationshipRecord.setType( relationshipTypeTokenRepository.getOrCreateId( type ) );
        return true;
    }

    @Override
    public void endOfEntity()
    {
        if ( relationshipRecord.inUse() )
        {
            relationshipRecord.setId( relationshipIds.next() );
            relationshipRecord.setNextProp( createAndWritePropertyChain() );
            relationshipStore.updateRecord( relationshipRecord );
        }
        // TODO else collect, right?

        relationshipRecord.clear();
        relationshipRecord.setInUse( true );
        super.endOfEntity();
    }
}
