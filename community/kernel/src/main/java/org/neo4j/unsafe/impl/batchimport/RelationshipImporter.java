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
package org.neo4j.unsafe.impl.batchimport;

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.DataImporter.Monitor;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.Client;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.PrepareIdSequence;

import static java.lang.String.format;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Imports relationships using data from {@link InputChunk}.
 */
public class RelationshipImporter extends EntityImporter
{
    private final BatchingRelationshipTypeTokenRepository relationshipTypeTokenRepository;
    private final IdMapper idMapper;
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord relationshipRecord;
    private final BatchingIdGetter relationshipIds;
    private final Client typeCounts;
    private final Collector badCollector;
    private final boolean validateRelationshipData;
    private final boolean doubleRecordUnits;
    private final LongFunction<IdSequence> prepareIdSequence;

    private long relationshipCount;

    // State to keep in the event of bad relationships that need to be handed to the Collector
    private Object startId;
    private Group startIdGroup;
    private Object endId;
    private Group endIdGroup;
    private String type;

    protected RelationshipImporter( BatchingNeoStores stores, IdMapper idMapper,
            DataStatistics typeDistribution,
            Monitor monitor, Collector badCollector, boolean validateRelationshipData, boolean doubleRecordUnits )
    {
        super( stores, monitor );
        this.doubleRecordUnits = doubleRecordUnits;
        this.relationshipTypeTokenRepository = stores.getRelationshipTypeRepository();
        this.idMapper = idMapper;
        this.badCollector = badCollector;
        this.validateRelationshipData = validateRelationshipData;
        this.relationshipStore = stores.getRelationshipStore();
        this.relationshipRecord = relationshipStore.newRecord();
        this.relationshipIds = new BatchingIdGetter( relationshipStore );
        this.typeCounts = typeDistribution.newClient();
        this.prepareIdSequence = PrepareIdSequence.of( doubleRecordUnits ).apply( stores.getRelationshipStore() );
        relationshipRecord.setInUse( true );
    }

    @Override
    protected PrimitiveRecord primitiveRecord()
    {
        return relationshipRecord;
    }

    @Override
    public boolean startId( long id )
    {
        relationshipRecord.setFirstNode( id );
        return true;
    }

    @Override
    public boolean startId( Object id, Group group )
    {
        this.startId = id;
        this.startIdGroup = group;

        long nodeId = nodeId( id, group );
        relationshipRecord.setFirstNode( nodeId );
        return true;
    }

    @Override
    public boolean endId( long id )
    {
        relationshipRecord.setSecondNode( id );
        return true;
    }

    @Override
    public boolean endId( Object id, Group group )
    {
        this.endId = id;
        this.endIdGroup = group;

        long nodeId = nodeId( id, group );
        relationshipRecord.setSecondNode( nodeId );
        return true;
    }

    private long nodeId( Object id, Group group )
    {
        long nodeId = idMapper.get( id, group );
        if ( nodeId == ID_NOT_FOUND )
        {
            relationshipRecord.setInUse( false );
            return ID_NOT_FOUND;
        }

        return nodeId;
    }

    @Override
    public boolean type( int typeId )
    {
        relationshipRecord.setType( typeId );
        return true;
    }

    @Override
    public boolean type( String type )
    {
        this.type = type;
        int typeId = relationshipTypeTokenRepository.getOrCreateId( type );
        return type( typeId );
    }

    @Override
    public void endOfEntity()
    {
        if ( relationshipRecord.inUse() &&
                relationshipRecord.getFirstNode() != ID_NOT_FOUND &&
                relationshipRecord.getSecondNode() != ID_NOT_FOUND &&
                relationshipRecord.getType() != -1 )
        {
            relationshipRecord.setId( relationshipIds.next() );
            if ( doubleRecordUnits )
            {
                // simply reserve one id for this relationship to grow during linking stage
                relationshipIds.next();
            }
            relationshipRecord.setNextProp( createAndWritePropertyChain() );
            relationshipRecord.setFirstInFirstChain( false );
            relationshipRecord.setFirstInSecondChain( false );
            relationshipRecord.setFirstPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            relationshipRecord.setSecondPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            relationshipStore.prepareForCommit( relationshipRecord, prepareIdSequence.apply( relationshipRecord.getId() ) );
            relationshipStore.updateRecord( relationshipRecord );
            relationshipCount++;
            typeCounts.increment( relationshipRecord.getType() );
        }
        else
        {
            if ( validateRelationshipData )
            {
                validateNode( startId, Type.START_ID );
                validateNode( endId, Type.END_ID );
                if ( relationshipRecord.getType() == -1 )
                {
                    throw new MissingRelationshipDataException( Type.TYPE,
                            relationshipDataString() + " is missing " + Type.TYPE + " field" );
                }
            }
            badCollector.collectBadRelationship( startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name(),
                    relationshipRecord.getFirstNode() == ID_NOT_FOUND ? startId : endId );
        }

        relationshipRecord.clear();
        relationshipRecord.setInUse( true );
        startId = null;
        startIdGroup = null;
        endId = null;
        endIdGroup = null;
        type = null;
        super.endOfEntity();
    }

    private Group group( Group group )
    {
        return group != null ? group : Group.GLOBAL;
    }

    private void validateNode( Object id, Type fieldType )
    {
        if ( id == null )
        {
            throw new MissingRelationshipDataException( fieldType, relationshipDataString() +
                    " is missing " + fieldType + " field" );
        }
    }

    private String relationshipDataString()
    {
        return format( "start:%s (%s) type:%s end:%s (%s)",
                startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name() );
    }

    @Override
    public void close()
    {
        super.close();
        typeCounts.close();
        monitor.relationshipsImported( relationshipCount );
    }
}
