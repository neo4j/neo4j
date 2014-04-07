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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;

import static org.neo4j.helpers.collection.IteratorUtil.first;

public class RelationshipWriter extends Thread
{
    private final ArrayBlockingQueue<RelChainBuilder> chainsToWrite;
    private final int denseNodeThreshold;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relGroupStore;
    private final LegacyNodeStoreReader nodeReader;
    private final AtomicReference<Throwable> caughtException;

    public RelationshipWriter( ArrayBlockingQueue<RelChainBuilder> chainsToWrite, int denseNodeThreshold, NodeStore
            nodeStore, RelationshipStore relationshipStore, RelationshipGroupStore relGroupStore,
            LegacyNodeStoreReader nodeReader, AtomicReference<Throwable> caughtException )
    {
        this.chainsToWrite = chainsToWrite;
        this.denseNodeThreshold = denseNodeThreshold;
        this.nodeStore = nodeStore;
        this.relationshipStore = relationshipStore;
        this.relGroupStore = relGroupStore;
        this.nodeReader = nodeReader;
        this.caughtException = caughtException;
    }

    @Override
    public void run()
    {
        try
        {
            while(true)
            {
                RelChainBuilder next = chainsToWrite.take();
                if(next.nodeId() == -1)
                {
                    // Signals that there are no more chains.
                    return;
                }

                if ( next.size() >= denseNodeThreshold )
                {
                    migrateDenseNode( nodeStore, relationshipStore, relGroupStore, next );
                }
                else
                {
                    migrateNormalNode( nodeStore, relationshipStore, next );
                }
            }
        }
        catch ( InterruptedException | IOException e )
        {
            caughtException.set( e );
        }
    }

    private void migrateNormalNode( NodeStore nodeStore, RelationshipStore relationshipStore,
                                    RelChainBuilder relationships ) throws IOException
    {
        /* Add node record
         * Add/update all relationship records */
        nodeStore.forceUpdateRecord( nodeReader.readNodeStore( relationships.nodeId() ) );
        int i = 0;
        for ( RelationshipRecord record : relationships )
        {
            if ( i == 0 )
            {
                setDegree( relationships.nodeId(), record, relationships.size() );
            }
            applyChangesToRecord( relationships.nodeId(), record, relationshipStore );
            relationshipStore.forceUpdateRecord( record );
            i++;
        }
    }

    private void migrateDenseNode( NodeStore nodeStore, RelationshipStore relationshipStore,
                                   RelationshipGroupStore relGroupStore, RelChainBuilder relChain ) throws IOException
    {
        Map<Integer, Relationships> byType = splitUp( relChain.nodeId(), relChain );
        List<RelationshipGroupRecord> groupRecords = new ArrayList<>();
        for ( Map.Entry<Integer, Relationships> entry : byType.entrySet() )
        {
            Relationships relationships = entry.getValue();
            applyLinks( relChain.nodeId(), relationships.out, relationshipStore, Direction.OUTGOING );
            applyLinks( relChain.nodeId(), relationships.in, relationshipStore, Direction.INCOMING );
            applyLinks( relChain.nodeId(), relationships.loop, relationshipStore, Direction.BOTH );
            RelationshipGroupRecord groupRecord = new RelationshipGroupRecord( relGroupStore.nextId(), entry.getKey() );
            groupRecords.add( groupRecord );
            groupRecord.setInUse( true );
            groupRecord.setOwningNode( relChain.nodeId() );
            if ( !relationships.out.isEmpty() )
            {
                groupRecord.setFirstOut( first( relationships.out ).getId() );
            }
            if ( !relationships.in.isEmpty() )
            {
                groupRecord.setFirstIn( first( relationships.in ).getId() );
            }
            if ( !relationships.loop.isEmpty() )
            {
                groupRecord.setFirstLoop( first( relationships.loop ).getId() );
            }
        }

        RelationshipGroupRecord previousGroup = null;
        for ( int i = 0; i < groupRecords.size(); i++ )
        {
            RelationshipGroupRecord groupRecord = groupRecords.get( i );
            if ( i+1 < groupRecords.size() )
            {
                RelationshipGroupRecord nextRecord = groupRecords.get( i+1 );
                groupRecord.setNext( nextRecord.getId() );
            }
            if ( previousGroup != null )
            {
                groupRecord.setPrev( previousGroup.getId() );
            }
            previousGroup = groupRecord;
        }
        for ( RelationshipGroupRecord groupRecord : groupRecords )
        {
            relGroupStore.forceUpdateRecord( groupRecord );
        }

        NodeRecord node = nodeReader.readNodeStore( relChain.nodeId() );
        node.setNextRel( groupRecords.get( 0 ).getId() );
        node.setDense( true );
        nodeStore.forceUpdateRecord( node );
    }

    private void applyLinks( long nodeId, List<RelationshipRecord> records, RelationshipStore relationshipStore, Direction dir )
    {
        for ( int i = 0; i < records.size(); i++ )
        {
            RelationshipRecord record = records.get( i );
            if ( i > 0 )
            {   // link previous
                long previous = records.get( i-1 ).getId();
                if ( record.getFirstNode() == nodeId )
                {
                    record.setFirstPrevRel( previous );
                }
                if ( record.getSecondNode() == nodeId )
                {
                    record.setSecondPrevRel( previous );
                }
            }
            else
            {
                setDegree( nodeId, record, records.size() );
            }

            if ( i < records.size()-1 )
            {   // link next
                long next = records.get( i+1 ).getId();
                if ( record.getFirstNode() == nodeId )
                {
                    record.setFirstNextRel( next );
                }
                if ( record.getSecondNode() == nodeId )
                {
                    record.setSecondNextRel( next );
                }
            }
            else
            {   // end of chain
                if ( record.getFirstNode() == nodeId )
                {
                    record.setFirstNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                }
                if ( record.getSecondNode() == nodeId )
                {
                    record.setSecondNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                }
            }
            applyChangesToRecord( nodeId, record, relationshipStore );
            relationshipStore.forceUpdateRecord( record );
        }
    }

    private void setDegree( long nodeId, RelationshipRecord record, int size )
    {
        if ( nodeId == record.getFirstNode() )
        {
            record.setFirstInFirstChain( true );
            record.setFirstPrevRel( size );
        }
        if ( nodeId == record.getSecondNode() )
        {
            record.setFirstInSecondChain( true );
            record.setSecondPrevRel( size );
        }
    }

    private void applyChangesToRecord( long nodeId, RelationshipRecord record, RelationshipStore relationshipStore )
    {
        RelationshipRecord existingRecord = relationshipStore.getLightRel( record.getId() );
        if(existingRecord == null)
        {
            return;
        }

        // Not necessary for loops since those records will just be copied.
        if ( nodeId == record.getFirstNode() )
        {   // Copy end node stuff from the existing record
            record.setFirstInSecondChain( existingRecord.isFirstInSecondChain() );
            record.setSecondPrevRel( existingRecord.getSecondPrevRel() );
            record.setSecondNextRel( existingRecord.getSecondNextRel() );
        }
        else
        {   // Copy start node stuff from the existing record
            record.setFirstInFirstChain( existingRecord.isFirstInFirstChain() );
            record.setFirstPrevRel( existingRecord.getFirstPrevRel() );
            record.setFirstNextRel( existingRecord.getFirstNextRel() );
        }
    }

    private Map<Integer, Relationships> splitUp( long nodeId, RelChainBuilder records )
    {
        Map<Integer, Relationships> result = new HashMap<>();
        for ( RelationshipRecord record : records )
        {
            Integer type = record.getType();
            Relationships relationships = result.get( type );
            if ( relationships == null )
            {
                relationships = new Relationships( nodeId );
                result.put( type, relationships );
            }
            relationships.add( record );
        }
        return result;
    }


    private static class Relationships
    {
        private final long nodeId;
        final List<RelationshipRecord> out = new ArrayList<>();
        final List<RelationshipRecord> in = new ArrayList<>();
        final List<RelationshipRecord> loop = new ArrayList<>();

        Relationships( long nodeId )
        {
            this.nodeId = nodeId;
        }

        void add( RelationshipRecord record )
        {
            if ( record.getFirstNode() == nodeId )
            {
                if ( record.getSecondNode() == nodeId )
                {   // Loop
                    loop.add( record );
                }
                else
                {   // Out
                    out.add( record );
                }
            }
            else
            {   // In
                in.add( record );
            }
        }

        @Override
        public String toString()
        {
            return "Relationships[" + nodeId + ",out:" + out.size() + ", in:" + in.size() + ", loop:" + loop.size() + "]";
        }
    }
}
