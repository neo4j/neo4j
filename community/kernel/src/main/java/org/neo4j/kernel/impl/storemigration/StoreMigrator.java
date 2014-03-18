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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * Migrates a neo4j database from one version to the next. Instantiated with a {@link LegacyStore}
 * representing the old version and a {@link NeoStore} representing the new version.
 *
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 */
public class StoreMigrator
{
    private final MigrationProgressMonitor progressMonitor;

    public StoreMigrator( MigrationProgressMonitor progressMonitor )
    {
        this.progressMonitor = progressMonitor;
    }

    public void migrate( LegacyStore legacyStore, NeoStore neoStore ) throws IOException
    {
        progressMonitor.started();
        new Migration( legacyStore, neoStore ).migrate();
        progressMonitor.finished();
    }

    protected class Migration
    {
        private final LegacyStore legacyStore;
        private final NeoStore neoStore;
        private final long totalEntities;
        private int percentComplete;

        public Migration( LegacyStore legacyStore, NeoStore neoStore )
        {
            this.legacyStore = legacyStore;
            this.neoStore = neoStore;
            totalEntities = legacyStore.getNodeStoreReader().getMaxId();
        }

        private void migrate() throws IOException
        {
            // Migrate
            migrateNodesAndRelationships();

            // Close
            neoStore.close();
            legacyStore.close();

            // Just copy unchanged stores that doesn't need migration
            legacyStore.copyNeoStore( neoStore );
            legacyStore.copyRelationshipTypeTokenStore( neoStore );
            legacyStore.copyRelationshipTypeTokenNameStore( neoStore );
            legacyStore.copyDynamicStringPropertyStore( neoStore );
            legacyStore.copyDynamicArrayPropertyStore( neoStore );
            legacyStore.copyPropertyStore( neoStore );
            legacyStore.copyPropertyKeyTokenStore( neoStore );
            legacyStore.copyPropertyKeyTokenNameStore( neoStore );
            legacyStore.copyLabelTokenStore( neoStore );
            legacyStore.copyLabelTokenNameStore( neoStore );
            legacyStore.copyNodeLabelStore( neoStore );
            legacyStore.copySchemaStore( neoStore );
            legacyStore.copyLegacyIndexStoreFile( neoStore.getStorageFileName().getParentFile() );
        }

        private void migrateNodesAndRelationships() throws IOException
        {
            /* For each node
             *   load the full relationship chain into memory
             *   if ( more than THRESHOLD relationships )
             *      store in dense node way
             *   else
             *      store in normal way
             *
             * Keep ids */

            NodeStore nodeStore = neoStore.getNodeStore();
            RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            RelationshipGroupStore relGroupStore = neoStore.getRelationshipGroupStore();
            LegacyNodeStoreReader nodeReader = legacyStore.getNodeStoreReader();
            LegacyRelationshipStoreReader relReader = legacyStore.getRelStoreReader();
            nodeStore.setHighId( nodeReader.getMaxId() );
            int denseNodeThreshold = neoStore.getDenseNodeThreshold();
            relationshipStore.setHighId( relReader.getMaxId() );
            try
            {
                for ( NodeRecord node : loop( nodeReader.readNodeStore() ) )
                {
                    reportProgress( node.getId() );
                    Collection<RelationshipRecord> relationships = loadRelationships( node, relReader );
                    if ( relationships.size() >= denseNodeThreshold )
                    {
                        migrateDenseNode( nodeStore, relationshipStore, relGroupStore, node, relationships );
                    }
                    else
                    {
                        migrateNormalNode( nodeStore, relationshipStore, node, relationships );
                    }
                }
                legacyStore.copyNodeStoreIdFile( neoStore );
                legacyStore.copyRelationshipStoreIdFile( neoStore );
            }
            finally
            {
                nodeReader.close();
                relReader.close();
            }
        }

        private void migrateNormalNode( NodeStore nodeStore, RelationshipStore relationshipStore,
                NodeRecord node, Collection<RelationshipRecord> relationships )
        {
            /* Add node record
             * Add/update all relationship records */
            nodeStore.forceUpdateRecord( node );
            int i = 0;
            for ( RelationshipRecord record : relationships )
            {
                if ( i == 0 )
                {
                    setDegree( node.getId(), record, relationships.size() );
                }
                applyChangesToRecord( node.getId(), record, relationshipStore );
                relationshipStore.forceUpdateRecord( record );
                i++;
            }
        }

        private void migrateDenseNode( NodeStore nodeStore, RelationshipStore relationshipStore,
                RelationshipGroupStore relGroupStore, NodeRecord node, Collection<RelationshipRecord> records )
        {
            Map<Integer, Relationships> byType = splitUp( node.getId(), records );
            List<RelationshipGroupRecord> groupRecords = new ArrayList<>();
            for ( Map.Entry<Integer, Relationships> entry : byType.entrySet() )
            {
                Relationships relationships = entry.getValue();
                applyLinks( node.getId(), relationships.out, relationshipStore, Direction.OUTGOING );
                applyLinks( node.getId(), relationships.in, relationshipStore, Direction.INCOMING );
                applyLinks( node.getId(), relationships.loop, relationshipStore, Direction.BOTH );
                RelationshipGroupRecord groupRecord = new RelationshipGroupRecord( relGroupStore.nextId(), entry.getKey() );
                groupRecords.add( groupRecord );
                groupRecord.setInUse( true );
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
            try
            {
                RelationshipRecord existingRecord = relationshipStore.getRecord( record.getId() );
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
            catch ( InvalidRecordException e )
            {   // No need to apply changes, doesn't exist
            }
        }

        private Collection<RelationshipRecord> loadRelationships( NodeRecord nodeRecord,
                LegacyRelationshipStoreReader relReader )
        {
            Collection<RelationshipRecord> records = new ArrayList<>();
            long rel = nodeRecord.getNextRel();
            long node = nodeRecord.getId();
            while ( rel != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                RelationshipRecord record = relReader.getRecord( rel );
                records.add( record );
                rel = record.getFirstNode() == node ?
                        record.getFirstNextRel() : record.getSecondNextRel();
            }
            return records;
        }

        private Map<Integer, Relationships> splitUp( long nodeId, Collection<RelationshipRecord> records )
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

        private void reportProgress( long id )
        {
            int newPercent = totalEntities == 0 ? 100 : (int) ((id+1) * 100 / totalEntities);
            if ( newPercent > percentComplete )
            {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }
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
