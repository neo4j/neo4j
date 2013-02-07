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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyDynamicRecord;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyDynamicRecordFetcher;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyDynamicStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNeoStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyIndexStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyRecord;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipTypeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

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
        private int percentComplete = 0;

        public Migration( LegacyStore legacyStore, NeoStore neoStore )
        {
            this.legacyStore = legacyStore;
            this.neoStore = neoStore;
            totalEntities = legacyStore.getNodeStoreReader().getMaxId() + legacyStore.getRelationshipStoreReader().getMaxId();
        }

        private void migrate() throws IOException
        {
            migrateNeoStore( neoStore );
            migrateNodes( neoStore.getNodeStore(), new PropertyWriter( neoStore.getPropertyStore() ) );
            migrateRelationships( neoStore.getRelationshipStore(), new PropertyWriter( neoStore.getPropertyStore() ) );
            migratePropertyIndexes( neoStore.getPropertyStore().getIndexStore() );
            legacyStore.getPropertyStoreReader().close();
            migrateRelationshipTypes( neoStore.getRelationshipTypeStore() );
            legacyStore.close();
        }

        private void migrateNeoStore( NeoStore neoStore )
        {
            LegacyNeoStoreReader neoStoreReader = legacyStore.getNeoStoreReader();

            neoStore.setCreationTime( neoStoreReader.getCreationTime() );
            neoStore.setRandomNumber( neoStoreReader.getRandomNumber() );
            neoStore.setVersion( neoStoreReader.getVersion() );
            updateLastCommittedTxInSimulatedRecoveredStatus( neoStore, neoStoreReader.getLastCommittedTx() );
        }

        private void updateLastCommittedTxInSimulatedRecoveredStatus( NeoStore neoStore, long lastCommittedTx )
        {
            neoStore.setRecoveredStatus( true );
            neoStore.setLastCommittedTx( lastCommittedTx );
            neoStore.setRecoveredStatus( false );
        }

        private void migrateNodes( NodeStore nodeStore, PropertyWriter propertyWriter ) throws IOException
        {
            Iterable<NodeRecord> records = legacyStore.getNodeStoreReader().readNodeStore();
            // estimate total number of nodes using file size then calc number of dots or percentage complete
            for ( NodeRecord nodeRecord : records )
            {
                reportProgress(nodeRecord.getId());
                nodeStore.setHighId( nodeRecord.getId() + 1 );
                if ( nodeRecord.inUse() )
                {
                    long startOfPropertyChain = nodeRecord.getNextProp();
                    if ( startOfPropertyChain != Record.NO_NEXT_RELATIONSHIP.intValue() )
                    {
                        long propertyRecordId = migrateProperties( startOfPropertyChain, propertyWriter );
                        nodeRecord.setNextProp( propertyRecordId );
                    }
                    nodeStore.updateRecord( nodeRecord );
                } else
                {
                    nodeStore.freeId( nodeRecord.getId() );
                }
            }
            legacyStore.getNodeStoreReader().close();
        }

        private void migrateRelationships( RelationshipStore relationshipStore, PropertyWriter propertyWriter ) throws IOException
        {
            long nodeMaxId = legacyStore.getNodeStoreReader().getMaxId();

            Iterable<RelationshipRecord> records = legacyStore.getRelationshipStoreReader().readRelationshipStore();
            for ( RelationshipRecord relationshipRecord : records )
            {
                reportProgress( nodeMaxId + relationshipRecord.getId() );
                relationshipStore.setHighId( relationshipRecord.getId() + 1 );
                if ( relationshipRecord.inUse() )
                {
                    long startOfPropertyChain = relationshipRecord.getNextProp();
                    if ( startOfPropertyChain != Record.NO_NEXT_RELATIONSHIP.intValue() )
                    {
                        long propertyRecordId = migrateProperties( startOfPropertyChain, propertyWriter );
                        relationshipRecord.setNextProp( propertyRecordId );
                    }
                    relationshipStore.updateRecord( relationshipRecord );
                } else
                {
                    relationshipStore.freeId( relationshipRecord.getId() );
                }
            }
            legacyStore.getRelationshipStoreReader().close();
        }

        private void reportProgress( long id )
        {
            int newPercent = (int) (id * 100 / totalEntities);
            if ( newPercent > percentComplete ) {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }

        private long migrateProperties( long startOfPropertyChain, PropertyWriter propertyWriter ) throws IOException
        {
            LegacyPropertyRecord propertyRecord = legacyStore.getPropertyStoreReader().readPropertyRecord( startOfPropertyChain );
            List<Pair<Integer, Object>> properties = new ArrayList<Pair<Integer, Object>>();
            while ( propertyRecord.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                properties.add( extractValue( propertyRecord ) );
                propertyRecord = legacyStore.getPropertyStoreReader().readPropertyRecord( propertyRecord.getNextProp() );
            }
            properties.add( extractValue( propertyRecord ) );
            return propertyWriter.writeProperties( properties );
        }

        private Pair<Integer, Object> extractValue( LegacyPropertyRecord propertyRecord )
        {
            int keyIndexId = propertyRecord.getKeyIndexId();
            Object value = propertyRecord.getType().getValue( propertyRecord, legacyStore.getDynamicRecordFetcher() );
            return Pair.of( keyIndexId, value );
        }

        public void migrateRelationshipTypes( RelationshipTypeStore relationshipTypeStore ) throws IOException
        {
            LegacyRelationshipTypeStoreReader relationshipTypeStoreReader = legacyStore.getRelationshipTypeStoreReader();
            LegacyDynamicStoreReader relationshipTypeNameStoreReader = legacyStore.getRelationshipTypeNameStoreReader();

            for ( RelationshipTypeRecord relationshipTypeRecord : relationshipTypeStoreReader.readRelationshipTypes() )
            {
                List<LegacyDynamicRecord> dynamicRecords = relationshipTypeNameStoreReader.getPropertyChain( relationshipTypeRecord.getNameId() );
                String name = LegacyDynamicRecordFetcher.joinRecordsIntoString( relationshipTypeRecord.getNameId(), dynamicRecords );
                createRelationshipType( relationshipTypeStore, name, relationshipTypeRecord.getId() );
            }
            relationshipTypeNameStoreReader.close();
        }

        public void createRelationshipType( RelationshipTypeStore relationshipTypeStore, String name, int id )
        {
            long nextIdFromStore = relationshipTypeStore.nextId();
            while ( nextIdFromStore < id )
            {
                nextIdFromStore = relationshipTypeStore.nextId();
            }

            RelationshipTypeRecord record = new RelationshipTypeRecord( id );

            record.setInUse( true );
            record.setCreated();
            Collection<DynamicRecord> keyRecords = relationshipTypeStore.allocateNameRecords(
                    encodeString( name ) );
            record.setNameId( (int) first( keyRecords ).getId() );
            for ( DynamicRecord keyRecord : keyRecords )
            {
                record.addNameRecord( keyRecord );
            }
            relationshipTypeStore.updateRecord( record );
        }

        public void migratePropertyIndexes( PropertyIndexStore propIndexStore ) throws IOException
        {
            LegacyPropertyIndexStoreReader indexStoreReader = legacyStore.getPropertyIndexStoreReader();
            LegacyDynamicStoreReader propertyIndexKeyStoreReader = legacyStore.getPropertyIndexKeyStoreReader();

            for ( PropertyIndexRecord propertyIndexRecord : indexStoreReader.readPropertyIndexStore() )
            {
                List<LegacyDynamicRecord> dynamicRecords = propertyIndexKeyStoreReader.getPropertyChain( propertyIndexRecord.getNameId() );
                String key = LegacyDynamicRecordFetcher.joinRecordsIntoString( propertyIndexRecord.getNameId(), dynamicRecords );
                createPropertyIndex( propIndexStore, key, propertyIndexRecord.getId() );
            }
            propertyIndexKeyStoreReader.close();
        }

        public void createPropertyIndex( PropertyIndexStore propIndexStore, String key, int id )
        {
            long nextIdFromStore = propIndexStore.nextId();
            while ( nextIdFromStore < id )
            {
                nextIdFromStore = propIndexStore.nextId();
            }

            PropertyIndexRecord record = new PropertyIndexRecord( id );

            record.setInUse( true );
            record.setCreated();
            Collection<DynamicRecord> keyRecords = propIndexStore.allocateNameRecords(
                    encodeString( key ) );
            record.setNameId( (int) first( keyRecords ).getId() );
            for ( DynamicRecord keyRecord : keyRecords )
            {
                record.addNameRecord( keyRecord );
            }
            propIndexStore.updateRecord( record );
        }
    }
}
