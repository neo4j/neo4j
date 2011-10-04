/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

public class StoreMigrator
{
    private LegacyStore legacyStore;

    public StoreMigrator( LegacyStore legacyStore )
    {
        this.legacyStore = legacyStore;
    }

    public void migrateTo( NeoStore neoStore ) throws IOException
    {
        migrateNodes( neoStore.getNodeStore(), new PropertyWriter( neoStore.getPropertyStore() ) );
        migrateRelationships( neoStore.getRelationshipStore(), new PropertyWriter( neoStore.getPropertyStore() ) );
        migratePropertyIndexes( neoStore.getPropertyStore().getIndexStore() );
        legacyStore.getPropertyStoreReader().close();
        migrateRelationshipTypes( neoStore.getRelationshipTypeStore() );
//        migrateIdGenerators( neoStore );
        legacyStore.getDynamicRecordFetcher().close();
    }

    private void migrateNodes( NodeStore nodeStore, PropertyWriter propertyWriter ) throws IOException
    {
        Iterable<NodeRecord> records = legacyStore.getNodeStoreReader().readNodeStore();
        for ( NodeRecord nodeRecord : records )
        {
            long startOfPropertyChain = nodeRecord.getNextProp();
            if ( startOfPropertyChain != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                long propertyRecordId = migrateProperties( startOfPropertyChain, propertyWriter );
                nodeRecord.setNextProp( propertyRecordId );
            }
            nodeStore.setHighId( nodeRecord.getId() + 1 );
            nodeStore.updateRecord( nodeRecord );
        }
        legacyStore.getNodeStoreReader().close();
    }

    private void migrateRelationships( RelationshipStore relationshipStore, PropertyWriter propertyWriter ) throws IOException
    {
        Iterable<RelationshipRecord> records = legacyStore.getRelationshipStoreReader().readRelationshipStore();
        for ( RelationshipRecord relationshipRecord : records )
        {
            long startOfPropertyChain = relationshipRecord.getNextProp();
            if ( startOfPropertyChain != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                long propertyRecordId = migrateProperties( startOfPropertyChain, propertyWriter );
                relationshipRecord.setNextProp( propertyRecordId );
            }
            relationshipStore.setHighId( relationshipRecord.getId() + 1 );
            relationshipStore.updateRecord( relationshipRecord );
        }
        legacyStore.getRelationshipStoreReader().close();
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
            List<LegacyDynamicRecord> dynamicRecords = relationshipTypeNameStoreReader.getPropertyChain( relationshipTypeRecord.getTypeBlock() );
            String name = LegacyDynamicRecordFetcher.joinRecordsIntoString( relationshipTypeRecord.getTypeBlock(), dynamicRecords );
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
        int keyBlockId = (int) relationshipTypeStore.nextBlockId();
        record.setTypeBlock( keyBlockId );
        Collection<DynamicRecord> keyRecords = relationshipTypeStore.allocateTypeNameRecords(
                keyBlockId, encodeString( name ) );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addTypeRecord( keyRecord );
        }
        relationshipTypeStore.updateRecord( record );
    }

    public void migratePropertyIndexes( PropertyIndexStore propIndexStore ) throws IOException
    {
        LegacyPropertyIndexStoreReader indexStoreReader = legacyStore.getPropertyIndexStoreReader();
        LegacyDynamicStoreReader propertyIndexKeyStoreReader = legacyStore.getPropertyIndexKeyStoreReader();

        for ( PropertyIndexRecord propertyIndexRecord : indexStoreReader.readPropertyIndexStore() )
        {
            List<LegacyDynamicRecord> dynamicRecords = propertyIndexKeyStoreReader.getPropertyChain( propertyIndexRecord.getKeyBlockId() );
            String key = LegacyDynamicRecordFetcher.joinRecordsIntoString( propertyIndexRecord.getKeyBlockId(), dynamicRecords );
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
        int keyBlockId = propIndexStore.nextKeyBlockId();
        record.setKeyBlockId( keyBlockId );
        Collection<DynamicRecord> keyRecords = propIndexStore.allocateKeyRecords(
                keyBlockId, encodeString( key ) );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addKeyRecord( keyRecord );
        }
        propIndexStore.updateRecord( record );
    }

//    private void migrateIdGenerators( NeoStore neoStore ) throws IOException
//    {
//        String[] idGeneratorSuffixes = new String[]{".nodestore.db.id", ".relationshipstore.db.id"};
//        for ( String suffix : idGeneratorSuffixes )
//        {
//            FileUtils.copyFile( new File( legacyStore.getStorageFileName() + suffix ),
//                    new File( neoStore.getStorageFileName() + suffix ) );
//        }
//    }
}
