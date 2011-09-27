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

import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.getBestSuitedEncoding;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;

public class RelationshipTypeMigration
{
    private LegacyStore legacyStore;

    public RelationshipTypeMigration( LegacyStore legacyStore )
    {
        this.legacyStore = legacyStore;
    }

    public void migrateRelationshipTypes( NeoStore neoStore ) throws IOException
    {
        LegacyRelationshipTypeStoreReader relationshipTypeStoreReader = legacyStore.getRelationshipTypeStoreReader();
        LegacyDynamicStoreReader relationshipTypeNameStoreReader = legacyStore.getRelationshipTypeNameStoreReader();

        RelationshipTypeStore relationshipTypeStore = neoStore.getRelationshipTypeStore();

        for ( RelationshipTypeRecord relationshipTypeRecord : relationshipTypeStoreReader.readRelationshipTypes() )
        {
            List<LegacyDynamicRecord> dynamicRecords = relationshipTypeNameStoreReader.getPropertyChain( relationshipTypeRecord.getTypeBlock() );
            String name = LegacyDynamicRecordFetcher.joinRecordsIntoString( relationshipTypeRecord.getTypeBlock(), dynamicRecords );
            createRelationshipType( relationshipTypeStore, name, relationshipTypeRecord.getId() );
        }
    }

    public void createRelationshipType( RelationshipTypeStore relationshipTypeStore, String name, int id )
    {
        long nextIdFromStore = relationshipTypeStore.nextId();
        if (nextIdFromStore != id) {
            throw new IllegalStateException( String.format( "Expected next id from store %d to match legacy id %d", nextIdFromStore, id ) );
        }

        RelationshipTypeRecord record = new RelationshipTypeRecord( id );

        record.setInUse( true );
        record.setCreated();
        int keyBlockId = (int) relationshipTypeStore.nextBlockId();
        record.setTypeBlock( keyBlockId );
        Collection<DynamicRecord> keyRecords =
            relationshipTypeStore.allocateTypeNameRecords( keyBlockId, getBestSuitedEncoding( name ) );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addTypeRecord( keyRecord );
        }
        relationshipTypeStore.updateRecord( record );
    }

}
