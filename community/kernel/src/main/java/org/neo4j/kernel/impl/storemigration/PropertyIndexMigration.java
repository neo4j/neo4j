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
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;

public class PropertyIndexMigration
{
    private LegacyStore legacyStore;

    public PropertyIndexMigration( LegacyStore legacyStore )
    {
        this.legacyStore = legacyStore;
    }

    public void migratePropertyIndexes( NeoStore neoStore ) throws IOException
    {
        LegacyPropertyIndexStoreReader indexStoreReader = legacyStore.getPropertyIndexStoreReader();
        LegacyDynamicStoreReader propertyIndexKeyStoreReader = legacyStore.getPropertyIndexKeyStoreReader();

        PropertyIndexStore indexStore = neoStore.getPropertyStore().getIndexStore();

        for ( PropertyIndexRecord propertyIndexRecord : indexStoreReader.readPropertyIndexStore() )
        {
            List<LegacyDynamicRecord> dynamicRecords = propertyIndexKeyStoreReader.getPropertyChain( propertyIndexRecord.getKeyBlockId() );
            String key = LegacyDynamicRecordFetcher.joinRecordsIntoString( propertyIndexRecord.getKeyBlockId(), dynamicRecords );
            createPropertyIndex( indexStore, key, propertyIndexRecord.getId() );
        }
    }

    public void createPropertyIndex( PropertyIndexStore propIndexStore, String key, int id )
    {
        long nextIdFromStore = propIndexStore.nextId();
        if (nextIdFromStore != id) {
            throw new IllegalStateException( String.format( "Expected next id from store %d to match legacy id %d", nextIdFromStore, id ) );
        }

        PropertyIndexRecord record = new PropertyIndexRecord( id );

        record.setInUse( true );
        record.setCreated();
        int keyBlockId = propIndexStore.nextKeyBlockId();
        record.setKeyBlockId( keyBlockId );
        Collection<DynamicRecord> keyRecords =
            propIndexStore.allocateKeyRecords( keyBlockId, getBestSuitedEncoding( key ) );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addKeyRecord( keyRecord );
        }
        propIndexStore.updateRecord( record );
    }

}
