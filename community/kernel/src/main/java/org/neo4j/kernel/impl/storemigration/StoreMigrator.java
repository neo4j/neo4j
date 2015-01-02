/**
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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.helpers.UTF8.encode;
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
            migrateNeoStore( neoStore );
            migrateNodes( neoStore.getNodeStore() );
            migratePropertyIndexes( neoStore.getPropertyStore() );
            migrateLastTransactionLog();
            migrateLastLuceneLog();

            // Close
            neoStore.close();
            legacyStore.close();

            // Just copy unchanged stores that doesn't need migration
            legacyStore.copyRelationshipStore( neoStore );
            legacyStore.copyRelationshipTypeTokenStore( neoStore );
            legacyStore.copyRelationshipTypeTokenNameStore( neoStore );
            legacyStore.copyDynamicStringPropertyStore( neoStore );
            legacyStore.copyDynamicArrayPropertyStore( neoStore );
            legacyStore.copyLegacyIndexStoreFile( neoStore.getStorageFileName().getParentFile() );
        }

        private void migratePropertyIndexes( PropertyStore propertyStore ) throws IOException
        {
            Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();

            // dedup and write new property key token store (incl. names)
            Map<Integer, Integer> propertyKeyTranslation =
                    dedupAndWritePropertyKeyTokenStore( propertyStore, tokens );

            // read property store, replace property key ids
            migratePropertyStore( propertyKeyTranslation, propertyStore );
        }

        private void migrateLastTransactionLog() throws IOException
        {
            StoreChannel newLogChannel = legacyStore.beginTranslatingLastTransactionLog( neoStore );
            if ( newLogChannel == null )
            {
                // There are no transaction logs for us to translate, so we skip this step.
                return;
            }

            LogBuffer logBuffer = new DirectMappedLogBuffer( newLogChannel, ByteCounterMonitor.NULL );

            for ( LogEntry entry : loop( legacyStore.iterateLastTransactionLogEntries( logBuffer ) ) )
            {
                LogIoUtils.writeLogEntry( entry, logBuffer );
            }
            logBuffer.force();
            newLogChannel.close();
        }

        private void migrateLastLuceneLog() throws IOException
        {
            StoreChannel newLogChannel = legacyStore.beginTranslatingLastLuceneLog( neoStore );
            if ( newLogChannel == null )
            {
                // There are no lucene legacy index transactions for us to translate, so we skip this step.
                return;
            }

            try
            {
                LogBuffer logBuffer = new DirectMappedLogBuffer( newLogChannel, ByteCounterMonitor.NULL );

                for ( LogEntry entry : loop( legacyStore.iterateLastLuceneLogEntries( logBuffer ) ) )
                {
                    LogIoUtils.writeLogEntry( entry, logBuffer );
                }
                logBuffer.force();
            }
            finally
            {
                newLogChannel.close();
            }
        }

        private void migrateNeoStore( NeoStore neoStore ) throws IOException
        {
            legacyStore.copyNeoStore( neoStore );
            neoStore.setStoreVersion( NeoStore.versionStringToLong( CommonAbstractStore.ALL_STORES_VERSION ) );
        }

        private Map<Integer, Integer> dedupAndWritePropertyKeyTokenStore( PropertyStore propertyStore,
                Token[] tokens /*ordered ASC*/ )
        {
            PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
            Map<Integer/*duplicate*/, Integer/*use this instead*/> translations = new HashMap<Integer, Integer>();
            Map<String, Integer> createdTokens = new HashMap<String, Integer>();
            for ( Token token : tokens )
            {
                Integer id = createdTokens.get( token.name() );
                if ( id == null )
                {   // Not a duplicate, add to store
                    id = (int) keyTokenStore.nextId();
                    PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
                    Collection<DynamicRecord> nameRecords =
                            keyTokenStore.allocateNameRecords( encode( token.name() ) );
                    record.setNameId( (int) first( nameRecords ).getId() );
                    record.addNameRecords( nameRecords );
                    record.setInUse( true );
                    record.setCreated();
                    keyTokenStore.updateRecord( record );
                    createdTokens.put( token.name(), id );
                }
                translations.put( token.id(), id );
            }
            return translations;
        }

        private void migratePropertyStore( Map<Integer, Integer> propertyKeyTranslation,
                PropertyStore propertyStore ) throws IOException
        {
            long lastInUseId = -1;
            for ( PropertyRecord propertyRecord : loop( legacyStore.getPropertyStoreReader().readPropertyStore() ) )
            {
                // Translate property keys
                for ( PropertyBlock block : propertyRecord.getPropertyBlocks() )
                {
                    int key = block.getKeyIndexId();
                    Integer translation = propertyKeyTranslation.get( key );
                    if ( translation != null )
                    {
                        block.setKeyIndexId( translation );
                    }
                }
                propertyStore.setHighId( propertyRecord.getId()+1 );
                propertyStore.updateRecord( propertyRecord );
                for ( long id = lastInUseId+1; id < propertyRecord.getId(); id++ )
                {
                    propertyStore.freeId( id );
                }
                lastInUseId = propertyRecord.getId();
            }
        }

        private void migrateNodes( NodeStore nodeStore ) throws IOException
        {
            for ( NodeRecord nodeRecord : loop( legacyStore.getNodeStoreReader().readNodeStore() ) )
            {
                reportProgress( nodeRecord.getId() );
                nodeStore.setHighId( nodeRecord.getId() + 1 );
                if ( nodeRecord.inUse() )
                {
                    nodeStore.updateRecord( nodeRecord );
                }
                else
                {
                    nodeStore.freeId( nodeRecord.getId() );
                }
            }
            legacyStore.getNodeStoreReader().close();
        }

        private void reportProgress( long id )
        {
            int newPercent = (int) (id * 100 / totalEntities);
            if ( newPercent > percentComplete )
            {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }
    }
}
