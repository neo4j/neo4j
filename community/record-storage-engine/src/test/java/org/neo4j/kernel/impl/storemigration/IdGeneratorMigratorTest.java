/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@PageCacheExtension
class IdGeneratorMigratorTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    @Test
    void shouldFindAndStoreDeletedIds() throws IOException
    {
        // given
        RecordDatabaseLayout db = RecordDatabaseLayout.ofFlat( directory.directory( "from" ) );
        RecordDatabaseLayout upgrade = RecordDatabaseLayout.ofFlat( directory.directory( "to" ) );
        long nodeStoreStartId;
        long stringStoreStartId;
        long relationshipStoreStartId;
        try ( NeoStores neoStores = new StoreFactory( db, defaults(), new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME ), pageCache, fs,
                StandardV3_4.RECORD_FORMATS, nullLogProvider(), PageCacheTracer.NULL, writable(), immutable.empty() ).openAllNeoStores( true ) )
        {
            // Let nodes have every fourth a deleted record
            createSomeRecordsAndSomeHoles( neoStores.getNodeStore(), 500, 1, 3 );
            createSomeRecordsAndSomeHoles( neoStores.getPropertyStore().getStringStore(), 100, 1, 2 );
            nodeStoreStartId = neoStores.getNodeStore().getNumberOfReservedLowIds();
            stringStoreStartId = neoStores.getPropertyStore().getStringStore().getNumberOfReservedLowIds();
        }
        // Pretend that the relationship store was copied so that relationship id file should be migrated from there
        try ( NeoStores neoStores = new StoreFactory( upgrade, defaults(), new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME ), pageCache,
                fs, Standard.LATEST_RECORD_FORMATS, nullLogProvider(), PageCacheTracer.NULL, writable(), immutable.empty() ).openAllNeoStores( true ) )
        {
            // Let relationships have every fourth a created record
            createSomeRecordsAndSomeHoles( neoStores.getRelationshipStore(), 600, 3, 1 );
            relationshipStoreStartId = neoStores.getRelationshipStore().getNumberOfReservedLowIds();
        }
        fs.deleteFile( upgrade.nodeStore() );
        fs.deleteFile( upgrade.idNodeStore() );
        fs.deleteFile( upgrade.propertyStringStore() );
        fs.deleteFile( upgrade.idPropertyStringStore() );
        fs.deleteFile( db.relationshipStore() );
        fs.deleteFile( upgrade.idRelationshipStore() );

        // when
        IdGeneratorMigrator migrator = new IdGeneratorMigrator( fs, pageCache, defaults(), PageCacheTracer.NULL );
        migrator.migrate( db, upgrade, ProgressReporter.SILENT, StandardV3_4.STORE_VERSION, Standard.LATEST_STORE_VERSION, IndexImporterFactory.EMPTY );
        migrator.moveMigratedFiles( upgrade, db, StandardV3_4.STORE_VERSION, Standard.LATEST_STORE_VERSION );

        // then
        assertIdGeneratorContainsIds( db.idNodeStore(), RecordIdType.NODE, 500, 1, 3, nodeStoreStartId );
        assertIdGeneratorContainsIds( db.idPropertyStringStore(), RecordIdType.STRING_BLOCK, 100, 1, 2, stringStoreStartId );
        assertIdGeneratorContainsIds( db.idRelationshipStore(), RecordIdType.RELATIONSHIP, 600, 3, 1, relationshipStoreStartId );
    }

    private void assertIdGeneratorContainsIds( Path idFilePath, RecordIdType idType, int rounds, int numDeleted, int numCreated, long startingId )
            throws IOException
    {
        try ( IdGenerator idGenerator = new IndexedIdGenerator( pageCache, idFilePath, immediate(), idType, false, () -> -1, Long.MAX_VALUE, writable(),
                Config.defaults(), DEFAULT_DATABASE_NAME, CursorContext.NULL, IndexedIdGenerator.NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            idGenerator.start( ignored ->
            {
                throw new RuntimeException( "Should not ask for free ids" );
            }, CursorContext.NULL );
            long nextExpectedId = startingId;
            for ( int i = 0; i < rounds; i++ )
            {
                for ( int d = 0; d < numDeleted; d++ )
                {
                    idGenerator.maintenance( true, CursorContext.NULL );
                    assertEquals( nextExpectedId++, idGenerator.nextId( CursorContext.NULL ) );
                }
                nextExpectedId += numCreated;
            }
        }
    }

    private static <R extends AbstractBaseRecord> void createSomeRecordsAndSomeHoles( RecordStore<R> store, int rounds, int numDeleted, int numCreated )
    {
        R record = store.newRecord();
        record.setInUse( true );
        long id = store.getNumberOfReservedLowIds();
        try ( var cursor = store.openPageCursorForWriting( 0, CursorContext.NULL ) )
        {
            for ( int i = 0; i < rounds; i++ )
            {
                id += numDeleted;
                for ( int c = 0; c < numCreated; c++ )
                {
                    record.setId( id++ );
                    // we don't look at these id generators anyway during migration
                    store.updateRecord( record, IdUpdateListener.IGNORE, cursor, CursorContext.NULL, StoreCursors.NULL );
                }
            }
        }
    }
}
