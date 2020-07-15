/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
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
        DatabaseLayout db = DatabaseLayout.ofFlat( directory.directory( "from" ) );
        DatabaseLayout upgrade = DatabaseLayout.ofFlat( directory.directory( "to" ) );
        long nodeStoreStartId;
        long stringStoreStartId;
        long relationshipStoreStartId;
        try ( NeoStores neoStores = new StoreFactory( db, defaults(), new DefaultIdGeneratorFactory( fs, immediate() ), pageCache, fs,
                StandardV3_4.RECORD_FORMATS, nullLogProvider(), PageCacheTracer.NULL, immutable.empty() ).openAllNeoStores( true ) )
        {
            // Let nodes have every fourth a deleted record
            createSomeRecordsAndSomeHoles( neoStores.getNodeStore(), 500, 1, 3 );
            createSomeRecordsAndSomeHoles( neoStores.getPropertyStore().getStringStore(), 100, 1, 2 );
            nodeStoreStartId = neoStores.getNodeStore().getNumberOfReservedLowIds();
            stringStoreStartId = neoStores.getPropertyStore().getStringStore().getNumberOfReservedLowIds();
        }
        // Pretend that the relationship store was migrated so that relationship id file should be migrated from there
        try ( NeoStores neoStores = new StoreFactory( upgrade, defaults(), new DefaultIdGeneratorFactory( fs, immediate() ), pageCache, fs,
                StandardV4_0.RECORD_FORMATS, nullLogProvider(), PageCacheTracer.NULL, immutable.empty() ).openAllNeoStores( true ) )
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
        fs.deleteFile( db.idRelationshipStore() );

        // when
        IdGeneratorMigrator migrator = new IdGeneratorMigrator( fs, pageCache, defaults(), PageCacheTracer.NULL );
        migrator.migrate( db, upgrade, ProgressReporter.SILENT, StandardV3_4.STORE_VERSION, Standard.LATEST_STORE_VERSION );
        migrator.moveMigratedFiles( upgrade, db, StandardV3_4.STORE_VERSION, Standard.LATEST_STORE_VERSION );

        // then
        assertIdGeneratorContainsIds( db.idNodeStore(), IdType.NODE, 500, 1, 3, nodeStoreStartId );
        assertIdGeneratorContainsIds( db.idPropertyStringStore(), IdType.STRING_BLOCK, 100, 1, 2, stringStoreStartId );
        assertIdGeneratorContainsIds( db.idRelationshipStore(), IdType.RELATIONSHIP, 600, 3, 1, relationshipStoreStartId );
    }

    private void assertIdGeneratorContainsIds( File idFile, IdType idType, int rounds, int numDeleted, int numCreated, long startingId ) throws IOException
    {
        try ( IdGenerator idGenerator = new IndexedIdGenerator( pageCache, idFile, immediate(), idType, false, () -> -1, Long.MAX_VALUE, false,
                PageCursorTracer.NULL ) )
        {
            idGenerator.start( ignored ->
            {
                throw new RuntimeException( "Should not ask for free ids" );
            }, PageCursorTracer.NULL );
            long nextExpectedId = startingId;
            for ( int i = 0; i < rounds; i++ )
            {
                for ( int d = 0; d < numDeleted; d++ )
                {
                    assertEquals( nextExpectedId++, idGenerator.nextId( PageCursorTracer.NULL ) );
                }
                nextExpectedId += numCreated;
            }
        }
    }

    private <R extends AbstractBaseRecord> void createSomeRecordsAndSomeHoles( RecordStore<R> store, int rounds, int numDeleted, int numCreated )
    {
        R record = store.newRecord();
        record.setInUse( true );
        long id = store.getNumberOfReservedLowIds();
        for ( int i = 0; i < rounds; i++ )
        {
            id += numDeleted;
            for ( int c = 0; c < numCreated; c++ )
            {
                record.setId( id++ );
                // we don't look at these id generators anyway during migration
                store.updateRecord( record, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
            }
        }
    }
}
