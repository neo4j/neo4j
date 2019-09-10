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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class StoreScanChunkIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void differentChunksHaveDifferentCursors()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            RecordStorageEngine recordStorageEngine = database.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            NeoStores neoStores = recordStorageEngine.testAccessNeoStores();
            RecordStorageReader storageReader = new RecordStorageReader( neoStores );
            TestStoreScanChunk scanChunk1 = new TestStoreScanChunk( storageReader, false );
            TestStoreScanChunk scanChunk2 = new TestStoreScanChunk( storageReader, false );
            assertNotSame( scanChunk1.getCursor(), scanChunk2.getCursor() );
            assertNotSame( scanChunk1.getStorePropertyCursor(), scanChunk2.getStorePropertyCursor() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private class TestStoreScanChunk extends StoreScanChunk<StorageNodeCursor>
    {
        TestStoreScanChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration )
        {
            super( storageReader.allocateNodeCursor(), storageReader, requiresPropertyMigration );
        }

        @Override
        protected void read( StorageNodeCursor cursor, long id )
        {
            cursor.single( id );
        }

        @Override
        void visitRecord( StorageNodeCursor record, InputEntityVisitor visitor )
        {
            // empty
        }

        StorageNodeCursor getCursor()
        {
            return cursor;
        }

        StoragePropertyCursor getStorePropertyCursor()
        {
            return storePropertyCursor;
        }
    }
}
