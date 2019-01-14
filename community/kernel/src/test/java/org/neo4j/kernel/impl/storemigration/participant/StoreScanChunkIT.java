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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.kernel.impl.api.store.StorePropertyCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import static org.junit.Assert.assertNotSame;

public class StoreScanChunkIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void differentChunksHaveDifferentCursors()
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
        try
        {
            RecordStorageEngine recordStorageEngine = database.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            NeoStores neoStores = recordStorageEngine.testAccessNeoStores();
            RecordCursor dummyCursor = Mockito.mock( RecordCursor.class );
            TestStoreScanChunk scanChunk1 = new TestStoreScanChunk( dummyCursor, neoStores, false );
            TestStoreScanChunk scanChunk2 = new TestStoreScanChunk( dummyCursor, neoStores, false );
            assertNotSame( scanChunk1.getRecordCursors(), scanChunk2.getRecordCursors() );
            assertNotSame( scanChunk1.getStorePropertyCursor(), scanChunk2.getStorePropertyCursor() );
        }
        finally
        {
            database.shutdown();
        }
    }

    private class TestStoreScanChunk extends StoreScanChunk<NodeRecord>
    {
        TestStoreScanChunk( RecordCursor<NodeRecord> cursor, NeoStores neoStores, boolean requiresPropertyMigration )
        {
            super( cursor, neoStores, requiresPropertyMigration );
        }

        @Override
        void visitRecord( NodeRecord record, InputEntityVisitor visitor )
        {
            // empty
        }

        RecordCursors getRecordCursors()
        {
            return recordCursors;
        }

        StorePropertyCursor getStorePropertyCursor()
        {
            return storePropertyCursor;
        }
    }
}
