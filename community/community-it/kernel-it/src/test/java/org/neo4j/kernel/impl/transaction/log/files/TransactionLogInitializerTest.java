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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.jupiter.api.Test;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory.INSTANCE;
import static org.neo4j.io.pagecache.tracing.cursor.CursorContext.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionStringToLong;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_STORE_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

@TestDirectoryExtension
class TransactionLogInitializerTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldResetMetaDataStoreWithTransactionId() throws Exception
    {
        //Given
        var metaStore = mock( MetadataProvider.class );
        var txn = new TransactionId( 3, -1322858814, currentTimeMillis() );
        when( metaStore.getStoreId() ).thenReturn( new StoreId( versionStringToLong( LATEST_STORE_VERSION ) ) );
        when( metaStore.getLastClosedTransaction() ).thenReturn( new long[]{txn.transactionId(), 0, 1613} );
        when( metaStore.getLastCommittedTransaction() ).thenReturn( txn );
        when( metaStore.getLastCommittedTransactionId() ).thenReturn( txn.transactionId() );
        when( metaStore.getLastClosedTransactionId() ).thenReturn( txn.transactionId() );
        DatabaseLayout databaseLayout = Neo4jLayout.of( testDirectory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );

        //When
        var initializer = new TransactionLogInitializer( testDirectory.getFileSystem(), metaStore, INSTANCE, PageCacheTracer.NULL );
        initializer.initializeEmptyLogFile( databaseLayout, databaseLayout.getTransactionLogsDirectory(), "LostFiles" );

        //Then
        verify( metaStore ).resetLastClosedTransaction( txn.transactionId(), txn.transactionId(), CURRENT_FORMAT_LOG_HEADER_SIZE, true, NULL );
    }
}
