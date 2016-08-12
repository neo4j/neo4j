/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogs;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.NO_INDEX_PROVIDER;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.impl.store.MetaDataStore.setRecord;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;

public class StoreMigratorTest
{
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory )
            .around( pageCacheRule )
            .around( random );

    @Test
    public void shouldExtractTransactionInformationFromMetaDataStore() throws Exception
    {
        // given
        // ... variables
        long txId = 42;
        long checksum = 123456789123456789L;
        long timestamp = 919191919191919191L;
        TransactionId expected = new TransactionId( txId, checksum, timestamp );

        // ... and files
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );

        // when
        // ... data in record
        setRecord( pageCache, neoStore, LAST_TRANSACTION_ID, txId );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM, checksum );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP, timestamp );

        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, config, logService, NO_INDEX_PROVIDER );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldExtractTransactionInformationFromLegacyLogsWhenCantFindInStore() throws Exception
    {
        // given
        // ... variables
        long txId = 42;
        long checksum = 123456789123456789L;
        long timestamp = 919191919191919191L;
        TransactionId expected = new TransactionId( txId, checksum, timestamp );

        // ... and files
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );
        LegacyLogs legacyLogs = mock( LegacyLogs.class );
        when( legacyLogs.getTransactionInformation( storeDir, txId ) ).thenReturn( expected );

        // when
        // ... neoStore is empty and with migrator
        StoreMigrator migrator =
                new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider, legacyLogs );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldGenerateTransactionInformationAsLastOption() throws Exception
    {
        // given
        // ... variables
        long txId = 42;
        TransactionId expected = new TransactionId( txId, FIELD_NOT_PRESENT, UNKNOWN_TX_COMMIT_TIMESTAMP );

        // ... and files
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );
        LegacyLogs legacyLogs = mock( LegacyLogs.class );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and transaction not in log
        when( legacyLogs.getTransactionInformation( storeDir, txId ) ).thenThrow( NoSuchTransactionException.class );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( expected.transactionId(), actual.transactionId() );
        assertEquals( expected.commitTimestamp(), actual.commitTimestamp() );
        // We do not expect checksum to be equal as it is randomly generated
    }

    @Test
    public void writeAndReadLastTxInformation() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        TransactionId writtenTxId = new TransactionId( random.nextLong(), random.nextLong(), random.nextLong() );

        migrator.writeLastTxInformation( directory.graphDbDir(), writtenTxId );

        TransactionId readTxId = migrator.readLastTxInformation( directory.graphDbDir() );

        assertEquals( writtenTxId, readTxId );
    }

    @Test
    public void writeAndReadLastTxLogPosition() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        LogPosition writtenLogPosition = new LogPosition( random.nextLong(), random.nextLong() );

        migrator.writeLastTxLogPosition( directory.graphDbDir(), writtenLogPosition );

        LogPosition readLogPosition = migrator.readLastTxLogPosition( directory.graphDbDir() );

        assertEquals( writtenLogPosition, readLogPosition );
    }

    private StoreMigrator newStoreMigrator()
    {
        return new StoreMigrator( fs, pageCacheRule.getPageCache( fs ),
                Config.empty(), NullLogService.getInstance(), schemaIndexProvider );
    }
}
