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
import java.util.Optional;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogs;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

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

public class StoreMigratorTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory )
            .around( fileSystemRule )
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
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
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
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService, NO_INDEX_PROVIDER );
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
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );

        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );
        LegacyLogs legacyLogs = mock( LegacyLogs.class );
        when( legacyLogs.getTransactionInformation( storeDir, txId ) ).thenReturn( Optional.of( expected ) );

        // when
        // ... neoStore is empty and with migrator
        StoreMigrator migrator =
                new StoreMigrator( fileSystemRule.get(), pageCache, config, logService, schemaIndexProvider, legacyLogs );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldGenerateTransactionInformationWhenLogsNotPresent() throws Exception
    {
        // given
        long txId = 42;
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        LegacyLogs legacyLogs = mock( LegacyLogs.class );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and transaction not in log
        when( legacyLogs.getTransactionInformation( storeDir, txId ) ).thenReturn( Optional.empty() );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService,
                schemaIndexProvider );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
    }

    @Test
    public void shouldGenerateTransactionInformationWhenLogsAreEmpty() throws Exception
    {
        // given
        long txId = 1;
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        LegacyLogs legacyLogs = mock( LegacyLogs.class );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and transaction not in log
        when( legacyLogs.getTransactionInformation( storeDir, txId ) ).thenReturn( Optional.empty() );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService,
                schemaIndexProvider );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.BASE_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
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
        return new StoreMigrator( fileSystemRule.get(), pageCacheRule.getPageCache( fileSystemRule.get() ),
                Config.empty(), NullLogService.getInstance(), schemaIndexProvider );
    }
}
