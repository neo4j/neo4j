/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.metrics.MetricsExtension;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.Assert.assertEquals;

public class BatchingNeoStoresIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private FileSystemAbstraction fileSystem;
    private File storeDir;
    private AssertableLogProvider provider;
    private SimpleLogService logService;

    @Before
    public void setUp()
    {
        fileSystem = fileSystemRule.get();
        storeDir = testDirectory.graphDbDir();
        provider = new AssertableLogProvider();
        logService = new SimpleLogService( provider, provider );
    }

    @Test
    public void startBatchingNeoStoreWithMetricsPluginEnabled() throws Exception
    {
        Config config = Config.defaults( MetricsSettings.metricsEnabled, "true"  );
        try ( BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, storeDir, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, AdditionalInitialIds.EMPTY, config ) )
        {
            batchingNeoStores.createNew();
        }
        provider.assertNone( AssertableLogProvider.inLog( MetricsExtension.class ).any() );
    }

    @Test
    public void createStoreWithNotEmptyInitialIds() throws IOException
    {
        try ( BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, storeDir, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, new TestAdditionalInitialIds(), Config.defaults() ) )
        {
            batchingNeoStores.createNew();
        }

        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            TransactionIdStore transactionIdStore = getTransactionIdStore( (GraphDatabaseAPI) database );
            assertEquals( 10, transactionIdStore.getLastCommittedTransactionId() );
        }
        finally
        {
            database.shutdown();
        }
    }

    private static TransactionIdStore getTransactionIdStore( GraphDatabaseAPI database )
    {
        DependencyResolver resolver = database.getDependencyResolver();
        return resolver.resolveDependency( TransactionIdStore.class );
    }

    private static class TestAdditionalInitialIds implements AdditionalInitialIds
    {
        @Override
        public long lastCommittedTransactionId()
        {
            return 10;
        }

        @Override
        public long lastCommittedTransactionChecksum()
        {
            return 11;
        }

        @Override
        public long lastCommittedTransactionLogVersion()
        {
            return 12;
        }

        @Override
        public long lastCommittedTransactionLogByteOffset()
        {
            return 13;
        }
    }
}
