/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( TestDirectoryExtension.class )
class OnlineBackupExtensionFactoryTest
{
    @Inject
    private TestDirectory testDirectory;
    private OnlineBackupExtensionFactory extensionFactory = new OnlineBackupExtensionFactory();
    private Dependencies dependencies = new Dependencies();

    @Test
    void doNotCreateExtensionForNonDefaultDatabase()
    {
        SimpleKernelContext kernelContext = new SimpleKernelContext( testDirectory.databaseDir(), DatabaseInfo.ENTERPRISE, dependencies );
        Lifecycle instance = extensionFactory.newInstance( kernelContext, new TestBackDependencies( "another" ) );
        assertThat( instance, not( instanceOf( OnlineBackupKernelExtension.class ) ) );
    }

    @Test
    void createExtensionForDefaultDatabase()
    {
        SimpleKernelContext kernelContext = new SimpleKernelContext( testDirectory.databaseDir(), DatabaseInfo.ENTERPRISE, dependencies );
        Lifecycle instance = extensionFactory.newInstance( kernelContext, new TestBackDependencies( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) );
        assertThat( instance, instanceOf( OnlineBackupKernelExtension.class ) );
    }

    private static class TestBackDependencies implements OnlineBackupExtensionFactory.Dependencies
    {
        private final String databaseName;

        TestBackDependencies( String databaseName )
        {
            this.databaseName = databaseName;
        }

        @Override
        public Config getConfig()
        {
            return Config.defaults();
        }

        @Override
        public GraphDatabaseAPI getGraphDatabaseAPI()
        {
            return null;
        }

        @Override
        public LogService logService()
        {
            return new SimpleLogService( NullLogProvider.getInstance() );
        }

        @Override
        public Monitors monitors()
        {
            return null;
        }

        @Override
        public NeoStoreDataSource neoStoreDataSource()
        {
            NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
            when( neoStoreDataSource.getDatabaseName() ).thenReturn( databaseName );
            return neoStoreDataSource;
        }

        @Override
        public Supplier<CheckPointer> checkPointer()
        {
            return null;
        }

        @Override
        public Supplier<TransactionIdStore> transactionIdStoreSupplier()
        {
            return null;
        }

        @Override
        public Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier()
        {
            return null;
        }

        @Override
        public Supplier<LogFileInformation> logFileInformationSupplier()
        {
            return null;
        }

        @Override
        public FileSystemAbstraction fileSystemAbstraction()
        {
            return null;
        }

        @Override
        public PageCache pageCache()
        {
            return null;
        }

        @Override
        public StoreCopyCheckPointMutex storeCopyCheckPointMutex()
        {
            return null;
        }
    }
}
