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
package org.neo4j.backup;

import java.util.function.Supplier;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

@Service.Implementation( KernelExtensionFactory.class )
public class OnlineBackupExtensionFactory extends KernelExtensionFactory<OnlineBackupExtensionFactory.Dependencies>
{
    static final String KEY = "online backup";

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseAPI getGraphDatabaseAPI();

        LogService logService();

        Monitors monitors();

        NeoStoreDataSource neoStoreDataSource();

        Supplier<CheckPointer> checkPointer();

        Supplier<TransactionIdStore> transactionIdStoreSupplier();

        Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier();

        Supplier<LogFileInformation> logFileInformationSupplier();

        FileSystemAbstraction fileSystemAbstraction();

        PageCache pageCache();

        StoreCopyCheckPointMutex storeCopyCheckPointMutex();
    }

    public OnlineBackupExtensionFactory()
    {
        super( KEY );
    }

    @Deprecated
    public Class<OnlineBackupSettings> getSettingsClass()
    {
        throw new AssertionError();
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        if ( !isCausalClusterInstance( context ) )
        {
            return new OnlineBackupKernelExtension( dependencies.getConfig(), dependencies.getGraphDatabaseAPI(),
                    dependencies.logService().getInternalLogProvider(), dependencies.monitors(), dependencies.neoStoreDataSource(), dependencies.checkPointer(),
                    dependencies.transactionIdStoreSupplier(), dependencies.logicalTransactionStoreSupplier(), dependencies.logFileInformationSupplier(),
                    dependencies.fileSystemAbstraction(), dependencies.pageCache(), dependencies.storeCopyCheckPointMutex() );
        }
        return new LifecycleAdapter();
    }

    private static boolean isCausalClusterInstance( KernelContext kernelContext )
    {
        OperationalMode thisMode = kernelContext.databaseInfo().operationalMode;
        return OperationalMode.core.equals( thisMode ) || OperationalMode.read_replica.equals( thisMode );
    }
}
