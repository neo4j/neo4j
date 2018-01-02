/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.neo4j.function.Supplier;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

@Service.Implementation(KernelExtensionFactory.class)
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
    }

    public OnlineBackupExtensionFactory()
    {
        super( KEY );
    }

    @Override
    public Class getSettingsClass()
    {
        return OnlineBackupSettings.class;
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return new OnlineBackupKernelExtension( dependencies.getConfig(), dependencies.getGraphDatabaseAPI(),
                dependencies.logService().getInternalLogProvider(), dependencies.monitors(),
                dependencies.neoStoreDataSource(),
                dependencies.checkPointer(),
                dependencies.transactionIdStoreSupplier(),
                dependencies.logicalTransactionStoreSupplier(),
                dependencies.logFileInformationSupplier(),
                dependencies.fileSystemAbstraction());
    }
}
