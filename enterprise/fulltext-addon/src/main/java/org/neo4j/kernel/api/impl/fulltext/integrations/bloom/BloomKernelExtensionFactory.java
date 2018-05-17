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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.JobScheduler;

/**
 * A {@link KernelExtensionFactory} for the bloom fulltext addon.
 *
 * @see BloomProcedures
 * @see BloomFulltextConfig
 */
@Service.Implementation( KernelExtensionFactory.class )
public class BloomKernelExtensionFactory extends KernelExtensionFactory<BloomKernelExtensionFactory.Dependencies>
{

    public static final String SERVICE_NAME = "bloom";
    public static final String BLOOM_RELATIONSHIPS = "bloomRelationships";
    public static final String BLOOM_NODES = "bloomNodes";

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseService db();

        FileSystemAbstraction fileSystem();

        Procedures procedures();

        LogService logService();

        AvailabilityGuard availabilityGuard();

        JobScheduler scheduler();

        TransactionIdStore transactionIdStore();

        NeoStoreFileListing fileListing();
    }

    public BloomKernelExtensionFactory()
    {
        super( SERVICE_NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        FileSystemAbstraction fs = dependencies.fileSystem();
        File storeDir = context.storeDir();
        Config config = dependencies.getConfig();
        GraphDatabaseService db = dependencies.db();
        Procedures procedures = dependencies.procedures();
        LogService logService = dependencies.logService();
        AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        JobScheduler scheduler = dependencies.scheduler();
        Supplier<TransactionIdStore> transactionIdStore = dependencies::transactionIdStore;
        Supplier<NeoStoreFileListing> fileListing = dependencies::fileListing;
        return new BloomKernelExtension(
                fs, storeDir, config, db, procedures, logService, availabilityGuard, scheduler, transactionIdStore, fileListing );
    }
}
