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
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.api.impl.fulltext.FulltextProviderImpl;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexType.NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexType.RELATIONSHIPS;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_NODES;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS;

class BloomKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final GraphDatabaseService db;
    private final Procedures procedures;
    private final LogService logService;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    private final Supplier<TransactionIdStore> transactionIdStore;
    private final Supplier<NeoStoreFileListing> fileListing;
    private FulltextProvider provider;

    BloomKernelExtension( FileSystemAbstraction fileSystem, File storeDir, Config config, GraphDatabaseService db, Procedures procedures, LogService logService,
            AvailabilityGuard availabilityGuard, JobScheduler scheduler, Supplier<TransactionIdStore> transactionIdStore,
            Supplier<NeoStoreFileListing> fileListing )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.fileSystem = fileSystem;
        this.db = db;
        this.procedures = procedures;
        this.logService = logService;
        this.availabilityGuard = availabilityGuard;
        this.scheduler = scheduler;
        this.transactionIdStore = transactionIdStore;
        this.fileListing = fileListing;
    }

    @Override
    public void start() throws IOException
    {
        if ( config.get( BloomFulltextConfig.bloom_enabled ) )
        {
            String analyzer = config.get( BloomFulltextConfig.bloom_default_analyzer );

            Log log = logService.getInternalLog( FulltextProviderImpl.class );
            provider = new FulltextProviderImpl( db, log, availabilityGuard, scheduler, transactionIdStore.get(),
                    fileSystem, storeDir, analyzer );
            provider.openIndex( BLOOM_NODES, NODES );
            provider.openIndex( BLOOM_RELATIONSHIPS, RELATIONSHIPS );
            provider.registerFileListing( fileListing.get() );

            procedures.registerComponent( FulltextProvider.class, context -> provider, true );
        }
        else
        {
            procedures.registerComponent( FulltextProvider.class, context -> FulltextProvider.NULL_PROVIDER, true );
        }
    }

    @Override
    public void stop() throws Exception
    {
        if ( provider != null )
        {
            provider.close();
        }
    }
}
