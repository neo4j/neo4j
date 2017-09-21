/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.impl.fulltext.FulltextFactory;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.RELATIONSHIPS;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_NODES;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS;

class BloomKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final GraphDatabaseService db;
    private final Procedures procedures;
    private LogService logService;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    private final Supplier<TransactionIdStore> transactionIdStore;
    private FulltextProvider provider;

    BloomKernelExtension( FileSystemAbstraction fileSystem, File storeDir, Config config,
                          GraphDatabaseService db, Procedures procedures,
                          LogService logService, AvailabilityGuard availabilityGuard,
                          JobScheduler scheduler,
                          Supplier<TransactionIdStore> transactionIdStore )
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
    }

    @Override
    public void start() throws IOException, KernelException
    {
        if ( config.get( BloomFulltextConfig.bloom_enabled ) )
        {
            List<String> properties = getProperties();
            String analyzer = config.get( BloomFulltextConfig.bloom_analyzer );

            Log log = logService.getInternalLog( FulltextProvider.class );
            provider = new FulltextProvider( db, log, availabilityGuard, scheduler, transactionIdStore.get() );
            FulltextFactory fulltextFactory = new FulltextFactory( fileSystem, storeDir, analyzer, provider );
            fulltextFactory.createFulltextIndex( BLOOM_NODES, NODES, properties );
            fulltextFactory.createFulltextIndex( BLOOM_RELATIONSHIPS, RELATIONSHIPS, properties );

            provider.init();
            procedures.registerComponent( FulltextProvider.class, context -> provider, true );
        }
    }

    private List<String> getProperties()
    {
        List<String> properties = config.get( BloomFulltextConfig.bloom_indexed_properties );
        if ( properties.isEmpty() )
        {
            throw new RuntimeException( "Properties to index must be configured for bloom fulltext" );
        }
        return properties;
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
