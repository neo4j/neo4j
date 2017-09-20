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

import org.apache.lucene.analysis.Analyzer;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.impl.fulltext.FulltextFactory;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

class BloomKernelExtension extends LifecycleAdapter
{
    public static final String BLOOM_RELATIONSHIPS = "bloomRelationships";
    public static final String BLOOM_NODES = "bloomNodes";
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final GraphDatabaseService db;
    private final Procedures procedures;
    private LogService logService;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    private FulltextProvider provider;

    BloomKernelExtension( FileSystemAbstraction fileSystemAbstraction, File storeDir, Config config,
                          GraphDatabaseService db, Procedures procedures,
                          LogService logService, AvailabilityGuard availabilityGuard,
                          JobScheduler scheduler )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.db = db;
        this.procedures = procedures;
        this.logService = logService;
        this.availabilityGuard = availabilityGuard;
        this.scheduler = scheduler;
    }

    @Override
    public void init() throws IOException, KernelException
    {
        if ( config.get( LoadableBloomFulltextConfig.bloom_enabled ) )
        {
            List<String> properties = getProperties();
            Analyzer analyzer = getAnalyzer();

            Log log = logService.getInternalLog( FulltextProvider.class );
            provider = new FulltextProvider( db, log, availabilityGuard, scheduler );
            FulltextFactory fulltextFactory = new FulltextFactory( fileSystemAbstraction, storeDir, analyzer );
            fulltextFactory.createFulltextIndex( BLOOM_NODES, FulltextProvider.FulltextIndexType.NODES, properties, provider );
            fulltextFactory.createFulltextIndex( BLOOM_RELATIONSHIPS, FulltextProvider.FulltextIndexType.RELATIONSHIPS, properties, provider );

            provider.init();
            procedures.registerComponent( FulltextProvider.class, context -> provider, true );
            procedures.registerProcedure( BloomProcedures.class );
        }
    }

    private List<String> getProperties()
    {
        List<String> properties = config.get( LoadableBloomFulltextConfig.bloom_indexed_properties );
        if ( properties.isEmpty() )
        {
            throw new RuntimeException( "Properties to index must be configured for bloom fulltext" );
        }
        return properties;
    }

    private Analyzer getAnalyzer()
    {
        Analyzer analyzer;
        try
        {
            Class configuredAnalayzer = Class.forName( config.get( LoadableBloomFulltextConfig.bloom_analyzer ) );
            analyzer = (Analyzer) configuredAnalayzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create the configured analyzer", e );
        }
        return analyzer;
    }

    @Override
    public void shutdown() throws Exception
    {
        if ( provider != null )
        {
            provider.close();
        }
    }
}
