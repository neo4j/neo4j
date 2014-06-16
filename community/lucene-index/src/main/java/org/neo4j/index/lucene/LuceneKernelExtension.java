/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.index.lucene;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class LuceneKernelExtension extends LifecycleAdapter
{
    private final Config config;
    private final IndexConfigStore indexStore;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final IndexProviders indexProviders;
    private LuceneDataSource luceneDataSource;
    private final LifeSupport life = new LifeSupport();

    public static abstract class Configuration
    {
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
    }

    public LuceneKernelExtension( Config config, IndexConfigStore indexStore,
            FileSystemAbstraction fileSystemAbstraction, IndexProviders indexProviders )
    {
        this.config = config;
        this.indexStore = indexStore;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.indexProviders = indexProviders;
    }

    @Override
    public void start() throws Throwable
    {
        luceneDataSource = life.add( new LuceneDataSource( config, indexStore, fileSystemAbstraction ) );
        // TODO Don't do this here, do proper life cycle management
        life.start();
        LuceneIndexImplementation indexImplementation = new LuceneIndexImplementation( luceneDataSource );
        indexProviders.registerIndexProvider( LuceneIndexImplementation.SERVICE_NAME, indexImplementation );
    }

    @Override
    public void stop() throws Throwable
    {
        indexProviders.unregisterIndexProvider( LuceneIndexImplementation.SERVICE_NAME );
        // TODO Don't do this here, do proper life cycle management
        life.shutdown();
    }
}
