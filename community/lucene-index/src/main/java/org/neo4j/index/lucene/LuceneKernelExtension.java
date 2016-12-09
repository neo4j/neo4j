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
package org.neo4j.index.lucene;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.spi.legacyindex.IndexProviders;
import org.neo4j.logging.LogProvider;

public class LuceneKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final Supplier<IndexConfigStore> indexStore;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final IndexProviders indexProviders;
    private final LogProvider logProvider;

    public LuceneKernelExtension( File storeDir, Config config, Supplier<IndexConfigStore> indexStore,
            FileSystemAbstraction fileSystemAbstraction, IndexProviders indexProviders,
            LogProvider logProvider)
    {
        this.storeDir = storeDir;
        this.config = config;
        this.indexStore = indexStore;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.indexProviders = indexProviders;
        this.logProvider = logProvider;
    }

    @Override
    public void init()
    {

        LuceneIndexImplementation indexImplementation =
                new LuceneIndexImplementation( storeDir, config, indexStore, fileSystemAbstraction, logProvider );
        indexProviders.registerIndexProvider( LuceneIndexImplementation.SERVICE_NAME, indexImplementation );
    }

    @Override
    public void shutdown()
    {
        indexProviders.unregisterIndexProvider( LuceneIndexImplementation.SERVICE_NAME );
    }
}
