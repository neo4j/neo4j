/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanIndexBuilder;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.getStoreDirectory;

/**
 * Means of obtaining a {@link LabelScanStore}, independent of the {@link org.neo4j.kernel.extension.KernelExtensions}
 * mechanism, when you need to access the store without running a full database. This is used during consistency
 * checking.
 *
 * Duplicate functionality from {@link LuceneKernelExtensionFactory}, {@link LuceneKernelExtension}
 */
public class LuceneLabelScanStoreBuilder
{
    private final File storeDir;
    private final Supplier<IndexStoreView> storeViewSupplier;
    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final OperationalMode operationalMode;
    private final LogProvider logProvider;

    private LuceneLabelScanStore labelScanStore;

    public LuceneLabelScanStoreBuilder( File storeDir, IndexStoreView storeView,
            FileSystemAbstraction fileSystem, Config config, OperationalMode operationalMode, LogProvider logProvider )
    {
        this (storeDir, () -> storeView, fileSystem, config, operationalMode, logProvider);
    }

    public LuceneLabelScanStoreBuilder( File storeDir, Supplier<IndexStoreView> storeViewSupplier,
            FileSystemAbstraction fileSystem, Config config, OperationalMode operationalMode, LogProvider logProvider )
    {
        this.storeDir = storeDir;
        this.storeViewSupplier = storeViewSupplier;
        this.fileSystem = fileSystem;
        this.config = config;
        this.operationalMode = operationalMode;
        this.logProvider = logProvider;
    }

    public LabelScanStore build()
    {
        if ( null == labelScanStore )
        {
            // TODO: Replace with kernel extension based lookup
            LabelScanStore.Monitor monitor = new LoggingMonitor( logProvider.getLog( LuceneLabelScanStore.class ) );
            LuceneLabelScanIndexBuilder indexBuilder = LuceneLabelScanIndexBuilder.create()
                    .withFileSystem( fileSystem )
                    .withIndexRootFolder( getStoreDirectory( storeDir ) )
                    .withConfig( config )
                    .withOperationalMode( operationalMode );
            labelScanStore = new LuceneLabelScanStore( indexBuilder, new FullLabelStream( storeViewSupplier ),
                    monitor );

            try
            {
                labelScanStore.init();
                labelScanStore.start();
            }
            catch ( IOException e )
            {
                // Throw better exception
                throw new RuntimeException( e );
            }

        }

        return labelScanStore;
    }
}
