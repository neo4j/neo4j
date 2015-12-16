/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.IndexWriterFactories;
import org.neo4j.kernel.api.impl.index.LuceneLabelScanStore;
import org.neo4j.kernel.api.impl.index.NodeRangeDocumentLabelScanStorageStrategy;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.transaction.state.SimpleNeoStoresSupplier;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.fullStoreLabelUpdateStream;

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
    private final NeoStoresSupplier neoStoresSupplier;
    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;

    private LuceneLabelScanStore labelScanStore = null;

    public LuceneLabelScanStoreBuilder( File storeDir,
                                        NeoStores neoStores,
                                        FileSystemAbstraction fileSystem,
                                        LogProvider logProvider )
    {
        this.storeDir = storeDir;
        this.neoStoresSupplier = new SimpleNeoStoresSupplier( neoStores );
        this.fileSystem = fileSystem;
        this.logProvider = logProvider;
    }

    public LabelScanStore build()
    {
        if ( null == labelScanStore )
        {
            // TODO: Replace with kernel extension based lookup
            labelScanStore = new LuceneLabelScanStore(
                    new NodeRangeDocumentLabelScanStorageStrategy(),
                    DirectoryFactory.PERSISTENT,
                    LabelScanStoreProvider.getStoreDirectory( storeDir ),
                    fileSystem, IndexWriterFactories.standard(),
                    fullStoreLabelUpdateStream( neoStoresSupplier ),
                    LuceneLabelScanStore.loggerMonitor( logProvider ) );

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
