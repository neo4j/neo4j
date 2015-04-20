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
package org.neo4j.kernel.api.impl.index;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.impl.index.LuceneLabelScanStore.Monitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.tracking;
import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;
import static org.neo4j.kernel.api.impl.index.LuceneLabelScanStore.loggerMonitor;
import static org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.fullStoreLabelUpdateStream;

@Service.Implementation(KernelExtensionFactory.class)
public class LuceneLabelScanStoreExtension extends KernelExtensionFactory<LuceneLabelScanStoreExtension.Dependencies>
{
    private final int priority;
    private final Monitor monitor;

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction getFileSystem();

        NeoStoreProvider getNeoStoreProvider();

        Logging getLogging();
    }

    public LuceneLabelScanStoreExtension()
    {
        this( 10, null );
    }

    LuceneLabelScanStoreExtension( int priority, Monitor monitor )
    {
        super( "lucene-scan-store");
        this.priority = priority;
        this.monitor = monitor;
    }

    @Override
    public LabelScanStoreProvider newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        DirectoryFactory directoryFactory = directoryFactory( dependencies.getConfig(), dependencies.getFileSystem() );
        File storeDir = dependencies.getConfig().get( GraphDatabaseSettings.store_dir );
        LuceneLabelScanStore scanStore = new LuceneLabelScanStore(
                new NodeRangeDocumentLabelScanStorageStrategy(),

                // <db>/schema/label/lucene
                directoryFactory, new File( new File( new File( storeDir, "schema" ), "label" ), "lucene" ),

                dependencies.getFileSystem(), tracking(),
                fullStoreLabelUpdateStream( dependencies.getNeoStoreProvider() ),
                monitor != null ? monitor : loggerMonitor( dependencies.getLogging() ) );

        return new LabelScanStoreProvider( scanStore, priority );
    }
}
