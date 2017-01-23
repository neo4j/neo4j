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
package org.neo4j.kernel.api.impl.labelscan;

import java.util.function.Supplier;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.labelscan.LabelScanStore.Monitor;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

@Service.Implementation(KernelExtensionFactory.class)
public class LuceneLabelScanStoreExtension extends KernelExtensionFactory<LuceneLabelScanStoreExtension.Dependencies>
{
    public static final String LABEL_SCAN_STORE_NAME = "lucene";

    private final int priority;
    private final Monitor monitor;

    public interface Dependencies
    {
        Config getConfig();

        /**
         * @return a {@link Supplier} of {@link IndexStoreView}, sort of like a delayed dependency lookup.
         * This is because we need the {@link IndexStoreView} dependency, although at the stage where we
         * grab dependencies, in {@link Lifecycle#init() init} that is, the {@link NeoStoreDataSource} hasn't been
         * {@link Lifecycle#start() started} yet and so haven't provided it.
         */
        Supplier<IndexStoreView> indexStoreView();

        LogService getLogService();
    }

    public LuceneLabelScanStoreExtension()
    {
        this( 10, null );
    }

    LuceneLabelScanStoreExtension( int priority, Monitor monitor )
    {
        super( "lucene-scan-store" );
        this.priority = priority;
        this.monitor = (monitor == null) ? Monitor.EMPTY : monitor;
    }

    @Override
    public LabelScanStoreProvider newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        Config config = dependencies.getConfig();
        boolean ephemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, context.fileSystem() );

        LuceneLabelScanIndexBuilder indexBuilder = getIndexBuilder( context, directoryFactory, config );
        LogProvider logger = dependencies.getLogService().getInternalLogProvider();
        Monitor loggingMonitor = new LoggingMonitor( logger.getLog( LuceneLabelScanStore.class ), monitor );
        LuceneLabelScanStore scanStore = new LuceneLabelScanStore( indexBuilder,
                new FullLabelStream( dependencies.indexStoreView() ), loggingMonitor );

        return new LabelScanStoreProvider( LABEL_SCAN_STORE_NAME, scanStore, priority );
    }

    private LuceneLabelScanIndexBuilder getIndexBuilder(
            KernelContext context, DirectoryFactory directoryFactory, Config config )
    {
        return LuceneLabelScanIndexBuilder.create()
                .withDirectoryFactory( directoryFactory )
                .withFileSystem( context.fileSystem() )
                .withIndexRootFolder( LabelScanStoreProvider.getStoreDirectory( context.storeDir() ) )
                .withConfig( config );
    }
}
