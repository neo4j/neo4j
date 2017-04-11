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
package org.neo4j.kernel.impl.api.scan;

import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.LabelIndex;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

public class NativeLabelScanStoreExtension extends
        KernelExtensionFactory<NativeLabelScanStoreExtension.Dependencies>
{
    private static final String NAME = LabelIndex.NATIVE.name();

    public interface Dependencies
    {
        Config getConfig();

        PageCache pageCache();

        Supplier<IndexStoreView> indexStoreView();

        LogService getLogService();

        Monitors monitors();
    }

    public NativeLabelScanStoreExtension()
    {
        super( NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        Log log = dependencies.getLogService().getInternalLog( NativeLabelScanStore.class );
        Monitors monitors = dependencies.monitors();
        monitors.addMonitorListener( new LoggingMonitor( log ) );
        NativeLabelScanStore labelScanStore = new NativeLabelScanStore(
                dependencies.pageCache(),
                context.storeDir(),
                new FullLabelStream( dependencies.indexStoreView() ),
                dependencies.getConfig().get( GraphDatabaseSettings.read_only ),
                monitors );

        return new LabelScanStoreProvider( NAME, labelScanStore );
    }
}
