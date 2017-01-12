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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class NativeLabelScanStoreExtension extends
        KernelExtensionFactory<NativeLabelScanStoreExtension.Dependencies>
{
    public interface Dependencies
    {
        PageCache pageCache();

        Supplier<IndexStoreView> indexStoreView();
    }

    public NativeLabelScanStoreExtension()
    {
        super( "native" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        return new LabelScanStoreProvider( new NativeLabelScanStore( dependencies.pageCache(),
                context.storeDir(), Long.SIZE, new FullLabelStream( dependencies.indexStoreView() ) ), 0 );
    }
}
