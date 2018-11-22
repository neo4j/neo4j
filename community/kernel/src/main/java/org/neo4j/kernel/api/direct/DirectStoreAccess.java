/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.direct;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.counts.CountsTracker;

public class DirectStoreAccess implements Closeable
{
    private final StoreAccess nativeStores;
    private final LabelScanStore labelScanStore;
    private final IndexProviderMap indexes;
    private final CountsTracker counts;

    public DirectStoreAccess( StoreAccess nativeStores, LabelScanStore labelScanStore, IndexProviderMap indexes, CountsTracker counts )
    {
        this.nativeStores = nativeStores;
        this.labelScanStore = labelScanStore;
        this.indexes = indexes;
        this.counts = counts;
    }

    public StoreAccess nativeStores()
    {
        return nativeStores;
    }

    public LabelScanStore labelScanStore()
    {
        return labelScanStore;
    }

    public IndexProviderMap indexes()
    {
        return indexes;
    }

    public CountsTracker counts()
    {
        return counts;
    }

    @Override
    public void close() throws IOException
    {
        // counts life cycle is managed outside
        nativeStores.close();
        labelScanStore.shutdown();
    }
}
