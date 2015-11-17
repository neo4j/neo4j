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
package org.neo4j.kernel.impl.transaction.log.rotation;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;

public class StoreFlusher
{
    private final NeoStores neoStores;
    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;
    private final Iterable<IndexImplementation> indexProviders;

    public StoreFlusher(
            StorageEngine storageEngine,
            Iterable<IndexImplementation> indexProviders )
    {
        this.neoStores = storageEngine.neoStores();
        this.indexingService = storageEngine.indexingService();
        this.labelScanStore = storageEngine.labelScanStore();
        this.indexProviders = indexProviders;
    }

    public void forceEverything()
    {
        indexingService.forceAll();
        labelScanStore.force();
        for ( IndexImplementation index : indexProviders )
        {
            index.force();
        }
        neoStores.flush();
    }
}
