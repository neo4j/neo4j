/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes;

public class IndexStoreViewFactory
{
    private final FullScanStoreView fullScanStoreView;
    private final LockService lockService;
    private final Config config;
    private final Supplier<StorageReader> storageReader;
    private final LogProvider logProvider;
    private final LabelScanStore labelScanStore;
    private final RelationshipTypeScanStore relationshipTypeScanStore;

    public IndexStoreViewFactory(
            Config config,
            Supplier<StorageReader> storageReader,
            FullScanStoreView fullScanStoreView,
            LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanStore,
            LockService lockService,
            LogProvider logProvider )
    {
        this.lockService = lockService;
        this.config = config;
        this.storageReader = storageReader;
        this.logProvider = logProvider;
        this.labelScanStore = labelScanStore;
        this.relationshipTypeScanStore = relationshipTypeScanStore;
        this.fullScanStoreView = fullScanStoreView;
    }

    public IndexStoreView createTokenIndexStoreView( IndexProxyProvider indexProxies )
    {
        if ( config.get( enable_scan_stores_as_token_indexes ) )
        {
            return new DynamicIndexStoreView( fullScanStoreView, lockService, config, indexProxies, storageReader, logProvider );
        }
        else
        {
            return new LegacyDynamicIndexStoreView(
                    fullScanStoreView, labelScanStore, relationshipTypeScanStore, lockService, storageReader, logProvider, config );
        }
    }
}
